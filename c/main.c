#include <sqlite3.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <curl/curl.h>
#include "log.h"

#define ACCOUNTS_DATA_FILE "/accountsData.ibank"
#define ERROR_MESSAGE_FORMAT "Security price synchronization failed: %s\n"
#define PRICE_URL_FORMAT "https://query1.finance.yahoo.com/v7/finance/download/%s?interval=1d&events=history"
#define MAX_SYMBOL_LEN 4
#define MAX_SECURITY_ID_LEN 36
#define HTTP_CONCURRENCY 4
#define MAX_PRICE_URL_LEN sizeof(PRICE_URL_FORMAT) + MAX_SYMBOL_LEN - 1
#define MAX_NUM_LEN 20
#define CSV_HEADER "Date,Open,High,Low,Close,Adj Close,Volume"

#define LOAD_STATE_VERIFY_HEADER 0
#define LOAD_STATE_DATE_YEAR 100
#define LOAD_STATE_DATE_MON 101
#define LOAD_STATE_DATE_MDAY 102
#define LOAD_STATE_OPEN 200
#define LOAD_STATE_HIGH 300
#define LOAD_STATE_LOW 400
#define LOAD_STATE_CLOSE 500
#define LOAD_STATE_ADJCLOSE 600
#define LOAD_STATE_VOLUME 700
#define LOAD_STATE_FAILED -1

typedef struct stock_prices {
    char security_id[37];
    char symbol[MAX_SYMBOL_LEN + 1];
    struct tm date;
    char open[21];
    char high[21];
    char low[21];
    char close[21];
    int volume;
    int load_state;
    CURL *curl;
    struct stock_prices *next;
} stock_prices;

typedef struct stock_prices_builder {
    stock_prices *first;
    stock_prices *last;
    int count;
} stock_prices_builder;

static int process_security_select_row_sqlite_cb(void *builder_ptr,
                                                 int cols,
                                                 char **values,
                                                 char **col_names) {
    if (strlen(values[0]) <= MAX_SECURITY_ID_LEN && strlen(values[1]) <= MAX_SYMBOL_LEN) {
        stock_prices_builder *builder = (stock_prices_builder*)builder_ptr;
        (builder->count)++;

        stock_prices *new_price = malloc(sizeof(stock_prices));
        new_price->load_state = LOAD_STATE_VERIFY_HEADER;
        strcpy(new_price->security_id, values[0]);
        strcpy(new_price->symbol, values[1]);
        if (builder->last) {
            builder->last->next = new_price;
            builder->last = new_price;
        } else {
            builder->first = new_price;
            builder->last = new_price;
        }
    }

    return SQLITE_OK;
}

static int read_securities(sqlite3 *db, int *count, stock_prices **prices) {
    stock_prices_builder builder = { NULL, NULL, 0 };
    int sqlite_ret;
    char *sqlite_err;

    sqlite_ret = sqlite3_exec(db,
                              "SELECT zuniqueid, zsymbol FROM zsecurity",
                              process_security_select_row_sqlite_cb,
                              &builder, &sqlite_err);
    if (sqlite_ret == SQLITE_OK) {
        *count = builder.count;
        *prices = builder.first;
        log_info("Found %d securities...", *count);

        return 0;
    } else {
        log_error(ERROR_MESSAGE_FORMAT, sqlite_err);
        sqlite3_free(sqlite_err);

        return 1;
    }
}

static size_t process_price_request_curl_cb(char *body, size_t n, size_t l, void *price_ptr) {
    stock_prices *price = (stock_prices*)price_ptr;
    long http_status;
    curl_easy_getinfo(price->curl, CURLINFO_RESPONSE_CODE, &http_status);

    if (http_status == 200) {
        if (price->load_state >= LOAD_STATE_VERIFY_HEADER) {
            for (int i = 0; i < l; i ++) {
                if (price->load_state < LOAD_STATE_DATE_YEAR) {
                    if (price->load_state == LOAD_STATE_VERIFY_HEADER + sizeof(CSV_HEADER) - 1 &&
                            body[i] == '\n') {
                        price->load_state = LOAD_STATE_DATE_YEAR;
                    } else if (price->load_state < LOAD_STATE_VERIFY_HEADER + sizeof(CSV_HEADER) - 1 &&
                            body[i] == CSV_HEADER[price->load_state - LOAD_STATE_VERIFY_HEADER]) {
                        price->load_state++;
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - bad CSV header index %d\n%s", price->load_state - LOAD_STATE_VERIFY_HEADER, body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state == LOAD_STATE_DATE_YEAR) {
                    if (body[i] == '-') {
                        price->date.tm_year = price->date.tm_year - 1900;
                        price->load_state = LOAD_STATE_DATE_MON;
                    } else if (body[i] >= '0' && body[i] <= '9') {
                        price->date.tm_year = price->date.tm_year * 10 + body[i] - '0';
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - date - year\n%s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state == LOAD_STATE_DATE_MON) {
                    if (body[i] == '-') {
                        price->date.tm_mon = price->date.tm_mon - 1;
                        price->load_state = LOAD_STATE_DATE_MDAY;
                    } else if (body[i] >= '0' && body[i] <= '9') {
                        price->date.tm_mon = price->date.tm_mon * 10 + body[i] - '0';
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - date - mon\n%s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state == LOAD_STATE_DATE_MDAY) {
                    if (body[i] == ',') {
                        price->load_state = LOAD_STATE_OPEN;
                    } else if (body[i] >= '0' && body[i] <= '9') {
                        price->date.tm_mday = price->date.tm_mday * 10 + body[i] - '0';
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - date - mday\n%s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state < LOAD_STATE_HIGH) {
                    if (body[i] == ',') {
                        price->open[price->load_state - LOAD_STATE_OPEN] = '\0';
                        price->load_state = LOAD_STATE_HIGH;
                    } else if (price->load_state < LOAD_STATE_OPEN + MAX_NUM_LEN &&
                               (body[i] == '.' || (body[i] >= '0' && body[i] <= '9'))) {
                        price->open[price->load_state - LOAD_STATE_OPEN] = body[i];
                        price->load_state++;
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - open %s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state < LOAD_STATE_LOW) {
                    if (body[i] == ',') {
                        price->high[price->load_state - LOAD_STATE_HIGH] = '\0';
                        price->load_state = LOAD_STATE_LOW;
                    } else if (price->load_state < LOAD_STATE_HIGH + MAX_NUM_LEN &&
                               (body[i] == '.' || (body[i] >= '0' && body[i] <= '9'))) {
                        price->high[price->load_state - LOAD_STATE_HIGH] = body[i];
                        price->load_state++;
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - high %s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state < LOAD_STATE_CLOSE) {
                    if (body[i] == ',') {
                        price->low[price->load_state - LOAD_STATE_LOW] = '\0';
                        price->load_state = LOAD_STATE_CLOSE;
                    } else if (price->load_state < LOAD_STATE_LOW + MAX_NUM_LEN &&
                               (body[i] == '.' || (body[i] >= '0' && body[i] <= '9'))) {
                        price->low[price->load_state - LOAD_STATE_LOW] = body[i];
                        price->load_state++;
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - low %s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state < LOAD_STATE_ADJCLOSE) {
                    if (body[i] == ',') {
                        price->close[price->load_state - LOAD_STATE_CLOSE] = '\0';
                        price->load_state = LOAD_STATE_ADJCLOSE;
                    } else if (price->load_state < LOAD_STATE_CLOSE + MAX_NUM_LEN &&
                               (body[i] == '.' || (body[i] >= '0' && body[i] <= '9'))) {
                        price->close[price->load_state - LOAD_STATE_CLOSE] = body[i];
                        price->load_state++;
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - close %s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                } else if (price->load_state < LOAD_STATE_VOLUME) {
                    if (body[i] == ',') {
                        price->load_state = LOAD_STATE_VOLUME;
                    }
                } else {
                    if (body[i] >= '0' && body[i] <= '9') {
                        price->volume = price->volume * 10 + body[i] - '0';
                    } else {
                        body[n*l] = '\0';
                        log_error("Failed to verify HTTP data - volume %s", body);
                        price->load_state = LOAD_STATE_FAILED;
                        break;
                    }
                }
            }
        }
    } else {
        price->load_state = -http_status;
    }

    return n*l;
}

static int populate_stock_prices(stock_prices *prices) {
    curl_global_init(CURL_GLOBAL_DEFAULT);
    CURL *curl = curl_easy_init();
    char url[MAX_PRICE_URL_LEN];
    char *symbol;

    while (prices != NULL) {
        prices->curl = curl;
        symbol = prices->symbol;
        sprintf(url, PRICE_URL_FORMAT, symbol);
        curl_easy_setopt(curl, CURLOPT_URL, url);
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, process_price_request_curl_cb);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, prices);
        if (curl_easy_perform(curl) != CURLE_OK) {
            log_error(ERROR_MESSAGE_FORMAT, "HTTP error downloading stock price");
            return 1;
        }
        prices = prices->next;
    }
    curl_global_cleanup();

    return 0;
}

static void free_stock_prices(stock_prices* prices) {
    if (prices->next != NULL) free_stock_prices(prices->next);
    free(prices);
}

int main(int argc, char **argv) {
    if (argc == 2) {
        stock_prices *prices = NULL;
        int read_count = 0;
        char *ibank_data_dir;
        char *sqlite_file;
        sqlite3 *db;
        int sqlite_ret;

        log_set_quiet(true);
        log_set_fp(stdout);
        ibank_data_dir = argv[1];
        sqlite_file = malloc(strlen(ibank_data_dir) + sizeof(ACCOUNTS_DATA_FILE));
        strcpy(sqlite_file, ibank_data_dir);
        strcat(sqlite_file, ACCOUNTS_DATA_FILE);
        log_info("Processing SQLite file %s...", sqlite_file);
        sqlite_ret = sqlite3_open(sqlite_file, &db);
        if (sqlite_ret == SQLITE_OK) {
            if (read_securities(db, &read_count, &prices) == 0) {
                if (populate_stock_prices(prices) == 0) {
                    stock_prices *wtf = prices;
                    while (wtf != NULL) {
                        log_info("%s (%s) - %s/%s/%s/%s/%d",
                                 wtf->security_id,
                                 wtf->symbol,
                                 wtf->open,
                                 wtf->high,
                                 wtf->low,
                                 wtf->close,
                                 wtf->volume);
                        wtf = wtf->next;
                    }
                }

                free_stock_prices(prices);
            }
        } else {
            log_error(ERROR_MESSAGE_FORMAT, sqlite3_errmsg(db));
        }

        sqlite3_close(db);
        free(sqlite_file);
    } else {
        fprintf(stderr, "Please specify path to ibank data file.\n");
        return 1;
    }
}
