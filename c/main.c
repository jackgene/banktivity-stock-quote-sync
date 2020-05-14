#include <sqlite3.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <curl/curl.h>
#include "log.h"

// General constants
#define MAX_SYMBOL_LEN 5
#define MAX_SECURITY_ID_LEN 36
#define MAX_NUM_LEN 20
#define ERROR_MESSAGE_FORMAT "Security price synchronization failed: %s\n"

// Database constants
#define ACCOUNTS_DATA_FILE "/accountsData.ibank"
#define SELECT_SECURITY_SQL "SELECT zuniqueid, zsymbol FROM zsecurity"
#define ENT 42
#define OPT 1
// Apple epoch (2001-01-01) +12 hours
// Maximizes chances of iBank displaying the same date for all timezones
#define IBANK_EPOCH 978292800L
#define UPDATE_PRICE_SQL "\
UPDATE zprice \
SET\
    zvolume = ?,\
    zclosingprice = ?,\
    zhighprice = ?,\
    zlowprice = ?,\
    zopeningprice = ? \
WHERE\
    z_ent = ? AND z_opt = ? AND \
    zdate = ? AND zsecurityid = ?"
#define UPDATE_PRICE_SQL_LEN sizeof(UPDATE_PRICE_SQL)
#define INSERT_PRICE_SQL "\
INSERT INTO zprice (\
    z_ent, z_opt, zdate, zsecurityid,\
    zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice\
) VALUES (\
    ?, ?, ?, ?, ?, ?, ?, ?, ?\
)"
#define INSERT_PRICE_SQL_LEN sizeof(INSERT_PRICE_SQL)
#define UPDATE_PK_SQL "\
UPDATE z_primarykey \
SET z_max = (SELECT MAX(z_pk) FROM zprice) \
WHERE z_name = 'Price'"

// HTTP constants
#define HTTP_CONCURRENCY 24
#define PRICE_URL_FORMAT "https://query1.finance.yahoo.com/v7/finance/download/%s?interval=1d&events=history"
#define MAX_PRICE_URL_LEN sizeof(PRICE_URL_FORMAT) + MAX_SYMBOL_LEN - 1
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
    int volume;
    char close[21];
    char high[21];
    char low[21];
    char open[21];
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

    sqlite_ret = sqlite3_exec(db, SELECT_SECURITY_SQL,
                              process_security_select_row_sqlite_cb,
                              &builder, &sqlite_err);
    if (sqlite_ret == SQLITE_OK) {
        *count = builder.count;
        *prices = builder.first;
        log_info("Found %d securities...", *count);

        return EXIT_SUCCESS;
    } else {
        log_error(ERROR_MESSAGE_FORMAT, sqlite_err);
        sqlite3_free(sqlite_err);

        return EXIT_FAILURE;
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
                    if (body[i] == '\n') {
                        break;
                    } else if (body[i] >= '0' && body[i] <= '9') {
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

static void submit_price_request_curl(CURLM *curl_multi, stock_prices *prices) {
    CURL *curl_easy = curl_easy_init();
    prices->curl = curl_easy;
    char *symbol = prices->symbol;
    char url[MAX_PRICE_URL_LEN];
    sprintf(url, PRICE_URL_FORMAT, symbol);
    curl_easy_setopt(curl_easy, CURLOPT_TCP_KEEPALIVE, 1L);
    curl_easy_setopt(curl_easy, CURLOPT_IPRESOLVE, CURL_IPRESOLVE_V4);
    curl_easy_setopt(curl_easy, CURLOPT_URL, url);
    curl_easy_setopt(curl_easy, CURLOPT_WRITEFUNCTION, process_price_request_curl_cb);
    curl_easy_setopt(curl_easy, CURLOPT_WRITEDATA, prices);
    curl_multi_add_handle(curl_multi, curl_easy);
}

static int populate_stock_prices(stock_prices *prices) {
    // Async HTTP calls, largely based on:
    // https://curl.haxx.se/libcurl/c/10-at-a-time.html
    CURLM *curl_multi;
    CURLMsg *msg;
    int msgs_left = -1;
    int active_connections = 1;

    curl_global_init(CURL_GLOBAL_ALL);
    curl_multi = curl_multi_init();
    curl_multi_setopt(curl_multi, CURLMOPT_MAXCONNECTS, (long)HTTP_CONCURRENCY);
    curl_multi_setopt(curl_multi, CURLOPT_TCP_FASTOPEN, 1L);

    while (prices != NULL) {
        log_debug("Downloading prices for %s...", prices->symbol);
        submit_price_request_curl(curl_multi, prices);
        prices = prices->next;
    }

    do {
        curl_multi_perform(curl_multi, &active_connections);
        
        while ((msg = curl_multi_info_read(curl_multi, &msgs_left))) {
            if (msg->msg == CURLMSG_DONE) {
                CURL *curl_easy = msg->easy_handle;
                curl_multi_remove_handle(curl_multi, curl_easy);
                curl_easy_cleanup(curl_easy);
            } else {
                log_error("HTTP error CURLMsg (%d)\n", msg->msg);
            }
        }
        if (active_connections)
            curl_multi_wait(curl_multi, NULL, 0, 20, NULL);
        
    } while (active_connections);

    curl_multi_cleanup(curl_multi);
    curl_global_cleanup();

    return EXIT_SUCCESS;
}

// Seconds since Apple epoch - 12 hours
static long ibank_time(struct tm *date) {
    return mktime(date) - IBANK_EPOCH;
}

static int persist_stock_prices(sqlite3 *db, stock_prices *prices) {
    int count = 0;
    int sqlite_ret;
    char *sqlite_err;
    sqlite3_stmt *update_stmt, *insert_stmt;

    sqlite3_prepare_v2(db, UPDATE_PRICE_SQL, UPDATE_PRICE_SQL_LEN, &update_stmt, NULL);
    sqlite3_prepare_v2(db, INSERT_PRICE_SQL, INSERT_PRICE_SQL_LEN, &insert_stmt, NULL);
    while (prices != NULL) {
        if (prices->load_state == LOAD_STATE_VOLUME) {
            sqlite3_bind_int(update_stmt, 1, prices->volume);
            sqlite3_bind_text(update_stmt, 2, prices->close, -1, SQLITE_STATIC);
            sqlite3_bind_text(update_stmt, 3, prices->high, -1, SQLITE_STATIC);
            sqlite3_bind_text(update_stmt, 4, prices->low, -1, SQLITE_STATIC);
            sqlite3_bind_text(update_stmt, 5, prices->open, -1, SQLITE_STATIC);
            sqlite3_bind_int(update_stmt, 6, ENT);
            sqlite3_bind_int(update_stmt, 7, OPT);
            sqlite3_bind_int64(update_stmt, 8, ibank_time(&prices->date));
            sqlite3_bind_text(update_stmt, 9, prices->security_id, -1, SQLITE_STATIC);
            sqlite_ret = sqlite3_step(update_stmt);
            if (sqlite_ret == SQLITE_DONE) {
                if (sqlite3_changes(db) > 0) {
                    count++;
                    log_debug("Existing entry for %s updated...", prices->symbol);
                } else {
                    sqlite3_bind_int(insert_stmt, 1, ENT);
                    sqlite3_bind_int(insert_stmt, 2, OPT);
                    sqlite3_bind_int64(insert_stmt, 3, ibank_time(&prices->date));
                    sqlite3_bind_text(insert_stmt, 4, prices->security_id, -1, SQLITE_STATIC);
                    sqlite3_bind_int(insert_stmt, 5, prices->volume);
                    sqlite3_bind_text(insert_stmt, 6, prices->close, -1, SQLITE_STATIC);
                    sqlite3_bind_text(insert_stmt, 7, prices->high, -1, SQLITE_STATIC);
                    sqlite3_bind_text(insert_stmt, 8, prices->low, -1, SQLITE_STATIC);
                    sqlite3_bind_text(insert_stmt, 9, prices->open, -1, SQLITE_STATIC);
                    sqlite_ret = sqlite3_step(insert_stmt);
                    if (sqlite_ret == SQLITE_DONE) {
                        count++;
                        log_debug("New entry for %s created...", prices->symbol);
                        sqlite3_reset(insert_stmt);
                    } else {
                        log_error("Price insert for %s failed (step code: %d)", prices->symbol, sqlite_ret);
                    }
                }
                sqlite3_reset(update_stmt);
            } else {
                log_error("Price update for %s failed (step code: %d)", prices->symbol, sqlite_ret);
            }
        }
        prices = prices->next;
    }

    sqlite_ret = sqlite3_exec(db, UPDATE_PK_SQL,
                              NULL, NULL, &sqlite_err);
    if (sqlite_ret == SQLITE_OK) {
        log_debug("Primary key for price updated...");
        log_info("Persisted prices for %d securities...", count);

        return EXIT_SUCCESS;
    } else {
        log_error(ERROR_MESSAGE_FORMAT, sqlite_err);
        sqlite3_free(sqlite_err);

        return EXIT_FAILURE;
    }
}

static void free_stock_prices(stock_prices *prices) {
    if (prices->next != NULL) free_stock_prices(prices->next);
    free(prices);
}

int main(int argc, char **argv) {
    int exit = EXIT_FAILURE;
    struct timespec start;
    clock_gettime(CLOCK_MONOTONIC, &start);

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
            if (read_securities(db, &read_count, &prices) == EXIT_SUCCESS) {
                if (populate_stock_prices(prices) == EXIT_SUCCESS) {
                    if (persist_stock_prices(db, prices) == EXIT_SUCCESS) {
                        exit = EXIT_SUCCESS;
                    }
                }

                free_stock_prices(prices);
            }
        } else {
            log_error(ERROR_MESSAGE_FORMAT, sqlite3_errmsg(db));
        }

        sqlite3_close(db);
        free(sqlite_file);

        if (exit == EXIT_SUCCESS) {
            struct timespec end;
            clock_gettime(CLOCK_MONOTONIC, &end);
            double elapsed_secs =
                end.tv_sec - start.tv_sec +
                (end.tv_nsec - start.tv_nsec) / 1.0e9;
            log_info("Security prices synchronized in %.3fs.", elapsed_secs);
        }
        return exit;
    } else {
        fprintf(stderr, "Please specify path to ibank data file.\n");
        return EXIT_FAILURE;
    }
}
