package main

import (
	"database/sql"
	"encoding/csv"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const ent = 42
const opt = 1
const dateLayout = "2006-01-02"
const httpConcurrency = 4

var stdOut = log.New(os.Stdout, "", log.Flags())
var stdErr = log.New(os.Stderr, "", log.Flags())
var appleEpoch = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)
var httpTransport = &http.Transport{
	MaxConnsPerHost:     httpConcurrency,
	MaxIdleConnsPerHost: httpConcurrency}
var httpClient = &http.Client{Transport: httpTransport}

type StockPrice struct {
	securityId string
	symbol     string
	date       time.Time
	open       decimal.Decimal
	high       decimal.Decimal
	low        decimal.Decimal
	close      decimal.Decimal
	volume     int
}

func checkDatabaseError(err error, database *sql.DB) {
	if err != nil {
		database.Close()
		stdErr.Fatal("Security price synchronization failed: ", err)
	}
}

func checkDatabaseTxError(err error, tx *sql.Tx, database *sql.DB) {
	if err != nil {
		tx.Rollback()
		database.Close()
		stdErr.Fatal("Security price synchronization failed: ", err)
	}
}

func readSecurities(tx *sql.Tx, database *sql.DB) []StockPrice {
	stockPrices := make([]StockPrice, 0, 10)
	rows, err := database.Query("SELECT zuniqueid, zsymbol FROM zsecurity")
	checkDatabaseError(err, database)

	for rows.Next() {
		stockPrice := StockPrice{}

		err := rows.Scan(&stockPrice.securityId, &stockPrice.symbol)
		checkDatabaseTxError(err, tx, database)

		stockPrices = append(stockPrices, stockPrice)
	}
	stdOut.Printf("Found %v securities...\n", len(stockPrices))

	return stockPrices
}

func enrichStockPrice(stockPrice StockPrice, out chan *StockPrice, tx *sql.Tx, database *sql.DB) {
	stdOut.Printf("Downloading prices for %v...\n", stockPrice.symbol)
	resp, err := httpClient.Get(
		"https://query1.finance.yahoo.com/v7/finance/download/" + stockPrice.symbol + "?interval=1d&events=history")
	checkDatabaseTxError(err, tx, database)

	if resp.StatusCode == http.StatusOK {
		csvReader := csv.NewReader(resp.Body)
		data, err := csvReader.ReadAll()
		checkDatabaseTxError(err, tx, database)

		if len(data) > 1 {
			headers := data[0]
			if headers[0] == "Date" && headers[1] == "Open" && headers[2] == "High" &&
				headers[3] == "Low" && headers[4] == "Close" &&
				headers[5] == "Adj Close" && headers[6] == "Volume" {
				cols := data[1]
				checkDatabaseTxError(err, tx, database)
				stockPrice.date, err = time.Parse(dateLayout, cols[0])
				checkDatabaseTxError(err, tx, database)
				stockPrice.open, err = decimal.NewFromString(cols[1])
				checkDatabaseTxError(err, tx, database)
				stockPrice.high, err = decimal.NewFromString(cols[2])
				checkDatabaseTxError(err, tx, database)
				stockPrice.low, err = decimal.NewFromString(cols[3])
				checkDatabaseTxError(err, tx, database)
				stockPrice.close, err = decimal.NewFromString(cols[4])
				checkDatabaseTxError(err, tx, database)
				stockPrice.volume, err = strconv.Atoi(cols[6])
				checkDatabaseTxError(err, tx, database)

				out <- &stockPrice
			} else {
				out <- nil
			}
		}
	} else {
		io.Copy(ioutil.Discard, resp.Body)
		if resp.StatusCode != http.StatusNotFound {
			stdErr.Printf("Got HTTP %v for %v...\n", resp.StatusCode, stockPrice.symbol)
		}
		out <- nil
	}
	resp.Body.Close()
}

func secondsSinceAppleEpoch(date time.Time) int {
	return int(date.Add(12 * time.Hour).Sub(appleEpoch).Seconds())
}

func persistStockPrice(in chan *StockPrice, inputCount int, tx *sql.Tx, database *sql.DB) {
	count := 0
	for stockPrice := range in {
		if stockPrice != nil {
			updateResult, err := tx.Exec(
				`UPDATE zprice
				SET
					zvolume = $1,
					zclosingprice = $2,
					zhighprice = $3,
					zlowprice = $4,
					zopeningprice = $5
				WHERE
					z_ent = $6 AND z_opt = $7 AND
					zdate = $8 AND zsecurityid = $9
				`,
				stockPrice.volume, stockPrice.close, stockPrice.high, stockPrice.low, stockPrice.open, ent, opt, secondsSinceAppleEpoch(stockPrice.date), stockPrice.securityId)
			checkDatabaseTxError(err, tx, database)
			updateCount, err := updateResult.RowsAffected()
			if updateCount == 0 {
				_, err := tx.Exec(
					`INSERT INTO zprice (
						z_ent, z_opt, zdate, zsecurityid,
						zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9
					)`,
					ent, opt, secondsSinceAppleEpoch(stockPrice.date), stockPrice.securityId, stockPrice.volume, stockPrice.close, stockPrice.high, stockPrice.low, stockPrice.open)
				checkDatabaseTxError(err, tx, database)
				stdOut.Printf("New entry for %v created...\n", stockPrice.symbol)
			} else {
				stdOut.Printf("Existing entry for %v updated...\n", stockPrice.symbol)
			}
			count += 1
		}
		inputCount -= 1
		if inputCount == 0 {
			close(in)
		}
	}
	_, err := tx.Exec(
		`UPDATE z_primarykey
		SET z_max = (SELECT MAX(z_pk) FROM zprice)
		WHERE z_name = 'Price'
		`)
	checkDatabaseTxError(err, tx, database)
	stdOut.Printf("Primary key for price updated...")
	stdOut.Printf("Persisted prices for %v securities...\n", count)
}

func main() {
	if len(os.Args) == 2 {
		iBankDataDir := os.Args[1]
		sqliteFile := filepath.Join(iBankDataDir, "accountsData.ibank")
		stdOut.Printf("Processing SQLite file %v...\n", sqliteFile)

		database, err := sql.Open("sqlite3", sqliteFile)
		checkDatabaseError(err, database)

		tx, err := database.Begin()
		checkDatabaseTxError(err, tx, database)

		persistenceChan := make(chan *StockPrice)

		stockPrices := readSecurities(tx, database)

		for _, stockPrice := range stockPrices {
			go enrichStockPrice(stockPrice, persistenceChan, tx, database)
		}

		persistStockPrice(persistenceChan, len(stockPrices), tx, database)

		tx.Commit()
		database.Close()
		stdOut.Println("Security prices synchronized successfully.")
	} else {
		stdErr.Fatal("Please specify path to ibank data file.")
	}
}
