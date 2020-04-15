package main

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"
)

const ent = 42
const opt = 1
const dateLayout = "2006-01-02"
const httpConcurrency = 4

var iBankEpoch = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)
var yahooQuoteCsvPattern *regexp.Regexp

type StockPrice struct {
	securityId string
	symbol     string
	date       time.Time
	open       decimal.Decimal
	high       decimal.Decimal
	low        decimal.Decimal
	close      decimal.Decimal
	volume     int64
}

func checkError(err error) {
	if err != nil {
		log.Fatal("Security price synchronization failed", err)
	}
}

func checkDatabaseError(err error, database *sql.DB) {
	if err != nil {
		database.Close()
		log.Fatal("Security price synchronization failed", err)
	}
}

func checkDatabaseTxError(err error, tx *sql.Tx, database *sql.DB) {
	if err != nil {
		tx.Rollback()
		database.Close()
		log.Fatal("Security price synchronization failed", err)
	}
}

func readSecurities(out chan *StockPrice, tx *sql.Tx, database *sql.DB) {
	count := 0
	rows, err := database.Query("SELECT zuniqueid, zsymbol FROM zsecurity")
	checkDatabaseError(err, database)

	for rows.Next() {
		stockPrice := StockPrice{}

		err := rows.Scan(&stockPrice.securityId, &stockPrice.symbol)
		checkDatabaseTxError(err, tx, database)

		out <- &stockPrice
		count += 1
	}
	close(out)
	log.Printf("Found %v securities...\n", count)
}

func enrichStockPrice(in chan *StockPrice, out chan *StockPrice, tx *sql.Tx, database *sql.DB) {
	for stockPrice := range in {
		log.Printf("Downloading prices for %v...\n", stockPrice.symbol)
		resp, err := http.Get("https://query1.finance.yahoo.com/v7/finance/download/" + stockPrice.symbol + "?interval=1d&events=history")
		checkDatabaseTxError(err, tx, database)

		if resp.StatusCode == 200 {
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			checkDatabaseTxError(err, tx, database)

			cols := yahooQuoteCsvPattern.FindStringSubmatch(string(body))
			stockPrice.date, err = time.Parse(dateLayout, cols[1])
			checkDatabaseTxError(err, tx, database)
			stockPrice.open, err = decimal.NewFromString(cols[2])
			checkDatabaseTxError(err, tx, database)
			stockPrice.high, err = decimal.NewFromString(cols[3])
			checkDatabaseTxError(err, tx, database)
			stockPrice.low, err = decimal.NewFromString(cols[4])
			checkDatabaseTxError(err, tx, database)
			stockPrice.close, err = decimal.NewFromString(cols[5])
			checkDatabaseTxError(err, tx, database)
			stockPrice.volume, err = strconv.ParseInt(cols[6], 10, 32)
			checkDatabaseTxError(err, tx, database)

			out <- stockPrice
		}
	}
	out <- nil
}

func secondsSinceIBankEpoch(date time.Time) int {
	return int(date.Add(12 * time.Hour).Sub(iBankEpoch).Seconds())
}

func persistStockPrice(in chan *StockPrice, tx *sql.Tx, database *sql.DB) {
	count := 0
	httpWorkers := httpConcurrency
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
				stockPrice.volume, stockPrice.close, stockPrice.high, stockPrice.low, stockPrice.open, ent, opt, secondsSinceIBankEpoch(stockPrice.date), stockPrice.securityId)
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
					ent, opt, secondsSinceIBankEpoch(stockPrice.date), stockPrice.securityId, stockPrice.volume, stockPrice.close, stockPrice.high, stockPrice.low, stockPrice.open)
				checkDatabaseTxError(err, tx, database)
				log.Printf("No existing record for %v, new row inserted...\n", stockPrice.symbol)
			} else {
				log.Printf("Existing record for %v updated...\n", stockPrice.symbol)
			}
			count += 1
		} else {
			httpWorkers -= 1
			if httpWorkers == 0 {
				close(in)
			}
		}
	}
	log.Printf("Persisted prices for %v securities...\n", count)
}

func main() {
	var err error
	// Sample CSV output:
	// Date,Open,High,Low,Close,Adj Close,Volume
	// 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
	yahooQuoteCsvPattern, err = regexp.Compile(
		`Date,Open,High,Low,Close,Adj Close,Volume\n([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)\n?.*`)
	checkError(err)

	if len(os.Args) == 2 {
		iBankDataDir := os.Args[1]
		iBankDataFile := filepath.Join(iBankDataDir, "accountsData.ibank")
		log.Printf("Processing SQLite file %v...\n", iBankDataFile)

		database, err := sql.Open("sqlite3", iBankDataFile)
		checkDatabaseError(err, database)

		tx, err := database.Begin()
		checkDatabaseTxError(err, tx, database)

		enrichmentCh := make(chan *StockPrice, 1024)
		persistenceCh := make(chan *StockPrice)

		readSecurities(enrichmentCh, tx, database)

		for i := 0; i < httpConcurrency; i += 1 {
			go enrichStockPrice(enrichmentCh, persistenceCh, tx, database)
		}

		persistStockPrice(persistenceCh, tx, database)

		tx.Commit()
		database.Close()
		log.Println("Security prices synchronized successfully.")
	} else {
		log.Fatal("Please specify path to ibank data file.")
	}
}