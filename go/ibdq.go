package main

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const maxSymbolLength = 5
const ent = 42
const opt = 1

var stdOut = log.New(os.Stdout, "", log.Flags())
var stdErr = log.New(os.Stderr, "", log.Flags())
var appleEpoch = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)
var httpClient = &http.Client{}

func checkDatabaseError(err error, database io.Closer) {
	if err != nil {
		_ = database.Close()
		stdErr.Fatal("Security price synchronization failed: ", err)
	}
}

func checkDatabaseTxError(err error, tx driver.Tx, database io.Closer) {
	if err != nil {
		_ = tx.Rollback()
		_ = database.Close()
		stdErr.Fatal("Security price synchronization failed: ", err)
	}
}

func readSecurityIDs(db Querier) ([]SecurityID, error) {
	rows, queryErr := db.Query(
		"SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ? ORDER BY zsymbol", maxSymbolLength)
	if queryErr != nil {
		return nil, queryErr
	}
	defer func() { _ = rows.Close() }()

	securityIDS := make([]SecurityID, 0, 32)
	var symbols strings.Builder
	addComma := false
	for rows.Next() {
		securityID := SecurityID{}

		if rowScanErr := rows.Scan(&securityID.UniqueId, &securityID.Symbol); rowScanErr != nil {
			return nil, rowScanErr
		}

		securityIDS = append(securityIDS, securityID)
		if addComma {
			symbols.WriteString(", ")
		} else {
			addComma = true
		}
		symbols.WriteString(securityID.Symbol)
	}
	stdOut.Printf("Found %v securities (%v)\n", len(securityIDS), symbols.String())

	return securityIDS, nil
}

func getStockPrices(symbols []string) (StockPrices, error) {
	var stockPrices StockPrices
	stdOut.Printf("Downloading prices")
	resp, httpGetErr := httpClient.Get(
		fmt.Sprintf("https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=%v", strings.Join(symbols, ",")))
	if httpGetErr != nil {
		return stockPrices, httpGetErr
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusOK {
		if jsonDecodeErr := json.NewDecoder(resp.Body).Decode(&stockPrices); jsonDecodeErr != nil {
			return stockPrices, jsonDecodeErr
		}

		return stockPrices, nil
	} else {
		if resp.StatusCode != http.StatusNotFound {
			return stockPrices, fmt.Errorf("HTTP %v", resp.StatusCode)
		}
		return stockPrices, nil
	}
}

func persistStockPrices(securities []Security, db Querier) error {
	count := 0
	for _, security := range securities {
		updateResult, updateErr := db.Exec(
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
			security.Price.Volume, security.Price.Close, security.Price.High, security.Price.Low, security.Price.Open,
			ent, opt, security.Price.Date.SecondsSinceAppleEpoch(), security.Id.UniqueId)
		if updateErr != nil {
			return updateErr
		}

		updateCount, updateCountErr := updateResult.RowsAffected()
		if updateCountErr != nil {
			return updateCountErr
		}

		if updateCount > 0 {
			stdOut.Printf("Existing entry for %v updated\n", security.Id.Symbol)
		} else {
			_, insertErr := db.Exec(
				`INSERT INTO zprice (
						z_ent, z_opt, zdate, zsecurityid,
						zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
					) VALUES (
						$1, $2, $3, $4, $5, $6, $7, $8, $9
					)`,
				ent, opt, security.Price.Date.SecondsSinceAppleEpoch(), security.Id.UniqueId,
				security.Price.Volume, security.Price.Close, security.Price.High, security.Price.Low, security.Price.Open)
			if insertErr != nil {
				return insertErr
			}
			stdOut.Printf("New entry for %v created\n", security.Id.Symbol)
		}
		count += 1
	}
	_, updatePrimaryKeyErr := db.Exec(
		`UPDATE z_primarykey
		SET z_max = (SELECT MAX(z_pk) FROM zprice)
		WHERE z_name = 'Price'
		`)
	if updatePrimaryKeyErr != nil {
		return updatePrimaryKeyErr
	}

	stdOut.Printf("Primary key for price updated\n")
	stdOut.Printf("Persisted prices for %v securities\n", count)
	return nil
}

func main() {
	start := time.Now()

	if len(os.Args) == 2 {
		banktivityDataDir := os.Args[1]
		sqliteFile := filepath.Join(banktivityDataDir, "accountsData.ibank")
		stdOut.Printf("Processing SQLite file %v...\n", sqliteFile)

		database, dbOpenErr := sql.Open("sqlite3", sqliteFile)
		checkDatabaseError(dbOpenErr, database)
		defer func() {
			if dbCloseErr := database.Close(); dbCloseErr != nil {
				stdErr.Printf("Failed to close database: %v\n", dbCloseErr)
			}
		}()

		securityIDs, readSecurityIDsErr := readSecurityIDs(database)
		checkDatabaseError(readSecurityIDsErr, database)

		symbols := make([]string, 0, len(securityIDs))
		for _, securityID := range securityIDs {
			symbols = append(symbols, securityID.Symbol)
		}
		stockPrices, getStockPricesErr := getStockPrices(symbols)
		checkDatabaseError(getStockPricesErr, database)

		tx, txBeginErr := database.Begin()
		checkDatabaseTxError(txBeginErr, tx, database)
		defer func() {
			if txCommitErr := tx.Commit(); txCommitErr != nil {
				stdErr.Printf("Failed to commit transaction: %v\n", txCommitErr)
			}
		}()

		securities := make([]Security, 0, len(stockPrices.BySymbol))
		for _, securityID := range securityIDs {
			if stockPrice, ok := stockPrices.BySymbol[securityID.Symbol]; ok {
				securities = append(securities, Security{
					Id:    securityID,
					Price: stockPrice,
				})
			}
		}
		persistStockPricesErr := persistStockPrices(securities, tx)
		checkDatabaseTxError(persistStockPricesErr, tx, database)

		stdOut.Printf("Securities updated in %.3fs", time.Since(start).Seconds())
	} else {
		stdErr.Fatal("Please specify path to Banktivity data file")
	}
}
