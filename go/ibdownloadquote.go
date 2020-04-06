package main

import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/shopspring/decimal"
	"io/ioutil"
	"net/http"
	"regexp"
	"strconv"
	"time"
)

const ent = 42
const opt = 1
const dateLayout = "2006-01-02"
var iBankEpoch = time.Date(2001, time.January, 1, 0, 0, 0, 0, time.UTC)

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func checkDatabaseError(err error, database *sql.DB) {
	if err != nil {
		database.Close()
		panic(err)
	}
}

func checkDatabaseTxError(err error, tx *sql.Tx, database *sql.DB) {
	if err != nil {
		tx.Rollback()
		database.Close()
		panic(err)
	}
}

func secondsSinceIBankEpoch(date time.Time) int {
	return int(date.Add(12 * time.Hour).Sub(iBankEpoch).Seconds())
}

func main() {
	// Sample CSV output:
	// Date,Open,High,Low,Close,Adj Close,Volume
	// 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
	YahooQuoteCsvPattern, err := regexp.Compile(
		`Date,Open,High,Low,Close,Adj Close,Volume\n([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)\n?.*`)
	checkError(err)

	database, err := sql.Open("sqlite3", "../../Banktivity.ibank/accountsData.ibank")
	checkDatabaseError(err, database)

	rows, err := database.Query("SELECT zuniqueid, zsymbol FROM zsecurity")
	checkDatabaseError(err, database)

	var securityid string
	var symbol string
	tx, err := database.Begin()
	checkDatabaseTxError(err, tx, database)
	for rows.Next() {
		err := rows.Scan(&securityid, &symbol)
		checkDatabaseTxError(err, tx, database)
		resp, err := http.Get("https://query1.finance.yahoo.com/v7/finance/download/" + symbol + "?interval=1d&events=history")
		checkDatabaseTxError(err, tx, database)

		if resp.StatusCode == 200 {
			defer resp.Body.Close()
			body, err := ioutil.ReadAll(resp.Body)
			checkDatabaseTxError(err, tx, database)

			cols := YahooQuoteCsvPattern.FindStringSubmatch(string(body))
			d, err := time.Parse(dateLayout, cols[1])
			checkDatabaseTxError(err, tx, database)
			o, err := decimal.NewFromString(cols[2])
			checkDatabaseTxError(err, tx, database)
			h, err := decimal.NewFromString(cols[3])
			checkDatabaseTxError(err, tx, database)
			l, err := decimal.NewFromString(cols[4])
			checkDatabaseTxError(err, tx, database)
			c, err := decimal.NewFromString(cols[5])
			checkDatabaseTxError(err, tx, database)
			vol, err := strconv.ParseInt(cols[6], 10, 32)
			checkDatabaseTxError(err, tx, database)

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
				vol, c, h, l, o, ent, opt, secondsSinceIBankEpoch(d), securityid)
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
					ent, opt, secondsSinceIBankEpoch(d), securityid, vol, c, h, l, o)
				checkDatabaseTxError(err, tx, database)
			    fmt.Println("No existing record, new row inserted...")
			} else {
			    fmt.Println("Existing record updated...")
			}
		}
	}

	tx.Commit()
	database.Close()
	fmt.Println("Done")
}
