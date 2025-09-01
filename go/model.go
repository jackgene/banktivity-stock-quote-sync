package main

import (
	"time"

	"github.com/shopspring/decimal"
)

type StockPrice struct {
	Date   time.Time       `json:"millisSinceEpoch,format:unixmilli"`
	Open   decimal.Decimal `json:"open"`
	High   decimal.Decimal `json:"high"`
	Low    decimal.Decimal `json:"low"`
	Close  decimal.Decimal `json:"close"`
	Volume int             `json:"volume"`
}

type StockPrices struct {
	BySymbol map[string]StockPrice `json:"bySymbol"`
}

type SecurityID struct {
	UniqueId string
	Symbol   string
}

type Security struct {
	Id    SecurityID
	Price StockPrice
}
