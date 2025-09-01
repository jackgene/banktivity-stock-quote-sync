package main

import (
	"encoding/json"
	"time"

	"github.com/shopspring/decimal"
)

type jsonEpochTime struct {
	time.Time
}

func (t *jsonEpochTime) SecondsSinceAppleEpoch() int {
	return int(t.Sub(appleEpoch).Seconds())
}

func (t *jsonEpochTime) UnmarshalJSON(data []byte) error {
	var millisSinceEpoch int64
	if err := json.Unmarshal(data, &millisSinceEpoch); err != nil {
		return err
	}
	t.Time = time.UnixMilli(millisSinceEpoch)

	return nil
}

type StockPrice struct {
	Date   jsonEpochTime
	Open   decimal.Decimal
	High   decimal.Decimal
	Low    decimal.Decimal
	Close  decimal.Decimal
	Volume int
}

type StockPrices struct {
	BySymbol map[string]StockPrice
}

type SecurityID struct {
	UniqueId string
	Symbol   string
}

type Security struct {
	Id    SecurityID
	Price StockPrice
}
