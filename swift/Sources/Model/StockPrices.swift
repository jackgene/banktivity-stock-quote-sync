struct StockPrices: Decodable {
    let bySymbol: [String: StockPrice]
}
