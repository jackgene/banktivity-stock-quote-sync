import Foundation

struct StockPrice: Decodable {
    let millisSinceEpoch: Int64
    let open: Decimal
    let low: Decimal
    let high: Decimal
    let close: Decimal
    let volume: Int
    var date: Date {
        Date(timeIntervalSince1970: TimeInterval(millisSinceEpoch / 1000))
    }
}
