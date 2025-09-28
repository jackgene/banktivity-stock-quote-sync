import Foundation

struct Quote: Decodable {
    let close: [Decimal?]?
    let high: [Decimal?]?
    let low: [Decimal?]?
    let open: [Decimal?]?
    let volume: [Int?]?
}

struct Indicators: Decodable {
    let quote: [Quote]
}

struct Result: Decodable {
    let timestamp: [Date]?
    let indicators: Indicators
}

struct Chart: Decodable {
    let result: [Result]?
}

struct HistoryResponse: Decodable {
    let chart: Chart
}
