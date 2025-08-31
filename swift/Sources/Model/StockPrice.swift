import Foundation

struct StockPrice: Decodable {
    let date: Date
    let open: Decimal
    let low: Decimal
    let high: Decimal
    let close: Decimal
    let volume: Int
    
    enum CodingKeys: CodingKey {
        case millisSinceEpoch
        case open
        case low
        case high
        case close
        case volume
    }
    
    init(from decoder: any Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        self.date = Date(timeIntervalSince1970: try container.decode(Double.self, forKey: .millisSinceEpoch) / 1_000)
        self.open = try container.decode(Decimal.self, forKey: .open)
        self.low = try container.decode(Decimal.self, forKey: .low)
        self.high = try container.decode(Decimal.self, forKey: .high)
        self.close = try container.decode(Decimal.self, forKey: .close)
        self.volume = try container.decode(Int.self, forKey: .volume)
    }
}
