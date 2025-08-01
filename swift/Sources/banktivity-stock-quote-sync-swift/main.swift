import Foundation
import Logging
import SQLite

extension FileHandle : @retroactive TextOutputStream {
    public func write(_ string: String) {
        guard let data = string.data(using: .utf8) else { return }
        self.write(data)
    }
}

struct SecurityId {
    let uniqueId: String
    let symbol: String
}

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

struct StockPrices: Decodable {
    let bySymbol: [String: StockPrice]
}

enum Error: Swift.Error {
    case error(String)
}
let startSecs = Date().timeIntervalSinceReferenceDate

// General constants
let maxSymbolLength = 5

// Database constants
let ent = 42
let opt = 1
let updateSql = """
UPDATE zprice
SET
    zvolume = ?,
    zclosingprice = ?,
    zhighprice = ?,
    zlowprice = ?,
    zopeningprice = ?
WHERE
    z_ent = ? AND z_opt = ? AND
    zdate = ? AND zsecurityid = ?
"""
let insertSql = """
INSERT INTO zprice (
    z_ent, z_opt, zdate, zsecurityid,
    zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?
)
"""

// HTTP constants
let httpConcurrency = 4

let logger = { () -> Logger in
    var logger = Logger(label: "banktivity-stock-quote-sync")
    logger.logLevel = Logger.Level.debug

    return logger
}()
var stdErr = FileHandle.standardError

func readSecurityIds(db: Connection) throws -> [String: String] {
    let securityIds: [String: String] = Dictionary(uniqueKeysWithValues: try db
        .prepare("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= \(maxSymbolLength)")
        .compactMap {(row) in
            if let uniqueId = row[0] as? String, let symbol = row[1] as? String {
                return (symbol, uniqueId)
            } else {
                return nil
            }
        })
    logger.info("Found \(securityIds.count) securities...")

    return securityIds
}

func getStockPrices(symbols: [String], completionHandler: @escaping @Sendable (Swift.Result<StockPrices, any Swift.Error>) -> Void) {
    let urlSession = URLSession(configuration: URLSessionConfiguration.ephemeral)
    logger.debug("Downloading prices for \(symbols)...")
    let url: URL = URL(string: "https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=\(symbols.joined(separator: ","))")!
    let task: URLSessionDataTask = urlSession.dataTask(with: url) { (data, response, error) in
        if let data = data, let httpResponse = response as? HTTPURLResponse {
            if (httpResponse.statusCode == 200) {
                let jsonDecoder: JSONDecoder = JSONDecoder()
                completionHandler(Swift.Result { try jsonDecoder.decode(StockPrices.self, from: data) })
            } else {
                logger.error("Received bad HTTP response \(httpResponse.statusCode)")
                completionHandler(.failure(Error.error("Received bad HTTP response \(httpResponse.statusCode)")))
            }
        } else if let error = error {
            completionHandler(.failure(error))
        } else {
            fatalError("URLDataTask is neither successful nor did it fail with an error")
        }
    }
    task.resume()
}

func persistStockPrices(stockPrices: [(SecurityId, StockPrice)], db: Connection) throws -> Void {
    let updateStmt = try db.prepare(updateSql)
    let insertStmt = try db.prepare(insertSql)
    let prePersistTotalChanges: Int = db.totalChanges
    
    for (securityId, stockPrice): (SecurityId, StockPrice) in stockPrices {
        let changes: Int = db.totalChanges
        try updateStmt.run(
            stockPrice.volume,
            "\(stockPrice.close)",
            "\(stockPrice.high)",
            "\(stockPrice.low)",
            "\(stockPrice.open)",
            ent,
            opt,
            stockPrice.date.timeIntervalSinceReferenceDate,
            securityId.uniqueId
        )
        if db.totalChanges > changes {
            logger.debug("Existing entry for \(securityId.symbol) updated...")
        } else {
            try insertStmt.run(
                ent,
                opt,
                stockPrice.date.timeIntervalSinceReferenceDate,
                securityId.uniqueId,
                stockPrice.volume,
                "\(stockPrice.close)",
                "\(stockPrice.high)",
                "\(stockPrice.low)",
                "\(stockPrice.open)"
            )
            logger.debug("New entry for \(securityId.symbol) created...")
        }
    }
    let count: Int = db.totalChanges - prePersistTotalChanges
    try db.run("""
        UPDATE z_primarykey
        SET z_max = (SELECT MAX(z_pk) FROM zprice)
        WHERE z_name = 'Price'
        """
    )
    logger.debug("Primary key for price updated...")
    logger.info("Persisted prices for \(count) securities...")
}

if CommandLine.arguments.count == 2 {
    let iBankDataDir: String = CommandLine.arguments[1]
    let sqliteFile: String = "\(iBankDataDir)/accountsData.ibank"
    logger.info("Processing SQLite file \(sqliteFile)...")
    let db = try Connection(sqliteFile)
    try db.transaction {
        let securityIdsBySymbol: [String: String] = try readSecurityIds(db: db)
        var stockPricesBySymbol: [String: StockPrice]? = nil
        var error: (any Swift.Error)? = nil
        let done: DispatchSemaphore = DispatchSemaphore(value: 0)
        getStockPrices(symbols: Array(securityIdsBySymbol.keys)) { (stockPrices: Swift.Result<StockPrices, any Swift.Error>) in
            defer { done.signal() }
            switch stockPrices {
            case let .success(stockPrices):
                stockPricesBySymbol = stockPrices.bySymbol
            case let .failure(e):
                error = e
            }
        }
        done.wait()
        if let error {
            throw error
        }
        guard let stockPricesBySymbol else {
            throw Error.error("wtf?")
        }
        try persistStockPrices(
            stockPrices: stockPricesBySymbol.compactMap { (symbol, stockPrice) in
                securityIdsBySymbol[symbol].map { uniqueId in
                    (SecurityId(uniqueId: uniqueId, symbol: symbol), stockPrice)
                }
            },
            db: db
        )
    }
    let elapsedSecs = Date().timeIntervalSinceReferenceDate - startSecs
    logger.info("Security prices synchronized in \(String(format: "%.3f", elapsedSecs))s.")
} else {
    print("Please specify path to ibank data file.", to: &stdErr)
    exit(1)
}
