import ArgumentParser
import Foundation
import Logging
import SQLite

@main
struct Main: ParsableCommand {
    // General constants
    static let maxSymbolLength: Int = 5
    
    // Database constants
    static let ent: Int = 42
    static let opt: Int = 1
    static let updateSql: String = """
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
    static let insertSql: String = """
    INSERT INTO zprice (
        z_ent, z_opt, zdate, zsecurityid,
        zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """
    
    static let logger: Logger = {
        var logger = Logger(label: "")
        logger.logLevel = Logger.Level.debug
        
        return logger
    }()
    
    static let configuration: CommandConfiguration = CommandConfiguration(commandName: "ibdq_swift")
    
    @Argument(help: "Path to Banktivity data directory")
    var banktivityDataDir: String
    
    static func readSecurityIds(db: Connection) throws -> [String: String] {
        let securityIds: [String: String] = Dictionary(uniqueKeysWithValues: try db
            .run("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ?", Self.maxSymbolLength)
            .compactMap {(row: Statement.Element) in
                if let uniqueId = row[0] as? String, let symbol = row[1] as? String {
                    return (symbol, uniqueId)
                } else {
                    return nil
                }
            })
        Self.logger.info("Found \(securityIds.count) securities...")
        
        return securityIds
    }
    
    static func getStockPrices(symbols: [String], completionHandler: @escaping @Sendable (Swift.Result<StockPrices, any Swift.Error>) -> Void) {
        let urlSession = URLSession(configuration: URLSessionConfiguration.ephemeral)
        Self.logger.debug("Downloading prices for \(symbols)...")
        let url: URL = URL(string: "https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=\(symbols.joined(separator: ","))")!
        let task: URLSessionDataTask = urlSession.dataTask(with: url) { (data, response, error) in
            if let data = data, let httpResponse = response as? HTTPURLResponse {
                if (httpResponse.statusCode == 200) {
                    let jsonDecoder: JSONDecoder = JSONDecoder()
                    completionHandler(Swift.Result { try jsonDecoder.decode(StockPrices.self, from: data) })
                } else {
                    Self.logger.error("Received bad HTTP response \(httpResponse.statusCode)")
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
    
    static func persistStockPrices(stockPrices: [(SecurityId, StockPrice)], db: Connection) throws -> Void {
        let updateStmt = try db.prepare(Self.updateSql)
        let insertStmt = try db.prepare(Self.insertSql)
        let prePersistTotalChanges: Int = db.totalChanges
        
        for (securityId, stockPrice): (SecurityId, StockPrice) in stockPrices {
            let changes: Int = db.totalChanges
            try updateStmt.run(
                stockPrice.volume,
                "\(stockPrice.close)",
                "\(stockPrice.high)",
                "\(stockPrice.low)",
                "\(stockPrice.open)",
                Self.ent,
                Self.opt,
                stockPrice.date.timeIntervalSinceReferenceDate,
                securityId.uniqueId
            )
            if db.totalChanges > changes {
                Self.logger.debug("Existing entry for \(securityId.symbol) updated...")
            } else {
                try insertStmt.run(
                    Self.ent,
                    Self.opt,
                    stockPrice.date.timeIntervalSinceReferenceDate,
                    securityId.uniqueId,
                    stockPrice.volume,
                    "\(stockPrice.close)",
                    "\(stockPrice.high)",
                    "\(stockPrice.low)",
                    "\(stockPrice.open)"
                )
                Self.logger.debug("New entry for \(securityId.symbol) created...")
            }
        }
        let count: Int = db.totalChanges - prePersistTotalChanges
        try db.run("""
        UPDATE z_primarykey
        SET z_max = (SELECT MAX(z_pk) FROM zprice)
        WHERE z_name = 'Price'
        """
        )
        Self.logger.debug("Primary key for price updated...")
        Self.logger.info("Persisted prices for \(count) securities...")
    }
    
    func run() throws {
        let startSecs = Date().timeIntervalSinceReferenceDate
        let sqliteFile: String = "\(banktivityDataDir)/accountsData.ibank"
        Self.logger.info("Processing SQLite file \(sqliteFile)...")
        let db = try Connection(sqliteFile)
        let securityIdsBySymbol: [String: String] = try Self.readSecurityIds(db: db)
        let done: DispatchSemaphore = DispatchSemaphore(value: 0)
        Self.getStockPrices(symbols: Array(securityIdsBySymbol.keys)) { (stockPrices: Swift.Result<StockPrices, any Swift.Error>) in
            defer { done.signal() }
            switch stockPrices {
            case let .success(stockPrices):
                do {
                    try db.transaction {
                        try Self.persistStockPrices(
                            stockPrices: stockPrices.bySymbol.compactMap { (symbol, stockPrice) in
                                securityIdsBySymbol[symbol].map { uniqueId in
                                    (SecurityId(uniqueId: uniqueId, symbol: symbol), stockPrice)
                                }
                            },
                            db: db
                        )
                    }
                } catch {
                    Self.logger.error("Failed to persist stock prices: \(error)")
                    Self.exit(withError: error)
                }
            case let .failure(error):
                Self.logger.error("Failed to get stock prices: \(error)")
                Self.exit(withError: error)
            }
        }
        done.wait()
        let elapsedSecs = Date().timeIntervalSinceReferenceDate - startSecs
        Self.logger.info("Security prices synchronized in \(String(format: "%.3f", elapsedSecs))s.")
    }
}
