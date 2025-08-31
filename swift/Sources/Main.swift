import ArgumentParser
import Foundation
import Logging
import SQLite
import enum Swift.Result

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
    
    static func readSecurityIds(db: Connection) throws -> [SecurityId] {
        let securityIds: [SecurityId] = try db
            .run("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ?", Self.maxSymbolLength)
            .compactMap {(row: Statement.Element) in
                if let uniqueId = row[0] as? String, let symbol = row[1] as? String {
                    return SecurityId(uniqueId: uniqueId, symbol: symbol)
                } else {
                    return nil
                }
            }
        Self.logger.info("Found \(securityIds.count) securities...")
        
        return securityIds
    }
    
    static func getStockPrices(symbols: [String], completionHandler: @escaping @Sendable (Result<StockPrices, any Swift.Error>) -> Void) {
        let urlSession = URLSession(configuration: URLSessionConfiguration.ephemeral)
        Self.logger.debug("Downloading prices for \(symbols)...")
        let url: URL = URL(string: "https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=\(symbols.joined(separator: ","))")!
        let task: URLSessionDataTask = urlSession.dataTask(with: url) { (data, response, error) in
            if let data = data, let httpResponse = response as? HTTPURLResponse {
                if (httpResponse.statusCode == 200) {
                    let jsonDecoder: JSONDecoder = JSONDecoder()
                    completionHandler(Result { try jsonDecoder.decode(StockPrices.self, from: data) })
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
    
    static func persistStockPrices(securities: [Security], db: Connection) throws -> Void {
        let updateStmt = try db.prepare(Self.updateSql)
        let insertStmt = try db.prepare(Self.insertSql)
        let prePersistTotalChanges: Int = db.totalChanges
        
        for security: Security in securities {
            let changes: Int = db.totalChanges
            try updateStmt.run(
                security.price.volume,
                "\(security.price.close)",
                "\(security.price.high)",
                "\(security.price.low)",
                "\(security.price.open)",
                Self.ent,
                Self.opt,
                security.price.date.timeIntervalSinceReferenceDate,
                security.id.uniqueId
            )
            if db.totalChanges > changes {
                Self.logger.debug("Existing entry for \(security.id.symbol) updated...")
            } else {
                try insertStmt.run(
                    Self.ent,
                    Self.opt,
                    security.price.date.timeIntervalSinceReferenceDate,
                    security.id.uniqueId,
                    security.price.volume,
                    "\(security.price.close)",
                    "\(security.price.high)",
                    "\(security.price.low)",
                    "\(security.price.open)"
                )
                Self.logger.debug("New entry for \(security.id.symbol) created...")
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
        let securityIds: [SecurityId] = try Self.readSecurityIds(db: db)
        let done: DispatchSemaphore = DispatchSemaphore(value: 0)
        Self.getStockPrices(symbols: securityIds.map { $0.symbol }) { (stockPrices: Result<StockPrices, any Swift.Error>) in
            defer { done.signal() }
            switch stockPrices {
            case let .success(stockPrices):
                do {
                    try db.transaction {
                        try Self.persistStockPrices(
                            securities: securityIds.compactMap { (securityId: SecurityId) in
                                stockPrices.bySymbol[securityId.symbol].map { (stockPrice: StockPrice) in
                                    Security(id: securityId, price: stockPrice)
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
