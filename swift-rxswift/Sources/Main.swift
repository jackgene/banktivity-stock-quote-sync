import ArgumentParser
import Foundation
import Logging
import RxCocoa
import RxSwift
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
    static let urlSession = URLSession(configuration: URLSessionConfiguration.ephemeral)
    
    static let configuration: CommandConfiguration = CommandConfiguration(commandName: "ibdq_swift")
    
    @Argument(help: "Path to Banktivity data directory")
    var banktivityDataDir: String
    
    static func connectToDatabase(sqliteFilePath: String) -> Single<Connection> {
        Single.create { (observer: (SingleEvent<Connection>) -> Void) in
            observer(Swift.Result { try Connection(sqliteFilePath) })
            return Disposables.create()
        }
    }
    
    static func readSecurityIDs(db: Connection) -> Single<[SecurityID]> {
        db.rx
            .run("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ?", Self.maxSymbolLength)
            .compactMap { (row: Statement.Element) in
                if let uniqueID = row[0] as? String, let symbol = row[1] as? String {
                    return SecurityID(uniqueID: uniqueID, symbol: symbol)
                } else {
                    return nil
                }
            }
            .reduce([]) { (accum: [SecurityID], next: SecurityID) in accum + [ next ] }
            .map { (securityIDs: [SecurityID]) in
                let securityIDsSorted: [SecurityID] = securityIDs.sorted { $0.symbol < $1.symbol }
                Self.logger.info("Found \(securityIDsSorted.count) securities (\(securityIDsSorted.map {$0.symbol}.joined(separator: ", ")))")
                return securityIDsSorted
            }
            .asSingle()
    }
    
    static func getStockPrices(symbols: some Sequence<String>) -> Single<StockPrices> {
        Self.logger.info("Downloading prices")
        let url: URL = URL(string: "https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=\(symbols.joined(separator: ","))")!
        return Self.urlSession.rx.data(request: URLRequest(url: url))
            .map { (data: Data) in
                try JSONDecoder().decode(StockPrices.self, from: data)
            }
            .asSingle()
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
                security.id.uniqueID
            )
            if db.totalChanges > changes {
                Self.logger.debug("Existing entry for \(security.id.symbol) updated")
            } else {
                try insertStmt.run(
                    Self.ent,
                    Self.opt,
                    security.price.date.timeIntervalSinceReferenceDate,
                    security.id.uniqueID,
                    security.price.volume,
                    "\(security.price.close)",
                    "\(security.price.high)",
                    "\(security.price.low)",
                    "\(security.price.open)"
                )
                Self.logger.debug("New entry for \(security.id.symbol) created")
            }
        }
        let count: Int = db.totalChanges - prePersistTotalChanges
        try db.run("""
        UPDATE z_primarykey
        SET z_max = (SELECT MAX(z_pk) FROM zprice)
        WHERE z_name = 'Price'
        """
        )
        Self.logger.debug("Primary key for price updated")
        Self.logger.info("Persisted prices for \(count) securities")
    }
    
    func run() {
        let startSecs: TimeInterval = Date().timeIntervalSinceReferenceDate
        
        let sqliteFilePath: String = "\(banktivityDataDir)/accountsData.ibank"
        Self.logger.info("Processing SQLite file \(sqliteFilePath)")
        
        let disposeBag: DisposeBag = .init()
        Self.connectToDatabase(sqliteFilePath: sqliteFilePath)
            .flatMap { (db: Connection) in
                Self.readSecurityIDs(db: db).flatMap { (securityIDs: [SecurityID]) in
                    Self.getStockPrices(symbols: securityIDs.map { $0.symbol }).map { (stockPrices: StockPrices) in
                        try db.transaction {
                            try Self.persistStockPrices(
                                securities: securityIDs.compactMap { (securityID: SecurityID) in
                                    stockPrices.bySymbol[securityID.symbol].map { (stockPrice: StockPrice) in
                                        Security(id: securityID, price: stockPrice)
                                    }
                                },
                                db: db
                            )
                        }
                    }
                }
            }
            .subscribe(
                onSuccess: { _ in
                    let elapsedSecs = Date().timeIntervalSinceReferenceDate - startSecs
                    Self.logger.info("Securities updated in \(String(format: "%.3f", elapsedSecs))s")
                    Self.exit()
                },
                onFailure: { error in
                    Self.exit(withError: error)
                }
            )
            .disposed(by: disposeBag)
        RunLoop.main.run()
    }
}
