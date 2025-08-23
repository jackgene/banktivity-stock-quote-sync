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
    
    static func readSecurityIds(db: Connection) -> Single<[String: String]> {
        db.rx
            .run("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ?", Self.maxSymbolLength)
            .compactMap { (row: Statement.Element) in
                if let uniqueId = row[0] as? String, let symbol = row[1] as? String {
                    return (symbol, uniqueId)
                } else {
                    return nil
                }
            }
            .reduce([]) { (accum: [(String, String)], next: (String, String)) in accum + [ next ] }
            .map { (symbolsAndSecurityIds: [(String, String)]) in
                Self.logger.info("Found \(symbolsAndSecurityIds.count) securities...")
                return Dictionary(uniqueKeysWithValues: symbolsAndSecurityIds)
            }
            .asSingle()
    }
    
    static func getStockPrices(symbols: some Sequence<String>) -> Single<StockPrices> {
        Self.logger.debug("Downloading prices for \(symbols)...")
        let url: URL = URL(string: "https://sparc-service.herokuapp.com/js/stock-prices.js?symbols=\(symbols.joined(separator: ","))")!
        return Self.urlSession.rx.data(request: URLRequest(url: url))
            .map { (data: Data) in
                try JSONDecoder().decode(StockPrices.self, from: data)
            }
            .asSingle()
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
    
    func run() {
        let startSecs: TimeInterval = Date().timeIntervalSinceReferenceDate
        
        let sqliteFilePath: String = "\(banktivityDataDir)/accountsData.ibank"
        Self.logger.info("Processing SQLite file \(sqliteFilePath)...")
        
        let disposeBag: DisposeBag = .init()
        Self.connectToDatabase(sqliteFilePath: sqliteFilePath)
            .flatMap { (db: Connection) in
                Self.readSecurityIds(db: db).flatMap { (securityIdsBySymbol: [String: String]) in
                    Self.getStockPrices(symbols: securityIdsBySymbol.keys).map { (stockPrices: StockPrices) in
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
                    }
                }
            }
            .subscribe(
                onSuccess: { _ in
                    let elapsedSecs = Date().timeIntervalSinceReferenceDate - startSecs
                    Self.logger.info("Security prices synchronized in \(String(format: "%.3f", elapsedSecs))s.")
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
