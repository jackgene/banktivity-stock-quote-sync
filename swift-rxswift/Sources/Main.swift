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
    static let Day: TimeInterval = 24 * 60 * 60
    static let MarketTimeZone: TimeZone = .init(identifier: "America/New_York")!
    static let MarketCloseHour: Int = 16
    
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
    static let urlSession: URLSession = {
        let urlSessionCfg = URLSessionConfiguration.ephemeral
        urlSessionCfg.httpMaximumConnectionsPerHost = 4
        urlSessionCfg.httpShouldUsePipelining = true
        
        return URLSession(configuration: urlSessionCfg)
    }()
    static let jsonDecoder: JSONDecoder = {
        let jsonDecoder = JSONDecoder()
        jsonDecoder.dateDecodingStrategy = .secondsSince1970
        
        return jsonDecoder
    }()
    
    static let configuration: CommandConfiguration = CommandConfiguration(commandName: "ibdq_swift")
    
    @Argument(help: "Path to Banktivity data directory")
    var banktivityDataDir: String
    
    static func connectToDatabase(sqliteFilePath: String) -> Single<Connection> {
        Single.create { (observer: (SingleEvent<Connection>) -> Void) in
            observer(Swift.Result { try Connection(sqliteFilePath) })
            return Disposables.create()
        }
    }
    
    static func readSecurityIDs(db: Connection) -> Observable<SecurityID> {
        db.rx
            .run("SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ? ORDER BY zsymbol", maxSymbolLength)
            .compactMap { (row: Statement.Element) in
                if let uniqueID = row[0] as? String, let symbol = row[1] as? String {
                    return SecurityID(uniqueID: uniqueID, symbol: symbol)
                } else {
                    return nil
                }
            }
    }
    
    static func getStockPrice(symbol: String) -> Maybe<StockPrice> {
        Self.logger.info("Downloading price for \(symbol)")
        var nowDateComponents: DateComponents = Calendar.current.dateComponents(in: MarketTimeZone, from: Date())
        let currentHourMarketTimeZone: Int = nowDateComponents.hour!
        nowDateComponents.hour = MarketCloseHour
        nowDateComponents.minute = 0
        nowDateComponents.second = 0
        nowDateComponents.nanosecond = 0
        let queryEndDate: Date = currentHourMarketTimeZone < MarketCloseHour ?
            nowDateComponents.date! - Day : // Market still open, exclude today
            nowDateComponents.date!         // Market closed, include today
        let queryStartDate: Date = queryEndDate - 4 * Day
        let url: URL = URL(
            string: "https://query1.finance.yahoo.com/v8/finance/chart/\(symbol)?" +
            "period1=\(Int(queryStartDate.timeIntervalSince1970))&" +
            "period2=\(Int(queryEndDate.timeIntervalSince1970))&" +
            "interval=1d&includePrePost=false")!
        var request: URLRequest = URLRequest(url: url)
        request.setValue("Mozilla/5.0", forHTTPHeaderField: "User-Agent")
        
        return urlSession.rx.response(request: request)
            .compactMap { (response: HTTPURLResponse, data: Data) in
                if response.statusCode == 200 {
                    let historyResponse: HistoryResponse = try jsonDecoder.decode(HistoryResponse.self, from: data)
                    return historyResponse.chart.result?.first?.timestamp?.last.flatMap { (date: Date) in
                        historyResponse.chart.result?.first?.indicators.quote.last.flatMap { (quote: Quote) -> StockPrice? in
                            guard
                                let open: Decimal = quote.open?.first.flatMap({ $0 }),
                                let low: Decimal = quote.low?.first.flatMap({ $0 }),
                                let high: Decimal = quote.high?.first.flatMap({ $0 }),
                                let close: Decimal = quote.close?.first.flatMap({ $0 }),
                                let volume: Int = quote.volume?.first.flatMap({ $0 })
                            else {
                                return nil
                            }
                            
                            var dateComponents: DateComponents = Calendar.current
                                .dateComponents(in: MarketTimeZone, from: date)
                            dateComponents.hour = MarketCloseHour
                            dateComponents.minute = 0
                            
                            return StockPrice(date: dateComponents.date!, open: open, low: low, high: high, close: close, volume: volume)
                        }
                    }
                } else {
                    return nil
                }
            }
            .asMaybe()
    }
    
    static func persistStockPrices(securities: [Security], db: Connection) throws -> Void {
        let updateStmt = try db.prepare(updateSql)
        let insertStmt = try db.prepare(insertSql)
        let prePersistTotalChanges: Int = db.totalChanges
        
        for security: Security in securities {
            let changes: Int = db.totalChanges
            try updateStmt.run(
                security.price.volume,
                "\(security.price.close)",
                "\(security.price.high)",
                "\(security.price.low)",
                "\(security.price.open)",
                ent,
                opt,
                security.price.date.timeIntervalSinceReferenceDate,
                security.id.uniqueID
            )
            if db.totalChanges > changes {
                logger.debug("Existing entry for \(security.id.symbol) updated")
            } else {
                try insertStmt.run(
                    ent,
                    opt,
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
        logger.debug("Primary key for price updated")
        logger.info("Persisted prices for \(count) securities")
    }
    
    func run() {
        let startSecs: TimeInterval = Date().timeIntervalSinceReferenceDate
        
        let sqliteFilePath: String = "\(banktivityDataDir)/accountsData.ibank"
        Self.logger.info("Processing SQLite file \(sqliteFilePath)")
        
        let disposeBag: DisposeBag = .init()
        Self.connectToDatabase(sqliteFilePath: sqliteFilePath)
            .flatMap { (db: Connection) in
                Self.readSecurityIDs(db: db)
                    .map { (securityID: SecurityID) in
                        Self.getStockPrice(symbol: securityID.symbol)
                            .map { (stockPrice: StockPrice) in
                                Security(id: securityID, price: stockPrice)
                            }
                    }
                    .merge()
                    .reduce([]) { (accum: [Security], security: Security) in
                        accum + [ security ]
                    }
                    .asSingle()
                    .map { (securities: [Security]) in
                        try db.transaction {
                            try Self.persistStockPrices(securities: securities, db: db)
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
                    Self.logger.error("Error: \(error.localizedDescription)")
                    Self.exit(withError: error)
                }
            )
            .disposed(by: disposeBag)
        RunLoop.main.run()
    }
}
