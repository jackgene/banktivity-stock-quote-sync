import CSV
import Foundation
import Logging
import SQLite

extension FileHandle : TextOutputStream {
    public func write(_ string: String) {
        guard let data = string.data(using: .utf8) else { return }
        self.write(data)
    }
}

struct StockPrice {
    var securityId: String
    var symbol: String
    var date: Date
    var open: Decimal
    var high: Decimal
    var low: Decimal
    var close: Decimal
    var volume: Int
}

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
    $1, $2, $3, $4, $5, $6, $7, $8, $9
)
"""

let logger = Logger(label: "banktivity-stock-quote-sync")
var stdErr = FileHandle.standardError
var stockPrices: [StockPrice] = [StockPrice]() // There's got to be a better way to do this.

func readSecurities(db: Connection) throws -> [(String, String)] {
    return try db.prepare("SELECT zuniqueid, zsymbol FROM zsecurity").compactMap {(row) in
        if let securityId = row[0] as? String, let symbol = row[1] as? String {
            return (securityId, symbol)
        } else {
            return nil
        }
    }
}

func getStockPrices(securities: [(String, String)]) throws ->  DispatchSemaphore {
    let doneSem: DispatchSemaphore = DispatchSemaphore(value: 0)
    let httpSem: DispatchSemaphore = DispatchSemaphore(value: 0)
    let urlSessionCfg = URLSessionConfiguration.ephemeral
    urlSessionCfg.httpMaximumConnectionsPerHost = 4
    let urlSession = URLSession(configuration: urlSessionCfg)
    let dateFormatter = DateFormatter()
    dateFormatter.locale = Locale(identifier: "en_US_POSIX") // set locale to reliable US_POSIX
    dateFormatter.dateFormat = "yyyy-MM-dd"
    dateFormatter.timeZone = TimeZone(secondsFromGMT: -12 * 60 * 60)
    for (uuid, symbol) in securities {
        logger.info("Downloading prices for \(symbol)...")
        let url: URL = URL(string: "https://query1.finance.yahoo.com/v7/finance/download/\(symbol)?interval=1d&events=history")!
        let task: URLSessionDataTask = urlSession.dataTask(with: url) {(data, response, error) in
            defer { httpSem.signal() }
            if let data = data, let httpResponse = response as? HTTPURLResponse {
                if (httpResponse.statusCode == 200) {
                    let stockPriceCsv = try! CSVReader(string: String(data: data, encoding: .utf8)!, hasHeaderRow: true)
                    if stockPriceCsv.next() != nil {
                        stockPrices.append(
                            StockPrice(
                                securityId: uuid,
                                symbol: symbol,
                                date: dateFormatter.date(from: stockPriceCsv["Date"]!)!,
                                open: Decimal(string: stockPriceCsv["Open"]!)!,
                                high: Decimal(string: stockPriceCsv["High"]!)!,
                                low: Decimal(string: stockPriceCsv["Low"]!)!,
                                close: Decimal(string: stockPriceCsv["Close"]!)!,
                                volume: Int(stockPriceCsv["Volume"]!)!
                            )
                        )
                    }
                } else if (httpResponse.statusCode != 404) {
                    logger.error("Received bad HTTP response \(httpResponse.statusCode)")
                }
            }
        }
        task.resume()
    }
    DispatchQueue.global().async {
        for _ in securities {
            httpSem.wait()
        }
        doneSem.signal()
    }

    return doneSem
}

func persistStockPrices(db: Connection) throws -> Void {
    let updateStmt = try db.prepare(updateSql)
    let insertStmt = try db.prepare(insertSql)
    let prePersistTotalChanges: Int = db.totalChanges
    
    for stockPrice: StockPrice in stockPrices {
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
            stockPrice.securityId
        )
        if db.totalChanges == changes {
            try insertStmt.run(
                ent,
                opt,
                stockPrice.date.timeIntervalSinceReferenceDate,
                stockPrice.securityId,
                stockPrice.volume,
                "\(stockPrice.close)",
                "\(stockPrice.high)",
                "\(stockPrice.low)",
                "\(stockPrice.open)"
            )
            logger.info("No existing record for \(stockPrice.symbol), new row inserted...")
        } else {
            logger.info("Existing record for \(stockPrice.symbol) updated...")
        }
    }
    let count: Int = db.totalChanges - prePersistTotalChanges
    try db.run("""
        UPDATE z_primarykey
        SET z_max = (SELECT MAX(z_pk) FROM zprice)
        WHERE z_name = 'Price'
        """
    )
    logger.info("Persisted prices for \(count) securities...")
}

if CommandLine.arguments.count == 2 {
    let iBankDataDir: String = CommandLine.arguments[1]
    let iBankDataFile: String = "\(iBankDataDir)/accountsData.ibank"
    logger.info("Processing SQLite file \(iBankDataFile)...")
    let db = try Connection(iBankDataFile)
    try db.transaction {
        let securities: [(String, String)] = try readSecurities(db: db)
        logger.info("Found \(securities.capacity) securities...")
        let pricesSem: DispatchSemaphore = try getStockPrices(securities: securities)
        pricesSem.wait()
        try persistStockPrices(db: db)
    }
    logger.info("Security prices synchronized successfully.")
} else {
    print("Please specify path to ibank data file.", to: &stdErr)
    exit(1)
}
