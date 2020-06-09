package my.edu.clhs.banktivity.stockquotesync

import com.github.doyaaaaaken.kotlincsv.dsl.csvReader
import io.ktor.client.HttpClient
import io.ktor.client.engine.apache.Apache
import io.ktor.client.request.get
import io.ktor.client.statement.HttpResponse
import io.ktor.client.statement.readText
import io.ktor.util.KtorExperimentalAPI
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import mu.KLogger
import mu.KotlinLogging
import java.io.OutputStream
import java.math.BigDecimal
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.*
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.logging.ConsoleHandler
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlin.system.measureTimeMillis


data class Price(
    val date: LocalDate,
    val open: BigDecimal,
    val high: BigDecimal,
    val low: BigDecimal,
    val close: BigDecimal,
    val volume: Int
)

data class Security(
    val securityId: String,
    val symbol: String,
    val price: Price? = null
)

const val MAX_SYMBOL_LENGTH = 5
const val ENT = 42
const val OPT = 1
const val READ_SECURITY_SQL = "SELECT zuniqueid, zsymbol FROM zsecurity WHERE LENGTH(zsymbol) <= ${MAX_SYMBOL_LENGTH}"
const val UPDATE_PRICE_SQL = """
UPDATE zprice
SET
    zvolume = ?,
    zclosingprice = ?,
    zhighprice = ?,
    zlowprice = ?,
    zopeningprice = ?
WHERE
    z_ent = ${ENT} AND z_opt = ${OPT} AND
    zdate = ? AND zsecurityid = ?
"""
const val INSERT_PRICE_SQL = """
INSERT INTO zprice (
    z_ent, z_opt, zdate, zsecurityid,
    zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
) VALUES (
    ${ENT}, ${OPT}, ?, ?, ?, ?, ?, ?, ?
)
"""
const val UPDATE_PK_SQL = """
UPDATE z_primarykey
SET z_max = (SELECT MAX(z_pk) FROM zprice)
WHERE z_name = 'Price'
"""

val logger: KLogger = {
    val jdkLogger = Logger.getLogger("ibdq")
    for (handler: Handler in jdkLogger.handlers) {
        jdkLogger.removeHandler(handler)
    }
    jdkLogger.addHandler(object: ConsoleHandler() {
        override fun setOutputStream(out: OutputStream?) {
            super.setOutputStream(System.out)
        }

        override fun setLevel(newLevel: Level?) {
            super.setLevel(Level.ALL)
        }
    })
    jdkLogger.level = Level.ALL
    jdkLogger.useParentHandlers = false

    KotlinLogging.logger("ibdq")
}()

fun readSecurities(conn: Connection): List<Security> {
    conn.createStatement().use { stmt: Statement ->
        stmt.executeQuery(READ_SECURITY_SQL).use { rs: ResultSet ->
            rs.use {
                val securities = generateSequence {
                    if (!it.next()) null
                    else {
                        Security(
                            securityId = it.getString("zuniqueid"),
                            symbol = it.getString("zsymbol")
                        )
                    }
                }.
                toList()
                logger.info { "Found ${securities.size} securities..." }

                return securities
            }
        }
    }
}

suspend fun getStockPrice(http: HttpClient, symbol: String): Price? {
    logger.debug { "Downloading prices for ${symbol}..." }
    val resp: HttpResponse = http.get(
        "https://query1.finance.yahoo.com/v7/finance/download/${symbol}?interval=1d&events=history")
    return if (resp.status.value != 200) null
    else {
        val rawData: String = resp.readText()
        val rows: List<Map<String, String>> = csvReader().readAllWithHeader(rawData)
        val row: Map<String, String> = rows.firstOrNull() ?: return null

        Price(
            date = LocalDate.parse(row["Date"] ?: return null),
            open = BigDecimal(row["Open"] ?: return null),
            high = BigDecimal(row["High"] ?: return null),
            low = BigDecimal(row["Low"] ?: return null),
            close = BigDecimal(row["Close"] ?: return null),
            volume = row["Volume"]?.toInt() ?: return null
        )
    }
}

@KtorExperimentalAPI
fun withStockPrices(securities: List<Security>): List<Security> = runBlocking {
    HttpClient(Apache) {
        expectSuccess = false
        engine {
            pipelining = true
            customizeClient {
                setMaxConnPerRoute(4)
            }
        }
    }.
    use { http: HttpClient ->
        securities.
            map { security: Security ->
                async {
                    val price: Price = getStockPrice(http, security.symbol) ?: return@async null
                    security.copy(price = price)
                }
            }.
            mapNotNull { it.await() }
    }
}

fun secondsSinceAppleEpoch(date: LocalDate): Long {
    return date.
        minusYears(31).
        atStartOfDay(ZoneId.ofOffset("", ZoneOffset.ofHours(-12))).
        toEpochSecond()
}

fun persistStockPrices(conn: Connection, securities: List<Security>) {
    conn.prepareStatement(UPDATE_PRICE_SQL).use { updateStmt: PreparedStatement ->
        conn.prepareStatement(INSERT_PRICE_SQL).use { insertStmt: PreparedStatement ->
            for (security: Security in securities) {
                if (security.price != null) {
                    updateStmt.setInt(1, security.price.volume)
                    updateStmt.setBigDecimal(2, security.price.close)
                    updateStmt.setBigDecimal(3, security.price.high)
                    updateStmt.setBigDecimal(4, security.price.low)
                    updateStmt.setBigDecimal(5, security.price.open)
                    updateStmt.setLong(6, secondsSinceAppleEpoch(security.price.date))
                    updateStmt.setString(7, security.securityId)

                    if (updateStmt.executeUpdate() == 1) {
                        logger.debug { "Existing entry for ${security.symbol} updated..." }
                    } else {
                        insertStmt.setLong(1, secondsSinceAppleEpoch(security.price.date))
                        insertStmt.setString(2, security.securityId)
                        insertStmt.setInt(3, security.price.volume)
                        insertStmt.setBigDecimal(4, security.price.close)
                        insertStmt.setBigDecimal(5, security.price.high)
                        insertStmt.setBigDecimal(6, security.price.low)
                        insertStmt.setBigDecimal(7, security.price.open)
                        if (insertStmt.executeUpdate() == 1) {
                            logger.debug { "New entry for ${security.symbol} created..." }
                        }
                    }
                }
            }
            conn.createStatement().use { stmt: Statement ->
                stmt.executeUpdate(UPDATE_PK_SQL)
                logger.debug { "Primary key for price updated..." }
            }
            logger.info { "Persisted prices for ${securities.size} securities..." }
        }
    }
}

@KtorExperimentalAPI
fun main(args: Array<String>) {
    if (args.size == 1) {
        val elapsedMillis: Long = measureTimeMillis {
            val ibankFilePathSpec: String = args[0]
            val sqliteFilePath: Path = Paths.
                get(System.getProperty("user.dir")).
                resolve(Paths.get(ibankFilePathSpec)).
                resolve(Paths.get("accountsData.ibank")).
                normalize()
            logger.info { "Processing SQLite file ${sqliteFilePath}..." }
            DriverManager.getConnection("jdbc:sqlite:${sqliteFilePath}").use { conn: Connection ->
                conn.autoCommit = false
                val securities: List<Security> = readSecurities(conn)
                val stockPrices: List<Security> = withStockPrices(securities)
                persistStockPrices(conn, stockPrices)
                conn.commit()
            }
        }
        logger.info { "Security prices synchronized in ${"%.3f".format(elapsedMillis / 1000.0)}s." }
        exitProcess(0)
    } else {
        println("Please specify path to ibank data file.")
        exitProcess(1)
    }
}
