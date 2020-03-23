package my.edu.clhs.banktivity.stockquotesync

import java.nio.file.{Path, Paths}
import java.time.{LocalDate, ZoneId, ZoneOffset}

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging
import slick.jdbc.SQLiteProfile.api._
import slick.jdbc.SQLiteProfile.backend.Database

import scala.concurrent._

object Main extends App with LazyLogging {
  // Sample CSV output:
  // Date,Open,High,Low,Close,Adj Close,Volume
  // 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
  val YahooQuoteCsvPattern =
    """Date,Open,High,Low,Close,Adj Close,Volume\n([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)\n?.*""".r

  def securities(db: Database)(implicit ec: ExecutionContext): Future[Seq[(String,String)]] = {
    for {
      secs: Seq[(String, String)] <- db.run(sql"SELECT zuniqueid, zsymbol FROM zsecurity".as[(String, String)])
      _ = logger.info(s"Found ${secs.size} securities...")
    } yield secs
  }

  def prices(
      http: HttpExt, securityId: String, symbol: String)(
      implicit ec: ExecutionContext, mat: Materializer):
      Future[Option[(String, String, LocalDate, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Int)]] = {
    logger.info(s"Downloading prices for ${symbol}...")
    http.singleRequest(
      HttpRequest(
        uri = s"https://query1.finance.yahoo.com/v7/finance/download/${symbol}?interval=1d&events=history"
      )
    ).
    flatMap {
      case HttpResponse(StatusCodes.OK, _, entity: HttpEntity, _) =>
        entity.dataBytes.
          runFold(ByteString.empty)(_ ++ _).
          map(_.utf8String match {
            case YahooQuoteCsvPattern(d: String, o: String, h: String, l: String, c: String, v: String) =>
              Some(
                (
                  securityId, symbol, LocalDate.parse(d),
                  BigDecimal(o), BigDecimal(h), BigDecimal(l), BigDecimal(c), v.toInt
                )
              )
            case _ => None
          })

      case HttpResponse(StatusCodes.NotFound, _, entity: HttpEntity, _) =>
        entity.discardBytes()
        Future.successful(None)

      case HttpResponse(statusCode: StatusCode, _, entity: HttpEntity, _) =>
        entity.discardBytes()
        Future.failed(new RuntimeException(s"""Received HTTP ${statusCode} for symbol "${symbol}""""))
    }
  }

  def persistPrices(
      db: Database, prices: Seq[(String, String, LocalDate, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Int)])(
      implicit ec: ExecutionContext):
      Future[Unit] = {
    db.run {
      DBIO.sequence(
        prices.toVector.map {
          case (
            securityId: String, _: String, date: LocalDate,
            open: BigDecimal, high: BigDecimal, low: BigDecimal, close: BigDecimal, vol: Int
            ) =>
            val ibankTimestamp: Long = date.
              minusYears(31).plusDays(1).
              atStartOfDay(ZoneId.ofOffset("", ZoneOffset.ofHours(3))).
              toEpochSecond

            sqlu"""
              UPDATE zprice
              SET
                zvolume = ${vol},
                zclosingprice = ${close},
                zhighprice = ${high},
                zlowprice = ${low},
                zopeningprice = ${open}
              WHERE
                z_ent = ${42} AND z_opt = ${1} AND
                zdate = ${ibankTimestamp} AND zsecurityid = ${securityId}
            """.flatMap {
              case 0 =>
                sqlu"""
                  INSERT INTO zprice (
                    z_ent, z_opt, zdate, zsecurityid,
                    zvolume, zclosingprice, zhighprice, zlowprice, zopeningprice
                  ) VALUES (
                    ${42}, ${1}, ${ibankTimestamp}, ${securityId},
                    ${vol}, ${close}, ${high}, ${low}, ${open}
                  )
                """

              case nonzero: Int =>
                DBIO.successful(nonzero)
            }
        }
      )
    }.
    map { updateCounts: Seq[Int] =>
      logger.info(s"Persisted prices for ${updateCounts.sum} securities...")
    }
  }

  args match {
    case Array(ibankFilePath: String, _*) =>
      val ibankFile: Path = Paths.
        get(System.getProperty("user.dir")).
        resolve(Paths.get(ibankFilePath)).
        resolve(Paths.get("accountsData.ibank")).
        normalize
      System.setProperty("ibankFilePath", ibankFile.toString)

      implicit val system: ActorSystem =
      ActorSystem("stock-quote-sync")
      implicit val ec: ExecutionContext = system.dispatcher
      implicit val mat: Materializer = Materializer(system)
      val http: HttpExt = Http(system)
      val db: Database = Database.forConfig("ibankDb")

      logger.info(s"Processing SQLite file ${ibankFile}...")
      (
        for {
          secs: Seq[(String,String)] <- securities(db)
          prices: Seq[(String, String, LocalDate, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Int)] <-
            Future.sequence(
              secs.map { case (securityId: String, symbol: String) => prices(http, securityId, symbol)}
            ).
            map(_.flatten)
          _ <- persistPrices(db, prices)
        } yield logger.info("Security prices synchronized successfully.")
      ).
      recoverWith {
        case t: Throwable =>
          logger.error("Security price synchronization failed", t)
          Future.failed(t)
      }.
      onComplete { _ =>
        http.shutdownAllConnectionPools()
        system.terminate()
      }

    case _ => System.err.println("Please specify path to ibank data file.")
  }
}
