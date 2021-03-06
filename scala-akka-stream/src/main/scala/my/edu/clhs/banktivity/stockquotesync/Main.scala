package my.edu.clhs.banktivity.stockquotesync

import java.nio.file.{Path, Paths}
import java.time.{LocalDate, ZoneId, ZoneOffset}

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpEntity, HttpRequest, HttpResponse, StatusCode, StatusCodes}
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream._
import akka.stream.alpakka.slick.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent._

object Main extends App with LazyLogging {
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
      implicit val session: SlickSession = SlickSession.forConfig("ibankDb")
      system.registerOnTermination(session.close())
      // Sample CSV output:
      // Date,Open,High,Low,Close,Adj Close,Volume
      // 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
      val YahooQuoteCsvPattern =
        """([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)""".r
      import session.profile.api._

      logger.info(s"Processing SQLite file ${ibankFile}...")
      val syncFut: Future[Unit] = for {
        _: Done <- Slick.
          source(sql"SELECT zuniqueid, zsymbol FROM zsecurity".as[(String, String)]).
          mapAsync(4) {
            case (securityId: String, symbol: String) =>
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
                    map { body: ByteString =>
                      Some((securityId, symbol, body.utf8String))
                    }

                case HttpResponse(StatusCodes.NotFound, _, entity: HttpEntity, _) =>
                  entity.discardBytes()
                  Future.successful(None)

                case HttpResponse(statusCode: StatusCode, _, entity: HttpEntity, _) =>
                  entity.discardBytes()
                  Future.failed(new RuntimeException(s"""Received HTTP ${statusCode} for symbol "${symbol}""""))
              }
          }.
          collect {
            case Some((securityId: String, symbol: String, body: String)) =>
              body.split("\n").tail.
                collect {
                  case YahooQuoteCsvPattern(date: String, o: String, h: String, l: String, c: String, vol: String) =>
                    (
                      securityId, symbol, LocalDate.parse(date),
                      BigDecimal(o), BigDecimal(h), BigDecimal(l), BigDecimal(c), vol.toInt
                    )
                }.
                head
          }.
          grouped(10000).
          runWith {
            Slick.sink { batch: Seq[(String, String, LocalDate, BigDecimal, BigDecimal, BigDecimal, BigDecimal, Int)] =>
              logger.info(s"Inserting ${batch.size} prices...")
              DBIO.sequence(
                batch.toVector.map {
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
              ).
              map(_.sum)
            }
          }.
          recoverWith {
            case t: Throwable =>
              logger.error("Stock quote synchronization failed", t)
              Future.failed(t)
          }
      } yield logger.info("Stock quotes synchronized successfully.")

      syncFut.onComplete { _ =>
        http.shutdownAllConnectionPools()
        system.terminate()
      }

    case _ => System.err.println("Please specify path to ibank data file.")
  }
}
