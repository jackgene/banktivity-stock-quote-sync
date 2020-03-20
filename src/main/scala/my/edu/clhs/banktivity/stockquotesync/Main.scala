package my.edu.clhs.banktivity.stockquotesync

import java.time.{LocalDate, ZoneId, ZoneOffset}

import akka.Done
import akka.actor.ActorSystem
import akka.http.scaladsl.model.{HttpRequest, HttpResponse, StatusCodes}
import akka.http.scaladsl.{Http, HttpExt}
import akka.stream._
import akka.stream.alpakka.slick.scaladsl._
import akka.util.ByteString
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent._

object Main extends App with LazyLogging {
  System.setProperty("ibankFilePath", args(0))

  implicit private val system: ActorSystem = ActorSystem("stock-quote-sync")
  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val mat: Materializer = Materializer(system)
  private val http: HttpExt = Http(system)
  implicit private val session: SlickSession = SlickSession.forConfig("ibankDb")
  system.registerOnTermination(session.close())
  // Sample CSV output:
  // Date,Open,High,Low,Close,Adj Close,Volume
  // 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
  private val QuoteRegex =
    """([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+)""".r

  import session.profile.api._

  for {
    _: Done <- Slick.
      source(sql"SELECT zuniqueid, zsymbol FROM zsecurity".as[(String,String)]).
      mapAsync(4) {
        case (securityId: String, symbol: String) => http.
          singleRequest(
            HttpRequest(
              uri = s"https://query1.finance.yahoo.com/v7/finance/download/${symbol}?interval=1d&events=history"
            )
          ).
          flatMap { httpResponse: HttpResponse =>
            if (httpResponse.status == StatusCodes.OK) {
              httpResponse.entity.dataBytes.
                runFold(ByteString.empty)(_ ++ _).
                map { body: ByteString =>
                  Some((securityId, symbol, body.utf8String))
                }
            } else {
              httpResponse.discardEntityBytes()
              Future.successful(None)
            }
          }
      }.
      collect {
        case Some((securityId: String, symbol: String, body: String)) =>
          body.split("\n").tail.
            collect {
              case QuoteRegex(date: String, o: String, h: String, l: String, c: String, _: String, vol: String) =>
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
          logger.info(s"Inserting ${batch.size} stock quotes...")
          DBIO.sequence(
            batch.toVector.map {
              case (
                  securityId: String, _: String, date: LocalDate,
                  open: BigDecimal, high: BigDecimal, low: BigDecimal, close: BigDecimal, vol: Int
                  ) =>
                val ibankTimestamp: Long = date.
                  minusYears(31).
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
                """.
                flatMap {
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
          map(_.size)
        }
      }.
      recover {
        case t: Throwable =>
          t.printStackTrace()
          Done
      }
    _ <- http.shutdownAllConnectionPools()
  } {
    logger.info("Stock quotes updated successfully.")
    system.terminate()
  }
}
