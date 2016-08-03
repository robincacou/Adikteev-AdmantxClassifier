package com.adikteev.classifier

import scala.util.{Failure, Success}
import scala.concurrent.ExecutionContext.Implicits.global
import java.net.URLEncoder

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import org.apache.samza.config.Config
import org.apache.samza.system.IncomingMessageEnvelope

import collection.mutable
import org.apache.samza.task._
import spray.can.Http
import spray.http.{HttpResponse, StatusCodes}
import spray.httpx.RequestBuilding._
import scala.concurrent.duration._

import scala.concurrent.Future

class ClassifierStreamTask extends StreamTask with WindowableTask {
  implicit val system = ActorSystem()
  implicit val timeout = Timeout(5.seconds)

  val tokensForDay = 10000
  val secondsBetweenWindows = 30
  val reachStore = mutable.HashMap.empty[String, Int]

  var tokensForNextWindow = 0

  val tokensForOneSecond = tokensForDay / 86400.0f

  // Counts the number of times each url has been seen
  override def process(envelope: IncomingMessageEnvelope, collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    val url = envelope.getMessage.asInstanceOf[String]

    val currentReach = reachStore.getOrElse(url, 0)
    reachStore.put(url, currentReach + 1)
  }

  def createAdmatxRequest(url: String): String = {
    val json = "{key:\"admantx\",method:\"descriptor\",filter:\"admantx\",cached:\"no\",mode:\"sync\",type:\"URL\"," +
      "\"decorator\":\"json\",body:\"" + url + "\"}"

    URLEncoder.encode(json, "utf-8")
  }

  def callAdmantx(url: String): Future[HttpResponse] = {
    val admantxUrl = "http://eutest01.admantx.com/admantx/service?request="

    val request = createAdmatxRequest(url)
    println(request)
    (IO(Http) ? Get(admantxUrl + request)).mapTo[HttpResponse]
  }

  // Classify the top N reached urls
  override def window(collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    tokensForNextWindow += Math.floor(tokensForOneSecond * secondsBetweenWindows.toFloat).toInt

    val sortedReachs = reachStore.toArray.sortBy(urlReachTupple => urlReachTupple._2)
    var actualTokensSpent = Math.min(sortedReachs.length, tokensForNextWindow)
    val toClassify = sortedReachs.takeRight(actualTokensSpent)

    toClassify.foreach(urlAndReach => {
      val (url: String, reach: Int) = urlAndReach
      callAdmantx(url) onComplete {
        case Success(r: HttpResponse) =>
          Storage.classifStore.put(url, r.entity.asString)
          reachStore.remove(url)
        case Failure(error) =>
          // Since the call failed, we will save this token for the next window
          actualTokensSpent -= 1
          System.err.println("[callAdmantx] Could not classify: " + url + "\n" + error)
      }
    })

    tokensForNextWindow -= actualTokensSpent
  }
}
