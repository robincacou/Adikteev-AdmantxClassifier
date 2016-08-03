package com.adikteev.classifier

import scala.util.{Failure, Success}
import java.net.URLEncoder

import akka.io.IO
import akka.pattern.ask
import org.apache.samza.config.Config
import org.apache.samza.system.IncomingMessageEnvelope

import collection.mutable
import org.apache.samza.task.{TaskContext, _}
import spray.can.Http
import spray.http.{HttpResponse, StatusCodes}
import spray.httpx.RequestBuilding._

import scala.concurrent.Future

class ClassifierStreamTask extends StreamTask with InitableTask with WindowableTask {

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

  override def init(config: Config, context: TaskContext): Unit = {

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
    tokensForNextWindow += (tokensForOneSecond * secondsBetweenWindows)

    val sortedReachs = reachStore.toArray.sortBy(urlReachTupple => urlReachTupple._2)
    val actualTokensSpent = Math.min(sortedReachs.length, tokensForNextWindow)
    val toClassify = sortedReachs.takeRight(actualTokensSpent)

    toClassify.foreach(urlAndReach => {
      val (url: String, reach: Int) = urlAndReach
      callAdmantx(url) onComplete {
        case Success(r: HttpResponse) =>
          Storage.classifStore.put(url, r.entity.asString)
          reachStore.remove(url)
        case Failure(error) =>
          System.err.println("[callAdmantx] Could not classify: " + url + "\n" + error)
      }
    })

    // We consume the tokens for each call, maybe we should not decrement the counter
    // when the admantx call has failed ?
    tokensForNextWindow -= actualTokensSpent
  }
}
