package com.example

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.util.{Failure, Success}
import akka.actor.{Actor, ActorSystem}
import akka.pattern.ask
import spray.routing._
import spray.http._
import HttpMethods._
import spray.httpx.RequestBuilding._
import spray.util._
import MediaTypes._
import java.net.URLEncoder

import akka.io.IO
import akka.util.Timeout
import com.alibaba.fastjson.JSONObject
import spray.can.Http

class ClassifierServiceActor extends Actor with ClassifierService {

  def actorRefFactory = context

  def receive = runRoute(routes)
}

trait ClassifierService extends HttpService {
  implicit val system = ActorSystem()
  implicit val timeout = Timeout(5.seconds)

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

  def routes: Route =
    path("classify") {
        get {
          parameter("url".as[String]) { url =>
            respondWithMediaType(MediaTypes.`application/json`) {

              onComplete(callAdmantx(url)) {
                  case Success(r: HttpResponse) =>
                    complete(StatusCodes.OK -> r.entity.asString)
                  case Failure(error) =>
                    System.err.println("[callAdmantx] " + error)
                    complete(StatusCodes.NoContent)
              }
            }
          }
        }
    }
}