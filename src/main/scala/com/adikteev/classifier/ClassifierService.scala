package com.adikteev.classifier

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.collection.parallel.mutable
import scala.util.{Failure, Success}
import java.net.URLEncoder
import java.util.Properties

import akka.actor.{Actor, ActorSystem}
import akka.pattern.ask
import akka.io.IO
import akka.util.Timeout
import spray.routing._
import spray.http._
import spray.httpx.RequestBuilding._
import spray.can.Http
import spray.util._
import MediaTypes._
import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.BasicConfigurator

class ClassifierServiceActor extends Actor with ClassifierService {

  def actorRefFactory = context

  // Configure log4j
  BasicConfigurator.configure();
  def receive = handleTimeouts orElse runRoute(routes)

  def handleTimeouts: Receive = {
    case Timedout(x: HttpRequest) =>
      System.err.println("[callAdmantx] Timeout")
      complete(StatusCodes.NoContent)
  }
}

trait ClassifierService extends HttpService {
  implicit val system = ActorSystem()
  implicit val timeout = Timeout(5.seconds)

  // This should be moved to an external database to be able to persist
  // the classifications we use, and separate the data from the actor
  val classifStore = mutable.ParHashMap.empty[String, String]

  // Kafka setup
  val props = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("acks", "all")
  props.put("retries", "0")
  props.put("linger.ms", "1")
  props.put("buffer.memory", "256000000")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  val kafkaProducer = new KafkaProducer[String, String](props)

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

              val classifOpt = classifStore.get(url)
              classifOpt match {
                case Some(classif) =>
                  complete(StatusCodes.OK -> classif)
                case None =>
                  // We insert url as value and as key, since we would like to have
                  // each instance of a single URL residing on the same partition
                  kafkaProducer.send(new ProducerRecord[String, String]("urls", url, url))
                  complete(StatusCodes.NoContent)
              }

              // TODO move in the call admantx segment
              /*
              onComplete(callAdmantx(url)) {
                  case Success(r: HttpResponse) =>
                    complete(StatusCodes.OK -> r.entity.asString)
                  case Failure(error) =>
                    System.err.println("[callAdmantx] " + error)
                    complete(StatusCodes.NoContent)
              }
              */
            }
          }
        }
    }
}