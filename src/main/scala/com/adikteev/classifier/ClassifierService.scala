package com.adikteev.classifier

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import java.util.Properties

import akka.actor.{Actor, ActorSystem}
import akka.util.Timeout
import spray.routing._
import spray.http._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.BasicConfigurator

class ClassifierServiceActor extends Actor with ClassifierService {

  def actorRefFactory = context

  // Configure log4j
  BasicConfigurator.configure()
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

  def routes: Route =
    path("classify") {
        get {
          parameter("url".as[String]) { url =>
            respondWithMediaType(MediaTypes.`application/json`) {

              val classifOpt = Storage.classifStore.get(url)
              classifOpt match {
                case Some(classif) =>
                  complete(StatusCodes.OK -> classif)
                case None =>
                  // We insert url as value and as key, since we would like to have
                  // each instance of a single URL residing on the same partition
                  kafkaProducer.send(new ProducerRecord[String, String]("urls", url, url))
                  complete(StatusCodes.NoContent)
              }
            }
          }
        }
    }
}