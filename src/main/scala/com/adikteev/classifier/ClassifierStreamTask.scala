package com.adikteev.classifier

import org.apache.samza.config.Config
import org.apache.samza.system.IncomingMessageEnvelope

import collection.mutable
import org.apache.samza.task.{TaskContext, _}

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

  // Classify the top N reached urls
  override def window(collector: MessageCollector, coordinator: TaskCoordinator): Unit = {
    tokensForNextWindow += (tokensForOneSecond * secondsBetweenWindows)

    val sortedReachs = reachStore.toArray.sortBy(urlReachTupple => urlReachTupple._2)
    val actualTokensSpent = Math.min(sortedReachs.length, tokensForNextWindow)
    sortedReachs.takeRight(actualTokensSpent)

    // TODO: call admantx and add to classifStore

    tokensForNextWindow -= actualTokensSpent
  }
}
