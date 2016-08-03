package com.adikteev.classifier

import scala.collection.parallel.mutable

object Storage {
  // This should be moved to an external database to be able to persist
  // the classifications we bought
  val classifStore = mutable.ParHashMap.empty[String, String]
}
