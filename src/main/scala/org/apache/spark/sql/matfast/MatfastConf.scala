package org.apache.spark.sql.matfast
import java.util.Properties

import scala.collection.JavaConverters._
import scala.collection.immutable

/**
  * Created by yongyangyu on 2/16/17.
  */

private[matfast] object MatfastConf {
  private val matfastConfEntries = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, MatfastConfEntry[_]]())

  private[matfast] class MatfastConfEntry[T] private(
                                                      val key: String,
                                                      val defaultValue: Option[T],
                                                      val valueConverter: String => T,
                                                      val stringConverter: T => String,
                                                      val doc: String,
                                                      val isPublic: Boolean) {
    def defaultValueString: String = defaultValue.map(stringConverter).getOrElse("<undefined>")

    override def toString: String = {
      s"MatfastConfEntry(key = $key, defaultValue = $defaultValueString, doc = $doc, isPublic = $isPublic)"
    }
  }

  private[matfast] object MatfastConfEntry {
    private def apply[T](key: String, defaultValue: Option[T], valueConverter: String => T,
                         stringConverter: T => String, doc: String, isPublic: Boolean): MatfastConfEntry[T] =
      matfastConfEntries.synchronized {
        if (matfastConfEntries.containsKey(key)) {
          throw new IllegalArgumentException(s"Duplicate MatfastConfEntry. $key has been registered")
        }
        val entry =
          new MatfastConfEntry[T](key, defaultValue, valueConverter, stringConverter, doc, isPublic)
        matfastConfEntries.put(key, entry)
        entry
      }

    def intConf(key: String, defaultValue: Option[Int] = None,
                doc: String = "", isPublic: Boolean = true): MatfastConfEntry[Int] =
      MatfastConfEntry(key, defaultValue, { v =>
        try {
          v.toInt
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be int, but was $v")
        }
      }, _.toString, doc, isPublic)

    def longConf(key: String, defaultValue: Option[Long] = None,
                 doc: String = "", isPublic: Boolean = true): MatfastConfEntry[Long] =
      MatfastConfEntry(key, defaultValue, { v =>
        try {
          v.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be long, but was $v")
        }
      }, _.toString, doc, isPublic)

    def doubleConf(key: String, defaultValue: Option[Double] = None,
                   doc: String = "", isPublic: Boolean = true): MatfastConfEntry[Double] =
      MatfastConfEntry(key, defaultValue, { v =>
        try {
          v.toDouble
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be double, but was $v")
        }
      }, _.toString, doc, isPublic)

    def booleanConf(key: String, defaultValue: Option[Boolean] = None,
                    doc: String = "", isPublic: Boolean = true): MatfastConfEntry[Boolean] =
      MatfastConfEntry(key, defaultValue, { v =>
        try {
          v.toBoolean
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(s"$key should be boolean, but was $v")
        }
      }, _.toString, doc, isPublic)

    def stringConf(key: String, defaultValue: Option[String] = None,
                   doc: String = "", isPublic: Boolean = true): MatfastConfEntry[String] =
      MatfastConfEntry(key, defaultValue, v => v, v => v, doc, isPublic)
  }

  import MatfastConfEntry._

  val SAMPLE_RATE = doubleConf("matfast.sampleRate", defaultValue = Some(0.05))
}

private[matfast] class MatfastConf extends Serializable {
  import MatfastConf._

  @transient protected[matfast] val settings = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  private[matfast] def sampleRate: Double = getConf(SAMPLE_RATE)

  def setConf(props: Properties): Unit = settings.synchronized {
    props.asScala.foreach { case (k, v) => setConfString(k, v)}
  }

  def setConfString(key: String, value: String): Unit = {
    require(key != null, "key cannot be null")
    require(value != null, s"value cannot be null for key: $key")
    val entry = matfastConfEntries.get(key)
    if (entry != null) {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(value)
    }
    settings.put(key, value)
  }

  def setConf[T](entry: MatfastConfEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    require(matfastConfEntries.get(entry.key) == entry, s"$entry is not registered")
    settings.put(entry.key, entry.stringConverter(value))
  }

  def getConfString(key: String): String = {
    Option(settings.get(key)).
      orElse {
        // Try to use the default value
        Option(matfastConfEntries.get(key)).map(_.defaultValueString)
      }.
      getOrElse(throw new NoSuchElementException(key))
  }

  def getConf[T](entry: MatfastConfEntry[T], defaultValue: T): T = {
    require(matfastConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).getOrElse(defaultValue)
  }

  def getConf[T](entry: MatfastConfEntry[T]): T = {
    require(matfastConfEntries.get(entry.key) == entry, s"$entry is not registered")
    Option(settings.get(entry.key)).map(entry.valueConverter).orElse(entry.defaultValue).
      getOrElse(throw new NoSuchElementException(entry.key))
  }

  def getConfString(key: String, defaultValue: String): String = {
    val entry = matfastConfEntries.get(key)
    if (entry != null && defaultValue != "<undefined>") {
      // Only verify configs in the SimbaConf object
      entry.valueConverter(defaultValue)
    }
    Option(settings.get(key)).getOrElse(defaultValue)
  }

  def getAllConfs: immutable.Map[String, String] =
    settings.synchronized { settings.asScala.toMap }

  def getAllDefinedConfs: Seq[(String, String, String)] = matfastConfEntries.synchronized {
    matfastConfEntries.values.asScala.filter(_.isPublic).map { entry =>
      (entry.key, entry.defaultValueString, entry.doc)
    }.toSeq
  }

  private[matfast] def unsetConf(key: String): Unit = {
    settings.remove(key)
  }

  private[matfast] def unsetConf(entry: MatfastConfEntry[_]): Unit = {
    settings.remove(entry.key)
  }

  private[matfast] def clear(): Unit = {
    settings.clear()
  }
}