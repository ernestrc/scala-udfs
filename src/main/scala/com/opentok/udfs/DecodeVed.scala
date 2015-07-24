package com.opentok.udfs

import java.io.FileOutputStream

import com.opentok.udfs.Ved.VedMessage
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.joda.time.{DateTimeZone, DateTime}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.{Failure, Try}

/**
 * Inspired by: https://moz.com/blog/inside-googles-ved-parameter
 * Compile protocol buffer first: protoc --java_out=src/main/java src/main/resources/ved.proto
 */
class DecodeVed extends GenericUDF {

  object Decoder {

    val log = Logger(LoggerFactory.getLogger("VedDecoder"))

    val stringOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING)
    val longOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG)
    val boolOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN)

    val TYPES = Map[String, ObjectInspector]("link_index" → stringOI,
      "link_type" → stringOI, "sub_result_position" → longOI,
      "result_position" → longOI, "page" → stringOI, "timestamp" → stringOI,
      "dt" → stringOI, "mysterious1" → longOI, "mysterious2" → longOI
    )

    implicit class Translator(n: Long) {

      def parseIndex: String = Try(n match {
        case a if a > 85 ⇒ "right_hand_column_or_bottom"
        case a if a > 45 ⇒ "main_column"
        case _ ⇒ null
      }).getOrElse(null)

      def parsePage(v: Ved.VedMessage): String = Try {
        if (!v.hasStartResultPosition && v.getLinkType == Ved.VedMessage.Type.sponsored_search_result) "page_1"
        else if (v.hasStartResultPosition) "page_" + (n / 10 + 1)
        else null
      }.getOrElse(null)

      def toDateTime(pattern: String): String = Try {
        log.info(s"Converting microseconds since epoch ($n) into timestamp")
        new DateTime(n / 1000, DateTimeZone.UTC)
          .toDateTime(DateTimeZone.forID("America/Los_Angeles"))
          .toString(pattern)
      }.getOrElse(null)

      def toNonZeroBasedNum: Long = n + 1

    }

    def decodePlainText(msg: String): Try[Ved.VedMessage] = Try {
      val builder = VedMessage.newBuilder()
      msg.split(",").map(_.split(":")).map { tok ⇒
        (tok(0), tok(1)) match {
          case ("i", v) ⇒ builder.setLinkIndex(v.toLong)
          case ("r", v) ⇒ builder.setResultPosition(v.toLong)
          case ("t", v) ⇒ builder.setLinkType(Ved.VedMessage.Type.valueOf(v.toInt))
          case ("s", v) ⇒ builder.setStartResultPosition(v.toLong)
        }
      }
      builder.build()
    }.map { msg ⇒
      log.info(s"Successfully decoded ved message from plain text:\n$msg")
      msg
    }

    def decodeProtobuf(msg: String, manipulate: String ⇒ String = s ⇒ s): Try[Ved.VedMessage] = Try {
      val binary = new sun.misc.BASE64Decoder().decodeBuffer(manipulate(msg.map {
        case '-' ⇒ '+'
        case '_' ⇒ '/'
        case c ⇒ c
      }))
      Ved.VedMessage.parseFrom(binary)
    }.map { msg ⇒
      log.info(s"Successfully decoded ved message from protobuf bin:\n$msg")
      msg
    }

    def toArray(v: VedMessage): Try[Array[Any]] = Try {
      Array(
        if (v.hasLinkIndex) v.getLinkIndex.parseIndex else null, //getField returns 0 or base enum if hasField is false. In hive we prefer NULL when data is not available
        if (v.hasLinkType) v.getLinkType.toString else null,
        if (v.hasSubResultPosition) v.getSubResultPosition.toNonZeroBasedNum else null,
        if (v.hasResultPosition) v.getResultPosition.toNonZeroBasedNum else null,
        v.getStartResultPosition.parsePage(v),
        if (v.getUnknownMsg.getNestedUnknownMsg.hasTs) v.getUnknownMsg.getNestedUnknownMsg.getTs.toDateTime("yyyy-MM-dd HH:mm:ss.SSS") else null,
        if (v.getUnknownMsg.getNestedUnknownMsg.hasTs) v.getUnknownMsg.getNestedUnknownMsg.getTs.toDateTime("yyyy-MM-dd") else null,
        if (v.getUnknownMsg.getNestedUnknownMsg.hasUnknown1) v.getUnknownMsg.getNestedUnknownMsg.getUnknown1 else null,
        if (v.getUnknownMsg.getNestedUnknownMsg.hasUnknown2) v.getUnknownMsg.getNestedUnknownMsg.getUnknown2 else null
      )
    }

    @tailrec
    def decode(msg: String): Try[Array[Any]] = {
      log.info(s"Starting decoding of $msg")
      msg.headOption match {
        case Some('1') ⇒ decodePlainText(msg.tail).flatMap(toArray)
        case Some('0') ⇒
          decodeProtobuf(msg.tail).orElse(decodeProtobuf(msg.tail, { s ⇒
            s + "=" * (4 - s.length % 4) //add padding
          })).flatMap(toArray)
        case Some(_) ⇒ decode(msg.tail)
        case None ⇒ Try(new Array[Any](TYPES.toList.length))
      }
    }

    def apply(msg: String): Array[Any] =
      decode(msg).recover {
        case e: Throwable ⇒
          log.error(s"There was an error when decoding ved code: $e")
          new Array[Any](TYPES.toList.length)
      }.get
  }

  private var inputInspector: PrimitiveObjectInspector = _

  def initialize(inputs: Array[ObjectInspector]): StructObjectInspector = {
    this.inputInspector = inputs(0).asInstanceOf[PrimitiveObjectInspector]
    val outputFieldNames = Decoder.TYPES.keys.toList
    val outputInspectors = Decoder.TYPES.values.toList
    ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors)
  }

  def getDisplayString(children: Array[String]): String = {
    "deserialize(" + children.mkString(",") + ")"
  }

  def evaluate(args: Array[DeferredObject]): Object = {
    val input = inputInspector.getPrimitiveJavaObject(args(0).get)
    Decoder(input.asInstanceOf[String])
  }
}