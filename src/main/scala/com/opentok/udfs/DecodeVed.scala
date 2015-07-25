package com.opentok.udfs

import com.opentok.udfs.Ved.VedMessage
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.joda.time.{DateTime, DateTimeZone}
import org.slf4j.LoggerFactory

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Inspired by: https://moz.com/blog/inside-googles-ved-parameter
 *
 * Compile .proto file with protoc compiler v2.4.1:
 * protoc --java_out=src/main/java src/main/resources/ved.proto
 */
class DecodeVed extends GenericUDF {

  val log = Logger(LoggerFactory.getLogger("VedDecoder"))

  object Decoder {

    /**
     * Attempts to interpret and translate ved messages.
     * Note that the heavy use of null is due to compatibility with hive
     */
    implicit class PimpedVedMessage(v: Ved.VedMessage) {

      private def toDateTime(n: Long, pattern: String): String = Try {
        log.info(s"Converting microseconds since epoch ($n) into timestamp")
        new DateTime(n / 1000, DateTimeZone.UTC)
          .toDateTime(DateTimeZone.forID("America/Los_Angeles"))
          .toString(pattern)
      }.getOrElse(null)

      def linkIndex: String = if (v.hasLinkIndex) v.getLinkIndex match {
        case a if a > 85 ⇒ "right_hand_column_or_bottom"
        case a if a > 45 ⇒ "main_column"
        case _ ⇒ null
      } else null

      def linkType: String = if (v.hasLinkType) v.getLinkType.toString else null

      def subResultPosition: String =
        if (v.hasSubResultPosition) (v.getSubResultPosition + 1).toString else null

      def resultPosition: String =
        if (v.hasResultPosition) (v.getResultPosition + 1).toString else null

      def page: String = Try(v.getLinkType match {
        case Ved.VedMessage.Type.sponsored_search_result ⇒ "page_1"
        case _ if v.hasStartResultPosition ⇒ "page_" + (v.getStartResultPosition / 10 + 1)
        case _ ⇒ null
      }).getOrElse(null)

      def timestamp: String =
        if (v.getMysteriousMsg.getNestedMysteriousMsg.hasTs)
          toDateTime(v.getMysteriousMsg.getNestedMysteriousMsg.getTs, "yyyy-MM-dd HH:mm:ss.SSS")
        else null

      def dt: String =
        if (v.getMysteriousMsg.getNestedMysteriousMsg.hasTs)
          toDateTime(v.getMysteriousMsg.getNestedMysteriousMsg.getTs, "yyyy-MM-dd")
        else null

      def mysterious1: String =
        if (v.getMysteriousMsg.getNestedMysteriousMsg.hasMysterious1)
          v.getMysteriousMsg.getNestedMysteriousMsg.getMysterious1.toString
        else null

      def mysterious2: String =
        if (v.getMysteriousMsg.getNestedMysteriousMsg.hasMysterious2)
          v.getMysteriousMsg.getNestedMysteriousMsg.getMysterious2.toString
        else null

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
      Array(v.linkIndex, v.linkType, v.subResultPosition, v.resultPosition,
        v.page, v.timestamp, v.dt, v.mysterious1, v.mysterious2)
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
        case None ⇒ Try(new Array[Any](FIELDS.length))
      }
    }

    def apply(msg: String): Array[Any] =
      decode(msg).recover {
        case e: Throwable ⇒
          log.error(s"There was an error when decoding ved code: $e")
          new Array[Any](FIELDS.length)
      }.get
  }

  private var inputInspector: PrimitiveObjectInspector = _

  val stringOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING)
  val FIELDS = List("link_index", "link_type", "sub_result_position", "result_position", "page", "timestamp", "dt", "mysterious1", "mysterious2")

  def initialize(inputs: Array[ObjectInspector]): StructObjectInspector = {
    this.inputInspector = inputs(0).asInstanceOf[PrimitiveObjectInspector]
    val outputInspectors = List(stringOI, stringOI, stringOI, stringOI, stringOI, stringOI, stringOI, stringOI, stringOI)
    ObjectInspectorFactory.getStandardStructObjectInspector(FIELDS, outputInspectors)
  }

  def getDisplayString(children: Array[String]): String = {
    "deserialize(" + children.mkString(",") + ")"
  }

  def evaluate(args: Array[DeferredObject]): Object = {
    val input = inputInspector.getPrimitiveJavaObject(args(0).get)
    Decoder(input.asInstanceOf[String])
  }
}