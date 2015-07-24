package com.opentok.udfs

import com.opentok.udfs.Ved.VedMessage
import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Inspired by: https://moz.com/blog/inside-googles-ved-parameter
 * Compile protocol buffer first: protoc --java_out=src/main/java src/main/resources/ved.proto
 */
class DecodeVed extends GenericUDF {

  object Decoder {

    val log = Logger(LoggerFactory.getLogger("VedDecoder"))

    val stringOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING)
    val intOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT)
    val boolOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN)

    val TYPES = Map[String, ObjectInspector]("link_index" → stringOI,
      "link_type" → stringOI, "sub_result_position" → intOI,
      "result_position" → intOI, "page" → intOI)

    implicit class Translator(n: Long) {

      def parseIndex: String = n match {
        case a if a > 85 ⇒ "right_hand_column_or_bottom"
        case a if a > 45 ⇒ "main_column"
        case _ ⇒ null
      }

      def parsePage: String = "page_" + n / 10 + 1

    }

    def decodePlainText(msg: String): Ved.VedMessage = {
      val builder = VedMessage.newBuilder()
      msg.split(",").map(_.split(":")).map { tok ⇒
        (tok(0), tok(1)) match {
          case ("i", v) ⇒ builder.setLinkIndex(v.toLong)
          case ("r", v) ⇒ builder.setResultPosition(v.toLong)
          case ("t", v) ⇒ builder.setLinkType(Ved.VedMessage.Type.valueOf(v))
          case ("s", v) ⇒ builder.setStartResultPosition(v.toLong)
        }
      }
      builder.build()
    }

    def decodeProtobuf(msg: String): Ved.VedMessage =
      Ved.VedMessage.parseFrom(
        new sun.misc.BASE64Decoder().decodeBuffer(msg.map {
          case '-' ⇒ '+'
          case '_' ⇒ '/'
          case c ⇒ c
        }))

    def toArray(v: VedMessage): Try[Array[Any]] = Try {
      val mirror = TYPES.keys.toVector
      log.info(s"Starting decoding of $v")
      Array(v.getLinkIndex.parseIndex,
        v.getLinkType.toString,
        v.getSubResultPosition,
        v.getResultPosition,
        v.getStartResultPosition.parsePage)
    }

    def apply(msg: String): Try[Array[Any]] = Try {
      msg.head match {
        case '1' /* decode plain text */ ⇒ decodePlainText(msg.tail)
        case '0' /* decode protobuf */ ⇒ decodeProtobuf(msg.tail)
      }
    }.flatMap(toArray)
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
    Decoder(input.asInstanceOf[String]).get
  }
}