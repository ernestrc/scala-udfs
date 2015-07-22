package com.opentok.udfs

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspector, ObjectInspectorFactory, PrimitiveObjectInspector, StructObjectInspector}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.util.Try

@Description(
  name = "deserialize_user",
  value = "_FUNC_(str2) deserializes paper trail string str2 into a user"
)
class DeserializeUser extends GenericUDF {

  object Deserializer {
    val log = Logger(LoggerFactory.getLogger("DeserializeUserUDF"))

    val stringOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING)
    val intOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.INT)
    val boolOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN)

    val USER = Map[String, ObjectInspector]("id" → intOI, "email" → stringOI, "encrypted_password" → stringOI, "reset_password_token" → stringOI,
      "reset_password_sent_at" → stringOI, "remember_created_at" → stringOI, "sign_in_count" → intOI, "current_sign_in_at" → stringOI,
      "last_sign_in_at" → stringOI, "current_sign_in_ip" → stringOI, "last_sign_in_ip" → stringOI, "confirmation_token" → stringOI,
      "confirmed_at" → stringOI, "confirmation_sent_at" → stringOI, "unconfirmed_email" → stringOI, "authentication_token" → stringOI,
      "created_at" → stringOI, "updated_at" → stringOI, "name" → stringOI, "sso_token" → stringOI, "username" → stringOI, "customer_id" → stringOI,
      "company_name" → stringOI, "company_url" → stringOI, "migrated" → boolOI, "tour_complete" → boolOI, "type" → stringOI, "v1_usage_plan_id" → intOI,
      "v1_support_plan_id" → intOI, "trial_ends_at" → stringOI, "converted_to_v2_at" → stringOI, "v2_usage_plan_id" → intOI,
      "v2_support_plan_id" → intOI, "state" → stringOI, "invoice_type" → stringOI, "developer" → boolOI, "company_industry" → stringOI,
      "address_zip" → stringOI, "company_type" → stringOI, "job_type" → stringOI, "qs_delivery" → stringOI, "role" → stringOI, "first_name" → stringOI, "last_name" → stringOI)

    val re = "([\\w]+):(.*)".r

    def clean(s: String): String = s.replace("⇒", "\n")

    def $deserialize(s: String): Try[Array[Any]] = Try {
      val mirror = USER.keys.toVector
      log.info(s"Starting deserialization of $s")
      re.findAllIn(s).matchData.foldLeft(new Array[Any](mirror.length)) {
        case (u, m) ⇒
          val k = m.group(1)
          val v = m.group(2)
          val idx = mirror.indexOf(k)
          log.debug(s"Key is $k, value is $v. Index in mirror is $idx")
          USER.get(k) match {
            case Some(`stringOI`) if v.isEmpty || v == "\n" || v == " " ⇒ u.updated(idx, null)
            case Some(`stringOI`) ⇒ u.updated(idx, v.trim())
            case Some(`intOI`) ⇒ u.updated(idx, Try(v.trim().toInt).getOrElse(null))
            case Some(`boolOI`) ⇒ u.updated(idx, Try(v.trim().toBoolean).getOrElse(null))
            case None ⇒ u //not a valid key
          }
      }
    }

    def deserialize(s: String) = $deserialize(clean(s))

  }

  private var inputInspector: PrimitiveObjectInspector = _

  def initialize(inputs: Array[ObjectInspector]): StructObjectInspector = {
    this.inputInspector = inputs(0).asInstanceOf[PrimitiveObjectInspector]
    val outputFieldNames = Deserializer.USER.keys.toList
    val outputInspectors = Deserializer.USER.values.toList
    ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors)
  }

  def getDisplayString(children: Array[String]): String = {
    "deserialize(" + children.mkString(",") + ")"
  }

  def evaluate(args: Array[DeferredObject]): Object = {
    val input = inputInspector.getPrimitiveJavaObject(args(0).get)
    Deserializer.deserialize(input.asInstanceOf[String]).get
  }
}
