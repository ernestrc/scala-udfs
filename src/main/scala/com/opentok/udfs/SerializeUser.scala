package com.opentok.udfs

import org.apache.hadoop.hive.ql.exec.Description
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory
import org.apache.hadoop.hive.serde2.objectinspector.{ObjectInspectorFactory, StructObjectInspector, ObjectInspector, PrimitiveObjectInspector}
import scala.collection.JavaConversions._

@Description(
  name = "serialize_user",
  value = "_FUNC_(str2) serializes paper trail string str2 into a user"
)
class SerializeUser extends GenericUDF {

  case class User(firstName: String, lastName: String)

  object User {
    def serialize(p: User): String = {
      p.firstName + "|" + p.lastName
    }

    def deserialize(s: String): User = {
      val parts = s.split("|")
      User(parts(0), parts(1))
    }
  }

  private var inputInspector: PrimitiveObjectInspector = _

  def initialize(inputs: Array[ObjectInspector]): StructObjectInspector = {
    this.inputInspector = inputs(0).asInstanceOf[PrimitiveObjectInspector]
    val stringOI = PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING)
    val outputFieldNames = "firstName" :: "lastName" :: Nil
    val outputInspectors = stringOI :: stringOI :: Nil
    ObjectInspectorFactory.getStandardStructObjectInspector(outputFieldNames, outputInspectors)
  }

  def getDisplayString(children: Array[String]): String = {
    "deserialize(" + children.mkString(",") + ")"
  }

  def evaluate(args: Array[DeferredObject]): Object = {
    val input = inputInspector.getPrimitiveJavaObject(args(0).get)
    val person = User.deserialize(input.asInstanceOf[String])
    Array(person.firstName, person.lastName)
  }

}
