package JSONParser

import net.liftweb.json._
import scala.io.Source

object GenericParser extends App {

  val schema = Source.fromURL(getClass.getResource("/CJPublisher.avsc")).mkString
  val json = parse(schema)
  val names = json \ "fields" \ "name"
  val namesList = names.values

  val newNames = namesList.asInstanceOf[List[(String, String)]]

  val onlyNames = newNames.map(value => value._2)

  val result = (for (name <- onlyNames) yield name).mkString("\",\"")


  val finalResult = "\"" + result + "\""
  print(finalResult)




}
