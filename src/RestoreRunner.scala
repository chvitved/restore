
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.regex.Pattern

import scala.Option.option2Iterable
import scala.collection.JavaConversions.asScalaBuffer

import au.com.bytecode.opencsv.CSVReader

object RestoreRunner extends App {
	val reader = new CSVReader(new InputStreamReader(new FileInputStream("/Users/chr/Downloads/a.csv"), "UTF-8"))
	var lines = reader.readAll()
	val headers = lines.head
	val data = lines tail
	
	val list = data.foldLeft(List[Map[String, String]]()) {(list, d) =>
	  val map = (0 until d.size).foldLeft(Map[String, String]()) {(map, index) =>
	    map + (headers(index) -> d(index))
	  }
	  map :: list
	}
	val auditlogs = list.flatMap(mapper(_))
	printToFile(auditlogs)
	println("entries: " + auditlogs.size)
	
	case class Auditlog(
	  autorisation: String,
	  doctorCpr: String,
	  username: String,
	  system: String,
	  method: String,
	  patientCpr: String,
	  role: String,
	  date: String,
	  onbehalfOf: String
	)
	
	
	
	def mapper(map: Map[String,String]): Option[Auditlog] = {
	  if (!map("source").endsWith("medicinkortet.log") ||
	      !map.contains("user") || map("user").trim.isEmpty) {
	    None
	  }else {
		  val usernamePattern = Pattern.compile(".*user=(.*?),.*")  
		  val m = usernamePattern.matcher(map("_raw"))
		  val username = if (m.find) m.group(1) else null
		  val time = map("_time")
		  Some(Auditlog(
			format(map("autorisation")),
			format(map("cpr")),
			format(username),
			format(map("system")),
			format(map("method")),
			format(map("person")),
			format(map("role").replace("'","")),
			format(time.substring(0,time.indexOf("."))),
			format(map("onbehalfof"))
		  ))
	  }
	  
	}
	
	def format(s: String): String = {
	  if (s == null || s == "null" || s.isEmpty) null
	  else s 
	}
	
	def toString(a: Auditlog): String = {
	  """%s, %s, %s, %s, %s, %s, %s, %s, %s""".format(a.autorisation, a.doctorCpr, a.username, a.system, a.method, a.patientCpr, a.role, a.date, a.onbehalfOf)
	}
	
	def printToFile(auditlogs: Seq[Auditlog]) {
		val p = new java.io.PrintWriter("out.auditlogs")
		try { 
		  for(a <- auditlogs) p.write(toString(a) + "\n")
		} finally {p.close()}
	}
	
	
	

}