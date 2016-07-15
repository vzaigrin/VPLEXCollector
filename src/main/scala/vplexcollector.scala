import sys.process._
import scala.xml._
import collection.mutable.ListBuffer
import java.net._
import java.io._
import scala.io._
import akka.actor.Actor
import akka.actor.ActorSystem
import akka.actor.Props
import fr.janalyse.ssh._


trait Logger {
  val loggerFile: String
  def log(msg: String)  {
    val out = new FileWriter(loggerFile, true)
    try {
      out.write(new java.util.Date + " " +  msg + "\n")
    }
    finally out.close()
  }
}


object vplexcollector extends App with Logger  {

  val loggerFile = "vplexcollector-error.log"
  
  if( (args.length != 0 && args.length != 2) || (args.length > 0 && args(0) != "-c") )  {
    println("Usage: vplexcollector -c config.file")
    log("Usage: vplexcollector -c config.file")
    sys.exit(-1)
  }

  val configFile = if(args.length == 2)  args(1)  else  "vplexcollector.xml"

  if( !(new java.io.File(configFile).exists) )  {
    println("Error: no such file " + configFile)
    log("Error: no such file " + configFile)
    sys.exit(-1)
  }

  val config: scala.xml.Elem = try {
    XML.loadFile(configFile)
  }  catch  {
    case _ : Throwable => println("Error in processing configuration file " + configFile)
                          log("Error in processing configuration file " + configFile)
                          sys.exit(-1)
  }
    
  if( (config \ "configuration").length != 1 )  {
    println("Error: no configuration in the file " + configFile)
    log("Error: no configuration in the file " + configFile)
    sys.exit(-1)
  }
    
  val configuration: Map[String,Any] = ((scala.xml.Utility.trim((config \ "configuration")(0)).child) map
    (c => c.label match {
      case "carbon" => "carbon" -> c.attributes.asAttrMap
      case "replacelist" => "replacelist" -> (c \ "replace" map
        (r => (r.attributes("what").toString, r.attributes("value").toString))).toList
      case _ => c.label -> c.text
    })
  ).toMap

  if( !configuration.contains("carbon") )  {
    println("Wrong carbon description in the configuration file " + configFile)
    log("Wrong carbon description in the configuration file " + configFile)
    sys.exit(-1)
  }

  val interval = if( configuration.contains("interval") )
                   configuration("interval").asInstanceOf[String].toInt
                 else
                   10

  val vplexList: List[Map[String,Any]] = (config \ "vplex" map (v =>
    (scala.xml.Utility.trim(v).child map (vc => vc.label match {
      case "monitors" => "monitors" -> (vc.child map (vc => vc.attributes.asAttrMap)).toList
      case _ => vc.label -> vc.text
    })).toMap
  )).toList

  if( vplexList.length == 0 )  {
    println("At least one VPLEX should be described in the configuration file " + configFile)
    log("At least one VPLEX should be described in the configuration file " + configFile)
    sys.exit(-1)
  }

  val system = ActorSystem("vplex")
  val vplexActList = (vplexList map (v => (v, system.actorOf( Props(new vplex(v, configuration)),
                                                              name = v("name").asInstanceOf[String])
                      ))).toList

  while(true) {
    for( v <- vplexActList )  { 
      v._2 ! "ask"
    }
    Thread.sleep(interval*60000)
  }

  system.shutdown
  sys.exit(0)
}


class vplex( vplex: Map[String,Any],
             configuration: Map[String,Any] ) extends Actor with Logger  {

  val format = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
  val loggerFile: String = configuration.getOrElse("errorlog","collector-error.log").asInstanceOf[String]
  val carbon = configuration("carbon").asInstanceOf[Map[String,String]]
  val repl = configuration.getOrElse("replacelist",List(("",""))).asInstanceOf[List[(String,String)]]
  val vname = vplex("name").asInstanceOf[String]
  var sock: java.net.Socket = null
  var out: java.io.OutputStream = null


  def receive = {
    case "ask" => ask
    case _ => log("Strange message")
  }
   

  def ask {
    
    sock = try { new Socket( carbon("address"), carbon("port").toInt ) }
           catch { case e: Exception => null }
    out = if( sock != null )  sock.getOutputStream()  else  null
    if( out == null )  {
      log(vname + " file: Can't connect to the carbon")
      return
    }

    val address = vplex("address").asInstanceOf[String]
    val username = vplex("username").asInstanceOf[String]
    val password = vplex("password").asInstanceOf[String]
    val monitors = vplex("monitors").asInstanceOf[List[Map[String,Any]]]

    for( monitor <- monitors )  {
      val file = monitor("file").asInstanceOf[String]
      val director = monitor("director").asInstanceOf[String]

      val headcmd = Array("head", "-1", file).mkString(" ")
      val headresult = SSH.once(address, username, password)(_.execute(headcmd)).split(",")

      val datacmd = Array("tail", "-1", file).mkString(" ")
      val dataresult = SSH.once(address, username, password)(_.execute(datacmd)).split(",")

      val timestamp: Long = format.parse(dataresult(0)).getTime() / 1000
      val result = (headresult.drop(1) zip dataresult.drop(1)).toMap

      for( (h,d) <- result )  {
        val msg = "vplex." + vname + "." + director + "." + replaceByList(h,repl).replaceFirst(" ",".") +
                  " " + d + " " + timestamp + "\r\n"
        try {
          out.write(msg.getBytes)
          out.flush
        }  catch  {
          case e: Exception => log(vname + ": Error in output to carbon")
        }
      }

    }
 
    if( sock != null )  sock.close
  }

  def replaceByList( text: String, repl: List[(String, String)] ): String = {
    if( !repl.isEmpty )
      return replaceByList( text.replace(repl(0)._1,repl(0)._2), repl.tail )
    else
      return text
  }

}
