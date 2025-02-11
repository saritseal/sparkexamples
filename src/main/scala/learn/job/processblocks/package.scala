package learn.job
import org.apache.spark.sql.{SparkSession, Dataset, Row}
import scala.collection.mutable.HashMap
import org.slf4j.{LoggerFactory, Logger}
import scala.util.{Try, Success, Failure}


package object processblocks {

    private val logger = LoggerFactory.getLogger(getClass())

    case class Block(   idFirst:Option[Int], idSecond:Option[Int], cmpFnameC1:Option[Float], cmpFnameC2:Option[Float], 
                        cmpLnameC1:Option[Float], cmpLnameC2:Option[Float], cmpSex:Option[Float], cmpBd:Option[Float], 
                        cmpBm:Option[Float],cmpBy:Option[Float],cmpPlz:Option[Float],isMatch:Option[Boolean])

    private[processblocks] lazy val getSparkSession = SparkSession.builder.getOrCreate()
    
    private[processblocks] def readData(path:String)(implicit spark:SparkSession): Dataset[Row] ={
        import spark.implicits._
        spark.read.option("header", "true").option("encoding", "UTF-8").option("nullValue", "?").csv(path)
    }

    private[processblocks] def convertToBlock(row:Row):Block={

        def getInt = (value:String) => value match {
            case "?" => None
            case x => Try(x.toInt) match {
                case Success(x) => Some(x)
                case Failure(e) => {
                    logger.error(s"failed to parse value ${e}")
                    None
                }
            }
        }

        def getFloat = (value:String) => value match {
            case "?" => None
            case x => Try(x.toFloat) match {
                case Success(x) => Some(x)
                case Failure(e) => {
                    None
                }
            }
        }   

        def getString = (value:String) => value match {
            case "?" => None
            case x => Try(String.valueOf(x)) match {
                case Success(x) => Some
                case Failure(e) => {
                    logger.error(s"failed to parse string value ${e}")
                    None
                }
            }
        }   
        def getBoolean = (value:String) => value match {
            case "?" => None
            case x => x match {
                case "TRUE" => Some(true)
                case "FALSE" => Some(false)
            }
        }   

        return Block(   idFirst = getInt(row.getAs[String]("id_1")), 
                        idSecond = getInt(row.getAs[String]("id_2")), 
                        cmpFnameC1 = getFloat(row.getAs[String]("cmp_fname_c1")), 
                        cmpFnameC2 = getFloat(row.getAs[String]("cmp_fname_c2")), 
                        cmpLnameC1 = getFloat(row.getAs[String]("cmp_lname_c1")), 
                        cmpLnameC2 = getFloat(row.getAs[String]("cmp_lname_c2")), 
                        cmpSex = getFloat(row.getAs[String]("cmp_sex")),
                        cmpBd = getFloat(row.getAs[String]("cmp_bd")),
                        cmpBy = getFloat(row.getAs[String]("cmp_by")),
                        cmpBm = getFloat(row.getAs[String]("cmp_bm")), 
                        cmpPlz = getFloat(row.getAs[String]("cmp_plz")),
                        isMatch = getBoolean(row.getAs[String]("is_match")))
    }

}
