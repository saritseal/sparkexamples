package learn.job.processblocks
import org.apache.spark.sql.functions.{col, when, count, lit, avg, min, max, stddev }
import learn.job.processblocks.*
import learn.job.processblocks
import org.slf4j.{LoggerFactory, Logger}
import scala.reflect.runtime.universe._


object ProcessBlocks extends App{
    val logger = LoggerFactory.getLogger(getClass())
    val spark = processblocks.getSparkSession
    import spark.implicits._

    
    val df = processblocks.readData(path="testdata/blocks")(spark).map(x => convertToBlock(x))
    val rows = df.count()
    logger.info(s"number of rows read ${rows}")

    val counts = df.select(when(col("isMatch").equalTo(true), "True").when(col("isMatch").equalTo(false), "False")
    .otherwise("Error").alias("matched")).groupBy("matched")agg({"matched"-> "count"})

    // distincts by ID
    val duplicates = df.groupBy(col("idFirst"), col("idSecond")).agg(count(lit("1")).alias("duplicate_counts")).filter(col("duplicate_counts") > 1)

    def getColumns(func:(org.apache.spark.sql.Column)=>org.apache.spark.sql.Column, funcName:String):Seq[org.apache.spark.sql.Column]={
        return Seq("idFirst", "idSecond", "cmpFnameC1", "cmpFnameC2", 
            "cmpLnameC1", "cmpLnameC2", "cmpSex", "cmpBd", 
            "cmpBm","cmpBy","cmpPlz").map(x => func(col(x)).alias(s"${funcName}_${x}"))
    }

    def constructStackExpr(statFunctions:Seq[String]):String={
        val columns =  Seq("idFirst", "idSecond", "cmpFnameC1", "cmpFnameC2", 
            "cmpLnameC1", "cmpLnameC2", "cmpSex", "cmpBd", 
            "cmpBm","cmpBy","cmpPlz")

        val stackExpr = columns.foldLeft(List[String]())((agg, x) => {
                agg ++ statFunctions.foldLeft(List[String]())( (a, y) =>  {
                    a :+ s"'${y}_${x}', ${y}_${x}"
                 }) 
            }).mkString(",")
        
        return f" stack(${columns.length * statFunctions.length}, ${stackExpr}) as (method, value) "
    }


    val columns = (getColumns(max, "max") ++ getColumns(avg,  "avg") ++ getColumns(min, "min") ++ getColumns(stddev, "stddev")) //
    // logger.info(columns)
    val statistics = df.select(columns : _*)
    statistics.show()
    statistics.printSchema()

    val stackExpr = constructStackExpr(Seq("max", "min", "avg", "stddev"))

    // Remember to cast the columns to a single type    
    val castedColumns = statistics.columns.foldLeft(List[org.apache.spark.sql.Column]())((agg, x)=>{
        col(x).cast("double").alias(s"""${x}"""):: agg
    })

    statistics.select(castedColumns: _*).selectExpr(stackExpr).show()

}