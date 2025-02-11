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


    val columns = (getColumns(max, "max") ++ getColumns(avg,  "avg") ++ getColumns(min, "min") ++ getColumns(stddev, "stddev")) //
    // logger.info(columns)
    val statistics = df.select(columns : _*)
    statistics.show()

    // val statistics = df.select( max(col("idFirst")).alias("max_idFirst"), 
    //                             max(col("idSecond")).alias("max_idSecond"), 
    //                             max(col("cmpFnameC1")).alias("max_cmpFnameC1"), 
    //                             max(col("cmpFnameC2")).alias("max_cmpFnameC2"), 
    //                             max(col("cmpLnameC1")).alias("max_cmpLnameC1"), 
    //                             max(col("cmpLnameC2")).alias("max_cmpLnameC2"), 
    //                             max(col("cmpSex")).alias("max_cmpSex"), 
    //                             max(col("cmpBd")).alias("max_cmpBd"), 
    //                             max(col("cmpBm")).alias("max_cmpBm"), 
    //                             max(col("cmpBy")).alias("max_cmpBy"), 
    //                             max(col("cmpPlz")).alias("max_cmpPlz"), 
    //                             avg(col("idFirst")).alias("avg_idFirst"), 
    //                             avg(col("idSecond")).alias("avg_idSecond"), 
    //                             avg(col("cmpFnameC1")).alias("avg_cmpFnameC1"), 
    //                             avg(col("cmpFnameC2")).alias("avg_cmpFnameC2"), 
    //                             avg(col("cmpLnameC1")).alias("avg_cmpLnameC1"), 
    //                             avg(col("cmpLnameC2")).alias("avg_cmpLnameC2"), 
    //                             avg(col("cmpSex")).alias("avg_cmpSex"), 
    //                             avg(col("cmpBd")).alias("avg_cmpBd"), 
    //                             avg(col("cmpBm")).alias("avg_cmpBm"), 
    //                             avg(col("cmpBy")).alias("avg_cmpBy"), 
    //                             avg(col("cmpPlz")).alias("avg_cmpPlz"), 
    //                             min(col("idFirst")).alias("min_idFirst"), 
    //                             min(col("idSecond")).alias("min_idSecond"), 
    //                             min(col("cmpFnameC1")).alias("min_cmpFnameC1"), 
    //                             min(col("cmpFnameC2")).alias("min_cmpFnameC2"), 
    //                             min(col("cmpLnameC1")).alias("min_cmpLnameC1"), 
    //                             min(col("cmpLnameC2")).alias("min_cmpLnameC2"), 
    //                             min(col("cmpSex")).alias("min_cmpSex"), 
    //                             min(col("cmpBd")).alias("min_cmpBd"), 
    //                             min(col("cmpBm")).alias("min_cmpBm"), 
    //                             min(col("cmpBy")).alias("min_cmpBy"), 
    //                             min(col("cmpPlz")).alias("min_cmpPlz"), 
    //                             stddev(col("idFirst")).alias("stddev_idFirst"), 
    //                             stddev(col("idSecond")).alias("stddev_idSecond"), 
    //                             stddev(col("cmpFnameC1")).alias("stddev_cmpFnameC1"), 
    //                             stddev(col("cmpFnameC2")).alias("stddev_cmpFnameC2"), 
    //                             stddev(col("cmpLnameC1")).alias("stddev_cmpLnameC1"), 
    //                             stddev(col("cmpLnameC2")).alias("stddev_cmpLnameC2"), 
    //                             stddev(col("cmpSex")).alias("stddev_cmpSex"), 
    //                             stddev(col("cmpBd")).alias("stddev_cmpBd"), 
    //                             stddev(col("cmpBm")).alias("stddev_cmpBm"), 
    //                             stddev(col("cmpBy")).alias("stddev_cmpBy"), 
    //                             stddev(col("cmpPlz")).alias("stddev_cmpPlz")).show()

    // counts.join(duplicates, joinExprs=lit(true), joinType="full_outer" ).na.fill(Map("idFirst" -> "None", "idSecond" -> "None", "duplicate_counts" -> 0 )).show()


    // idFirst, idSecond, cmpFnameC1, cmpFnameC2, 
    // cmpLnameC1, cmpLnameC2, cmpSex, cmpBd, 
    // cmpBm,cmpBy,cmpPlz

}