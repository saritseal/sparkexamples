package learn.job.nyc
import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{avg, col, min, max, count, sum,round, expr, isnull, when, first} 


package object nycdatafunctions {
    private val logger = LoggerFactory.getLogger(getClass())
    private[nyc] val getSparkSession = SparkSession.builder.appName("ProcessNYCData").master("local[4]").config("spark.jars.ivy", "/path/to/ivy/cache").getOrCreate()

    private def generate_expr(columns:Seq[String], aggregateFunctions:Seq[String]):String={
        val expr = columns.flatMap(c => { aggregateFunctions.foldRight(List[String]())((a, agg)=>{
            val column = c
            s"'${column}_${a}', ${column}_${a}" :: agg
        })} )
   
        // logger.info(s"Experssion: ${expr.mkString("\n")}")
        return  "stack(" + expr.length + ", "+ expr.mkString(", ") + ") as (row_label, value)"
    }
    private[nyc] def getColumnStatistics(filePath:String)(implicit spark:SparkSession)={
        import spark.implicits._

        var df = spark.read.parquet(filePath)

        val types = df.schema.fields.filter(x => x.dataType.typeName == "double" || x.dataType.typeName == "long" ).flatMap( x => {

            Seq( 
                round(sum(x.name),2).cast("string").alias(s"${x.name}_sum"), 
                round(avg(x.name), 2).cast("string").alias(s"${x.name}_avg"),
                round(count(x.name), 2).cast("string").alias(s"${x.name}_count"),
                round(min(x.name), 2).cast("string").alias(s"${x.name}_min"),
                round(max(x.name), 2).cast("string").alias(s"${x.name}_max"),
                round(sum(when(isnull(col(x.name)), 1).otherwise(0)) , 2).cast("string").alias(s"${x.name}_nulls")
            )})
        
        // This expressions unpacks al the columns
        val stats = df.agg(types.head, types.tail: _*)

        stats.show()

        logger.info(s"number of columns = ${stats.columns.length}")
          
        val exprString = generate_expr(df.schema.fields.filter(x => x.dataType.typeName == "double" || x.dataType.typeName == "long" ).map(_.name), Seq("sum", "avg", "min", "max", "count"))

        // The stackedDF has all columns with datatype as string if needed in other type you have to convert the datatype
        val stackedDF = stats.selectExpr(exprString)

        var finalDF = stackedDF.withColumn(
          "value",
          when(col("row_label").endsWith("_sum"), col("value").cast("double"))
            .when(col("row_label").endsWith("_avg"), col("value").cast("double"))
            .when(col("row_label").endsWith("_min"), col("value").cast("double"))
            .when(col("row_label").endsWith("_max"), col("value").cast("double"))
            .when(col("row_label").endsWith("_count"), col("value").cast("integer"))
            .otherwise(col("value"))
        )

        finalDF = stackedDF.withColumn("datatype", col("row_label"))
        // stack(numberof columnns, 'column_name', column_name, ......) as (row_label, value)

        finalDF.show(stats.columns.length, false)
    }
}

object ProcessNYCData extends App{
    val logger = LoggerFactory.getLogger(getClass())
    val spark = nycdatafunctions.getSparkSession
    
    import spark.implicits._
    nycdatafunctions.getColumnStatistics("testdata/fhvhv_tripdata_2024-01.parquet")(spark)
    
    logger.info("from ProcessNYCData")
}