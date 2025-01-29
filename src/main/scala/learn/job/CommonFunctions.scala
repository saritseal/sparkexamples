package learn.job

import org.slf4j.LoggerFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, Column}
import org.apache.spark.sql.functions.{avg, col, min, max, count, sum, round, expr, isnull, when, first, stddev, lit, percentile_approx} 


package object commonfunctions {
    private val logger = LoggerFactory.getLogger(getClass())
    private[job] val getSparkSession = SparkSession.builder.appName("ProcessNYCData").master("local[4]").config("spark.jars.ivy", "/path/to/ivy/cache").getOrCreate()

    // This function unpivots the data from a single row to multiple rows each matching a column
    // stack is very performance intensive
    private def generateExpr(columns:Seq[String], aggregateFunctions:Seq[String]):String={
        val expr = columns.flatMap(c => { aggregateFunctions.foldRight(List[String]())((a, agg)=>{
            val column = c
            s"'${column}_${a}', ${column}_${a}" :: agg
        })} )

        // logger.info(s"Experssion: ${expr.mkString("\n")}")
        return  "stack(" + expr.length + ", "+ expr.mkString(", ") + ") as (row_label, value)"
    }

    private def findNumercColumns(df:DataFrame):Seq[Column]={
        val types = df.schema.fields.filter(x => x.dataType.typeName == "double" || x.dataType.typeName == "long" ).flatMap( x => {
            Seq( 
                round(sum(x.name),2).cast("string").alias(s"${x.name}_sum"), 
                round(avg(x.name), 2).cast("string").alias(s"${x.name}_avg"),
                round(count(x.name), 2).cast("string").alias(s"${x.name}_count"),
                round(min(x.name), 2).cast("string").alias(s"${x.name}_min"),
                round(max(x.name), 2).cast("string").alias(s"${x.name}_max"),
                round(stddev(x.name), 2).cast("string").alias(s"${x.name}_stddev"),
                round(percentile_approx(col(x.name), lit(0.25), lit(1000)), 2).cast("string").alias(s"${x.name}_q1"),
                round(percentile_approx(col(x.name), lit(0.75), lit(1000)), 2).cast("string").alias(s"${x.name}_q3"),
                round(sum(when(isnull(col(x.name)), 1).otherwise(0)) , 2).cast("string").alias(s"${x.name}_nulls")
            )})
        return types
    }

    private def readFile(filePath:String)(spark:SparkSession):Either[Exception, DataFrame]={
        val df = filePath match{
            case x if x.endsWith("parquet") => Right(spark.read.parquet(filePath))
            case x if x.endsWith("csv") => Right(spark.read.csv(filePath))
            case x => Left(new Exception("the file is not of known file types"))
        }
        return df

    }

    private[job] def getDatasetStatistics(filePath:String)(implicit spark:SparkSession)={
        import spark.implicits._

        var df = readFile(filePath)(spark) match {
            case Left(x) =>  {
                throw x
            } 
            case Right(x) => x
        }

        val types = findNumercColumns(df)
        
        // This expressions unpacks al the columns
        val stats = df.agg(types.head, types.tail: _*)
        //stats.show()

        logger.info(s"number of columns = ${stats.columns.length}")

        val exprString = generateExpr(df.schema.fields.filter(x => x.dataType.typeName == "double" || x.dataType.typeName == "long" ).map(_.name), Seq("sum", "avg", "min", "max", "count", "stddev", "q1", "q3"))

        // The stackedDF has all columns with datatype as string if needed in other type you have to convert the datatype
        // stack(numberof columnns, 'column_name', column_name, ......) as (row_label, value)
        val stackedDF = stats.selectExpr(exprString)

        var finalDF = stackedDF.withColumn(
          "value",
          when(col("row_label").endsWith("_sum"), col("value").cast("double"))
            .when(col("row_label").endsWith("_avg"), col("value").cast("double"))
            .when(col("row_label").endsWith("_min"), col("value").cast("double"))
            .when(col("row_label").endsWith("_max"), col("value").cast("double"))
            .when(col("row_label").endsWith("_count"), col("value").cast("integer"))
            .when(col("row_label").endsWith("_stddev"), col("value").cast("double"))
            .when(col("row_label").endsWith("_q1"), col("value").cast("double"))
            .when(col("row_label").endsWith("_q3"), col("value").cast("double"))
            .otherwise(col("value"))
        )

        finalDF = stackedDF.withColumn("datatype", 
                    when(col("value").rlike("^[0-9]+$"), lit("Integer"))
                    .when(col("value").rlike("^[0-9]+\\.[0-9]+$"), lit("Double"))
                    .when(col("value").isin("true", "false"), lit("Boolean"))
                    .otherwise(lit("String")))

        
        finalDF.show(stats.columns.length, false)
    }
}