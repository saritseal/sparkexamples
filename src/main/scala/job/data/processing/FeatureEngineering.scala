package job.data.processing
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.util.Random
import job.data.metrics.JobStageTaskMetricsCollector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.commons.text.similarity.HammingDistance

package object featureengineeringfuncs{
    private [processing] val getSparkSession = SparkSession.builder()
                            .appName("FeatureEngineering")
                            .master("local[4]")
                            // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                            // .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                            .getOrCreate()
    
    private [this] def createRandomString(size:Int):String= Random.alphanumeric.toSeq.take(10).mkString
    
    private[processing] def createRandomStrings(rowCount:Int):Seq[String] = {
        (0 to rowCount).foldLeft(List[String]())((acc, i)=>{
            createRandomString(5) :: acc
        }).toSeq  
    }
}

package object stringdistances{
    private def prepareDataForHammingDistance(spark:SparkSession):(DataFrame, DataFrame)={

        import spark.implicits._
        val data = featureengineeringfuncs.createRandomStrings(10)

        val df1 = spark.sparkContext.parallelize(data).toDF("name")
        val df2 = df1.withColumn("name1", col("name")).drop("name")

        (df1, df2)

    }

    private def calculateHammingDistance(df1:DataFrame, df2:DataFrame):DataFrame={
        // The following mode would not work
        // val hamming_distance: (String, String) => Integer = new HammingDistance().apply

        val hamming_distance_udf = udf((s1:String, s2:String) => new HammingDistance().apply(s1, s2))
        // need to use a udf or spark.sql.functions in withColumn like lit, min etc
        df2.withColumn("hamming_distance", hamming_distance_udf(col("name"), col("name1"))).filter(col("hamming_distance") > 0 )
    }

    private val crossJoin = (df1:DataFrame, df2:DataFrame) => df1.crossJoin(df2).filter(col("name") =!= col("name1"))

    def hammingDistancePipeline(spark:SparkSession){
        val (df1, df2) = prepareDataForHammingDistance(spark)
        val df3 = crossJoin(df1, df2)
        val df4 = calculateHammingDistance(df1, df3)

        df4.coalesce(1).write.mode("overwrite").json("./output/crossjoin")
    }
}

object FeatureEngineering extends App{
    val spark = featureengineeringfuncs.getSparkSession
    import spark.implicits._
    spark.sparkContext.addSparkListener(new JobStageTaskMetricsCollector())

    stringdistances.hammingDistancePipeline(spark)
}
