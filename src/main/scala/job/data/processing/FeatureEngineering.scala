package job.data.processing
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.util.Random
import job.data.metrics.JobStageTaskMetricsCollector
import org.apache.spark.sql.functions.{col, udf}
import org.apache.commons.text.similarity.HammingDistance
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest
import java.util.Arrays


// vscode thinks these are incorrect
import breeze.stats.distributions._
import breeze.stats.distributions.RandBasis
import breeze.stats.distributions.ThreadLocalRandomGenerator

// The following is a psedurandom generatoe
import org.apache.commons.math3.random.MersenneTwister
import org.slf4j.LoggerFactory

package object featureengineeringfuncs{
    private val logger = LoggerFactory.getLogger(getClass)
    // import breeze.stats.distributions.RandBasis.implicits._
    private [processing] val getSparkSession = SparkSession.builder()
                                                            .appName("FeatureEngineering")
                                                            .master("local[4]")
                                                            // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                                                            // .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                                                            .getOrCreate()
    
    private [this] def createRandomString(size:Int):String= Random.alphanumeric.toSeq.take(10).mkString
    
    private [this] def getGaussian(mean:Double, stdDev:Double):Double={
        implicit val randBasis: RandBasis = RandBasis.withSeed(Random.nextInt())
        val normalDist = randBasis.gaussian(mean, stdDev).draw()
        normalDist
    }

    private [this] def getUniform(min:Double, max:Double):Double={
        val randBasis: RandBasis = RandBasis.withSeed(Random.nextInt())
        val uniformDist = new Uniform(min, max)(randBasis)
        uniformDist.draw()
    } 
    
    private[processing] def createRandomStrings(rowCount:Int):Seq[String] = {
        (0 to rowCount).foldLeft(List[String]())((acc, i)=>{
            createRandomString(5) :: acc
        }).toSeq  
    }

    private[processing] def createGaussianDoubles(rowCount:Int, mean:Double, stdDev:Double):Seq[Double] = {
        (0 to rowCount).foldLeft(List[Double]())((acc, i)=>{
            getGaussian(mean, stdDev) :: acc
        }).toSeq  
    }

    private[processing] def createUnifoemDoubles(rowCount:Int, min:Double, max:Double):Seq[Double] = {
        (0 to rowCount).foldLeft(List[Double]())((acc, i)=>{
            getUniform(min, max) :: acc
        }).toSeq  
    }

}

package object datadrift{
    val logger = LoggerFactory.getLogger(getClass)
    private def generateDataForDataDrift(spark:SparkSession):(DataFrame, DataFrame)={
        import spark.implicits._
        val gaussianData = featureengineeringfuncs.createGaussianDoubles(10, 0, 1)
        val gaussianDistrib = spark.sparkContext.parallelize(gaussianData).toDF("values")

        val uniformData = featureengineeringfuncs.createUnifoemDoubles(10, 0, 1)

        val uniformDistrib = spark.sparkContext.parallelize(uniformData).toDF("values")
        (gaussianDistrib, uniformDistrib)
    }

    def calculateKS(spark:SparkSession):Double={

        val (df1, df2) = generateDataForDataDrift(spark)
        import spark.implicits._

        val array1:Array[Double] = df1.map(x => x.getAs[Double]("values")).collect()
        val array2:Array[Double] = df2.map(x => x.getAs[Double]("values")).collect()

        logger.info(s"array1: ${Arrays.toString(array1)}")
        logger.info(s"array2: ${Arrays.toString(array2)}")

        val kstest = new KolmogorovSmirnovTest()
        val pvalue = kstest.kolmogorovSmirnovTest(array1, array2)        
        pvalue
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
    val logger = LoggerFactory.getLogger(getClass)

    import spark.implicits._
    spark.sparkContext.addSparkListener(new JobStageTaskMetricsCollector())

    stringdistances.hammingDistancePipeline(spark)

    val pValue = datadrift.calculateKS(spark)
    logger.info(s"pvalue is ${pValue}, distributions are different if ${pValue < 0.05}")
}
