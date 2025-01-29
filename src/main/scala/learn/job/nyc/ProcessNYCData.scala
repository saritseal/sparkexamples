package learn.job.nyc
import org.slf4j.LoggerFactory
import learn.job.commonfunctions

object ProcessNYCData extends App{
    val logger = LoggerFactory.getLogger(getClass())
    val spark = commonfunctions.getSparkSession
    
    import spark.implicits._
    commonfunctions.getDatasetStatistics("testdata/fhvhv_tripdata_2024-01.parquet")(spark)
    
    logger.info("from ProcessNYCData")
}