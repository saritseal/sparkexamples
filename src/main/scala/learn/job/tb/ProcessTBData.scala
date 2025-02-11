package learn.job.tb

import org.slf4j.LoggerFactory
import learn.job.commonfunctions

object ProcessTBData extends App{
    val logger = LoggerFactory.getLogger(getClass())
    val spark = commonfunctions.getSparkSession

    import spark.implicits._
    commonfunctions.getDatasetStatistics("testdata/TB_Burden_Country.csv")(spark)

    logger.info("from ProcessNYCData")
}