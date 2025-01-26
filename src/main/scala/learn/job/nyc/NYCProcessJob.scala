package learn.job.nyc
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.*
import org.joda.time.DateTime
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{ntile, col, when, log, round}

import java.sql.{Date, Timestamp}
import scala.util.Random



package object nycJob{
    private[nyc] def getSparkContext:SparkSession = SparkSession.builder.appName("nyc_j0b").getOrCreate()

    private val alphabets = "abcdefghijklmnopqrstuvwxyz".toList
    private val digits = "1234567890".toList

    private[this] def generateRandomString(size:Int)= Random.shuffle(alphabets).take(size).mkString
    private[this] def generateRandomInt(size:Int) = Random.shuffle(digits).take(size).mkString.toInt

    private[this] def generateRandomDate():DateTime={
        val currentDte = new DateTime()
        currentDte
    }

    private[this] def generateRandomDateTime():Timestamp={
        val currentDte = new DateTime()
        val ts = new Timestamp(currentDte.getMillis)
        ts
    }

    def generateExperience()={
        val experience = generateRandomInt(2) % 45 
        experience match{
            case 0 => 1
            case _ => experience
        }
    }

    def generateSalary():Double={
        generateRandomInt(5).toDouble
    }

    def generateName():String={
        s"${generateRandomString(7).capitalize} ${generateRandomString(6).capitalize}"
    }

    private[this] val birth_to_profession = List( 20, 22, 25, 19, 27)

    def generateDateOfBirth(experience:Int):Date={
        val yearsBeforeEmployment:Int = Random.shuffle(birth_to_profession).take(1).head
        val provisionalDate = generateRandomDate().minusYears(experience + yearsBeforeEmployment)
        new Date(provisionalDate.getMillis)
    }

    def createDataFrame(rowCount:Int) ={
        (0 to rowCount).foldLeft( List[Row]())(( acc, x)=>{

            val experience = nycJob.generateExperience()
            val dob = nycJob.generateDateOfBirth(experience)
            Row(nycJob.generateName(),
                nycJob.generateSalary(),
                experience, dob
            ) :: acc
        })

    }
    
}

object NYCProcessJob extends App{
    val spark = nycJob.getSparkContext
    import spark.implicits._


    val schema = StructType( Array( StructField("name", StringType), 
                                    StructField("salary", DoubleType), 
                                    StructField("experience", IntegerType),
                                    StructField("date_of_birth", DateType)),
    )

    val rdd = spark.sparkContext.parallelize(nycJob.createDataFrame(20))

    var df = spark.createDataFrame(rdd, schema)
    val windowSpec = Window.orderBy("experience")
    df = df.withColumn("experience_bucket", ntile(3).over(windowSpec))
        .withColumn( "experience_class", (when(col("experience_bucket") === 1, "Low")
                                        .when(col("experience_bucket") === 2, "Medium")
                                        .when(col("experience_bucket") === 3, "Senior")))
        .withColumn("salary_log", round(log(col("salary")),3))

    df.write.json("./output/salary")

    

}