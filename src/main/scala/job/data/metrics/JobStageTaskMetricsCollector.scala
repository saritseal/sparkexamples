package job.data.metrics
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.scheduler.{SparkListenerTaskStart, SparkListenerTaskEnd, SparkListenerJobStart, SparkListenerJobEnd}
import org.apache.spark.scheduler.{SparkListenerStageSubmitted, SparkListenerStageCompleted}
import org.apache.log4j.{Level, Logger}
import org.joda.time.DateTime
import org.joda.time.LocalDateTime
import org.joda.time.format.DateTimeFormat


class JobStageTaskMetricsCollector extends SparkListener {
    val logger = Logger.getLogger(this.getClass)
    
    override def onJobStart(jobStart:SparkListenerJobStart):Unit={
        val jobId = jobStart.jobId
        val startTime = jobStart.time

        logger.info(s"Job Start: ${jobId} started on ${startTime}")
    } 

    override def onJobEnd(jobEnd:SparkListenerJobEnd):Unit={
        val jobId = jobEnd.jobId
        val endTime = jobEnd.time
        val result = jobEnd.jobResult.toString()

        logger.info(s"Job End: ${jobId} started on ${endTime} with result ${result}")
    } 

    override def onStageSubmitted(stageStart:SparkListenerStageSubmitted):Unit={
        val stageId = stageStart.stageInfo.stageId
        val startTime = stageStart.stageInfo.submissionTime
        val completionTime = stageStart.stageInfo.completionTime

        logger.info(s"Stage Submitted: ${stageId} started on ${startTime}/${completionTime}")
    } 

    override def onStageCompleted(stageEnd:SparkListenerStageCompleted):Unit={
        val stageId = stageEnd.stageInfo.stageId
        val startTime = stageEnd.stageInfo.submissionTime
        val completionTime = stageEnd.stageInfo.completionTime

        logger.info(s"Stage Completed: ${stageId} started on ${startTime}/${completionTime}")
    } 
    
    override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
        val taskId = taskStart.taskInfo.id
        val taskStatus = taskStart.taskInfo.status
        val executorId = taskStart.taskInfo.executorId
        val startTime = LocalDateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:ms"))
        
        logger.info(s"Task Start: ${taskId} started by ${executorId} at ${startTime}")
    }

    override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
        val taskId = taskEnd.taskInfo.id
        val taskStatus = taskEnd.taskInfo.status
        val executorId = taskEnd.taskInfo.executorId
        val finishTime = LocalDateTime.now().toString(DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:ms"))

        logger.info(s"Task End: ${taskId} started by ${executorId} at ${finishTime}")
    }


}
