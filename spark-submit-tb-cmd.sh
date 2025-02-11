/opt/spark/bin/spark-submit \
  --class learn.job.tb.ProcessTBData \
  --master local[2] \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///app/logs/featureengineering \
  target/scala-2.12/sbt-1.0/sparkexamplesproject-0.1.0.jar