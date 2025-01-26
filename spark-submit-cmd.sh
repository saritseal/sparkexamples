/opt/spark/bin/spark-submit \
  --class learn.job.nyc.NYCProcessJob \
  --master local[2] \
  target/scala-2.12/sbt-1.0/sparkexamplesproject-0.1.0.jar