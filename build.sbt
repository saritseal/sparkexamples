// Set the Scala version
ThisBuild / scalaVersion := "2.12.18"

// Project definition
lazy val root = (project in file("."))
  .settings(
    name := "SparkExamplesProject",
    version := "0.1.0",
    //    organization := "com.example",
    scalaVersion := "2.12.18",

    // Dependencies
    libraryDependencies ++= Seq(
      // Apache Spark Core and SQL
      "org.apache.spark" %% "spark-core" % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-sql"  % "3.4.1" % "provided",
      "org.apache.spark" %% "spark-mllib" % "3.5.0",

      // Joda-Time for date/time handling
      "joda-time" % "joda-time" % "2.10.14",

      // Spark Logging (Optional)
      "org.slf4j" % "slf4j-api" % "1.7.36",
      "org.slf4j" % "slf4j-log4j12" % "1.7.36",

      // Apache commons Math 
      "org.apache.commons" % "commons-math3" % "3.6.1",

      // for numerical processing 
      // "org.scalanlp" %% "breeze" % "1.3"
      "org.scalanlp" %% "breeze" % "2.1.0",
      "org.scalanlp" %% "breeze-natives" % "2.1.0",
    ),

    // Enable better logging
    Test / fork := true,
    ThisBuild / assembly / assemblyJarName := "nycjob.jar"
  )

// Enable better compilation
enablePlugins(SbtPlugin)

// Resolver for Spark Packages (Optional)
resolvers += "Apache Spark Repository" at "https://repos.spark-packages.org/"