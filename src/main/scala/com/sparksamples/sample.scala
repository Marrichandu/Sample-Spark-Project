package com.sparksamples


import scala.language.implicitConversions
import org.apache.spark.SparkConf
import org.apache.log4j.Logger
//import org.apache.spark.sql.catalyst.dsl.expressions.StringToAttributeConversionHelper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, sum}


object sample extends Serializable {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit={


    logger.info("starting code")
    val spark=SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()
    spark.conf.set("spark.sql.adaptive.enabled",false)
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled",false)
    //spark.conf.set("spark.sql.shuffle.partitions",3)
    val df=loadSurveyDf(spark,"sample.csv")
    val partitionedDF=df.repartition(4)

    //countDf.foreach(println)
   // logger.info(countDf.collect().mkString("->"))

   val Reason=commonReason(partitionedDF)
    val Route=RouteNumbers(partitionedDF)
  val accidentsIn=AccidentsInBus(partitionedDF)
  val acidentsNotIn=AccidentsNotInBus(partitionedDF)
   val lessAccidents=AccidentsMin(partitionedDF)


    Reason.show(false)
    //Route.show()
    //accidentsIn.show()
    //acidentsNotIn.show()
    //lessAccidents.show()
    //logger.info(tp.foreach(row=>print(row)))
    logger.info("finished")
    scala.io.StdIn.readLine()
    spark.stop()
  }
  def commonReason(df1:DataFrame):DataFrame= {

    df1.select("Reason").filter(df1("Reason")==="Mechanical Problem"|| df1("Reason")==="Flat Tire" ||
      df1("Reason")==="Won`t Start"||df1("Reason")==="Heavy Traffic" )
      .groupBy("Reason").count().withColumnRenamed("count(1)", "count")
      .orderBy(col("count").desc)

  }
  def RouteNumbers(df2:DataFrame):DataFrame={
    df2.select("Route_Number").groupBy("Route_Number").count().withColumnRenamed("count(1)", "count")
      .orderBy(col("count").desc).limit(5)
  }
def AccidentsInBus(df3:DataFrame):DataFrame={
  df3.where("Number_Of_Students_On_The_Bus>0")
    .groupBy("School_Year").count()

}

  def AccidentsNotInBus(df4:DataFrame):DataFrame={
    df4.where("Number_Of_Students_On_The_Bus==0")
      .groupBy("School_Year").count()
  }
def AccidentsMin(df5:DataFrame):DataFrame={
  df5.groupBy("School_Year").count().orderBy(col("count")).limit(1)
}


 def loadSurveyDf(spark:SparkSession, DataFile: String):DataFrame={
   spark.read
     .option("header","true")
     .option("InferSchema","true")
     .csv(DataFile)
 }

  def getSparkConf:SparkConf={
    val sparkAppConf=new SparkConf()
    //sparkAppConf.set("spark.app.name","sample").set("spark.master","local[3]")
    //sparkAppConf.set("spark.app.name","sample").set("spark.master","local[3]").set("spark.sql.shuffle.partitions","2")
    sparkAppConf.setAppName("sample")
      .setMaster("local[3]")

    sparkAppConf
  }
}
