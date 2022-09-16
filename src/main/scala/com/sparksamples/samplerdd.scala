package com.sparksamples

import scala.language.implicitConversions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger


object samplerdd extends Serializable {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit={
    //Creating RDD
    val sparkAppConf=new SparkConf().setAppName("sampleBusData")
      .setMaster("local[3]")
    val sc=new SparkContext(sparkAppConf)

    val BusData = sc.textFile("sample.csv")
    val header = BusData.first()
    val filteredData=BusData.filter(row=> row!=header)
    val FilteredRows=filteredData.filter(row=>(row.split(",")(4)=="Mechanical Problem") ||
      (row.split(",")(4)=="Heavy Traffic")||
      (row.split(",")(4)=="Flat Tire")||
      (row.split(",")(4)=="Won`t Start"))
    val commonReason=FilteredRows.map{record => (record.split(",")(4),1.toInt)}.reduceByKey(_+_).sortBy(_._2,false).collect()
    //commonReason.foreach(x=>println(x._1))

  val topRoutes=FilteredRows.map{record => (record.split(",")(3),1.toInt)}.reduceByKey(_+_).sortBy(_._2,false).collect.take(5)
    //topRoutes.foreach(x=>println(x._1))

      val TotalIncidentsIn=filteredData.filter(row=>(row.split(",")(6)=="0"))
        .map{record => (record.split(",")(0),1.toInt)}.reduceByKey(_+_).collect()
    //logger.info(TotalIncidentsIn.mkString("->"))

    val TotalIncidentsOut=filteredData.filter(row=>(row.split(",")(6)!="0"))
      .map{record => (record.split(",")(0),1.toInt)}.reduceByKey(_+_).collect()
    //logger.info(TotalIncidentsOut.mkString("->"))

      val lessIncidents=filteredData.map{record => (record.split(",")(0),1.toInt)}.reduceByKey(_+_).sortBy(_._2).collect.take(1)
    //logger.info(lessIncidents.mkString("->"))




    println("Most Common Reasons are:")
    commonReason.foreach(x=>println(x._1))
    println("Top 5 Route Numbers are:")
    topRoutes.foreach(x=>println(x._1))
    println("Total Number Of accidents when Students in bus are:")
    TotalIncidentsIn.foreach(println)
    println("Total Number Of accidents when Students not in bus are:")
    TotalIncidentsOut.foreach(println)
    println("Year Where accidents are less are:")
    lessIncidents.foreach(x=>print(x._1))
    // val FinalRDD=PairedData.reduceByKey(_+_)
    // val FinalRDD=PairedData.countByKey()
    //val vk = FinalRDD.map(_.swap)
    //val vkSorted = vk.sortByKey(false)
    //val sortedRDD=FinalRDD.sortBy(_._2)


    //FinalRDD.collect()
    scala.io.StdIn.readLine()




  }
}
