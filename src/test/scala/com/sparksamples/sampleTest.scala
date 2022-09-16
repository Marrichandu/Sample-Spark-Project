package com.sparksamples

import org.scalatest.BeforeAndAfterAll
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.sparksamples.sample.{AccidentsInBus, AccidentsMin, AccidentsNotInBus, RouteNumbers, commonReason, loadSurveyDf}
import org.apache.parquet.format.IntType
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite

import scala.collection.mutable


class sampleTest extends FunSuite with BeforeAndAfterAll {
  @transient var spark:SparkSession=_
@transient var myDF:DataFrame=_

  override def beforeAll(): Unit = {
    spark=SparkSession.builder()
      .appName("sampleTest")
      .master("local[3]")
  .getOrCreate()

    val mySchema=StructType(List(
      StructField("School_Year",StringType),
      StructField("Run_Type",StringType),
      StructField("Bus_No",StringType),
      StructField("Route_Number",StringType),
      StructField("Reason",StringType),
      StructField("Occurred_On",StringType),
      StructField("Number_Of_Students_On_The_Bus",IntegerType)
    ))

    val rows=List(Row("2015-2016","Special Ed AM Run","48186","5","Mechanical Problem","2015-09-02T06:24:00",6),
      Row("2015-2016","Special Ed AM Run","12345","1","Flat Tire","2015-09-02T06:24:00",0),
      Row("2016-2017","Special Ed PM Run","45623","2","Won`t Start","2015-09-02T06:24:00",2),
      Row("2017-2018","Special Ed AM Run","99874","1","Mechanical Problem","2015-09-02T06:24:00",3),
      Row("2018-2019","Special Ed PM Run","12390","3","Heavy Traffic","2015-09-02T06:24:00",16),
      Row("2019-2020","Special Ed AM Run","34221","4","Mechanical Problem","2015-09-02T06:24:00",22)
    )

    val myRDD=spark.sparkContext.parallelize(rows,2)
    myDF=spark.createDataFrame(myRDD,mySchema)

  }


  override def afterAll():Unit={
    spark.close()
  }

  test("Data file loading"){
    val sampleDf=loadSurveyDf(spark,"sample.csv")
    val rcount=sampleDf.count()
    assert(rcount==277113,"Record count should be 277113")
  }

test("Min Accidents"){
  //val sampleDf=loadSurveyDf(spark,"sample.csv")
  val MinDF=AccidentsMin(myDF)
  val Result=MinDF.collect()
  assert(Result(0)(0)== "2016-2017","should be 2019-2020")
}

  test("AccidentsNotInBus"){
    //val sampleDf=loadSurveyDf(spark,"sample.csv")
    val MinDF=AccidentsNotInBus(myDF)
    val notInMap=new mutable.HashMap[String,Long]()
    MinDF.collect().foreach(r=> notInMap.put(r.getString(0),r.getLong(1)))
    assert(notInMap("2015-2016")==1,"should be 2016-2017")


  }

  test("AccidentsInBus"){
    //val sampleDf=loadSurveyDf(spark,"sample.csv")
    val MinDF=AccidentsInBus(myDF)
    val InMap=new mutable.HashMap[String,Long]()
    MinDF.collect().foreach(r=> InMap.put(r.getString(0),r.getLong(1)))
    assert(InMap("2015-2016")==1,"should be 2016-2017")
    assert(InMap("2016-2017")==1,"should be 2016-2017")
    assert(InMap("2017-2018")==1,"should be 2016-2017")
    assert(InMap("2018-2019")==1,"should be 2016-2017")
    assert(InMap("2019-2020")==1,"should be 2016-2017")
  }



  test("RouteNumbers"){
    //val sampleDf=loadSurveyDf(spark,"sample.csv")
    val MinDF=RouteNumbers(myDF)
    val Route=new mutable.HashMap[String,Long]()
    MinDF.collect().foreach(r=> Route.put(r.getString(0),r.getLong(1)))
    assert(Route("1")==2,"should be 2")
    assert(Route("2")==1,"should be 1")
    assert(Route("3")==1,"should be 1")
    assert(Route("4")==1,"should be 1")
    assert(Route("5")==1,"should be 1")

  }




  test("commonReason"){
    //val sampleDf=loadSurveyDf(spark,"sample.csv")
    val MinDF=commonReason(myDF)
    val Reason=new mutable.HashMap[String,Long]()
    MinDF.collect().foreach(r=> Reason.put(r.getString(0),r.getLong(1)))
    assert(Reason("Heavy Traffic")==1,"should be Heavy Traffic")
    assert(Reason("Mechanical Problem")==3,"should be Mechanical Problem")
    assert(Reason("Won`t Start")==1,"should be Won`t Start")
    assert(Reason("Flat Tire")==1,"should be Flat Tire")

  }
}
