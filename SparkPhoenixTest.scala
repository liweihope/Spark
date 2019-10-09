package com.jiuye

import org.apache.hadoop.conf.Configuration
import org.apache.phoenix.spark._
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
  * Created by Jepson on 2017/10/24.
  */
object SparkPhoenixTest {

  def main(args: Array[String]): Unit = {


    val zkUrl = "192.168.1.37,192.168.1.38,192.168.1.40,192.168.1.41,192.168.1.42:2181"
    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum",zkUrl)

    val spark = SparkSession
      .builder()
      .appName("SparkPhoenixTest3")
      .master("local[2]")
      .getOrCreate()

    //1.读取：
    //定义通过phoenix从hbase拿表数据，转成DF。注意指定字段(大写)和支持谓词下推
    val oms_orderinfoDF = spark.sqlContext.phoenixTableAsDataFrame(
      "JYDW.oms_orderinfo",
      Array("ORDERNO", "CUSTOMERID"),
      predicate = Some(
        """
          |CRETIME > CAST(TO_DATE('2018-02-10 00:00:01', 'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP)
        """.stripMargin),
      conf = configuration
    )

  

    oms_orderinfoDF.show()
    oms_orderinfoDF.createOrReplaceTempView("oms_orderinfo") //注册表 就可以写SQL




    //2.多张表读取 join 、group等 复杂计算


    //3.保存：
    oms_orderinfoDF.write
      .format("org.apache.phoenix.spark")
      .mode(SaveMode.Overwrite)
      .option("table", "JYDW.oms_orderinfo")
      .option("zkUrl",zkUrl)
      .save()

  }
}
