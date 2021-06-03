package ca.tangaot.bigdata.spark.req

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

object Spark_HotCategoryTop10_Optimize01 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // TODO Read data from file
    val fileData = sc.textFile("input/user_visit_action.txt")
    //
    fileData.persist(StorageLevel.MEMORY_AND_DISK)

    //TODO Total Click Times for Each Category
    // Filter out non click data
    val clickData= fileData.filter(
      data => {
        val splitData = data.split("_")
        val cid = splitData(6)
        cid != "-1"
      }
    )
    // Count click times for each category
    val clickCntData: RDD[(String, Int)] = clickData.map(
      data => {
        val splitData = data.split("_")
        val cid = splitData(6)
        (cid, 1)
      }
    ).reduceByKey(_ + _)

    //TODO Total Number of Order for Each Category
    // Filter out non order data
    val orderData = fileData.filter(
      data => {
        val splitData = data.split("_")
        val cid = splitData(8)
        cid != "null"
      }
    )

    val orderCntData = orderData.flatMap(
      data => {
        val splitData = data.split("_")
        val cidList = splitData(8)
        val cid = cidList.split(",")
        cid.map((_, 1))
      }
    ).reduceByKey(_+_)


    //TODO Total Number of Payment

    val paymentData = fileData.filter(
      data => {
        val splitData = data.split("_")
        val cidList = splitData(10)
        cidList != "null"
      }
    )

    val paymentCntData = paymentData.flatMap(
      data => {
        val splitData = data.split("_")
        val cidList = splitData(10)
        val cid = cidList.split(",")
        cid.map((_, 1))
      }
    ).reduceByKey(_+_)

    //TODO Sorting statistical results
    // Combine three behavior together
    // (Category ID, (click, 0 ,0))
    // (Category ID, (0, Order, 0))
    // (Category ID, (0, 0, Payment))
    // => (Category ID, ( Click，Order，Payment ))


    val clickCntMap = clickCntData.map {
      case (cid, num) => {
        (cid, (num, 0, 0))
      }
    }
    val orderCntMap = orderCntData.map {
      case (cid, num) => {
        (cid, (0, num, 0))
      }
    }
    val paymentCntMap = paymentCntData.map {
      case (cid, num) => {
        (cid, (0, 0, num))
      }
    }
    val unionData = clickCntMap.union(orderCntMap).union(paymentCntMap)
    val reducedData = unionData.reduceByKey {
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    }
    val hotTop10 = reducedData.sortBy(_._2,false).take(10)
    hotTop10.foreach(println)



    //关闭Spark连接
    sc.stop()
  }
}
