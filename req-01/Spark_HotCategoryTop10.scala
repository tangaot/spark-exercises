package ca.tangaot.bigdata.spark.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_HotCategoryTop10 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(sparkConf)

    // TODO Read data from file
    val fileData = sc.textFile("input/user_visit_action.txt")

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
    val clickCntData = clickData.map(
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
    // (Category ID, Click)
    // (Category ID, Order)
    // (Category ID, Payment)
    // => (Category ID, ( Click，Order，Payment ))

    val combineCntData: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))]
    = clickCntData.cogroup(orderCntData,paymentCntData)

    val combineMapData: RDD[(String, (Int, Int, Int))] = combineCntData.map{
      case (cid,(clickItr, orderItr, paymentItr)) => {
          var clickCnt = 0
          var orderCnt = 0
          var paymentCnt = 0

          val cIterator = clickItr.iterator
          if (cIterator.hasNext) {
            clickCnt = cIterator.next()
          }

          val oIterator = orderItr.iterator
          if (oIterator.hasNext) {
            orderCnt = oIterator.next()
          }

          val pIterator = paymentItr.iterator
          if (pIterator.hasNext) {
            paymentCnt = pIterator.next()
          }

          (cid, (clickCnt, orderCnt, paymentCnt))
        }
    }

    val hotTop10 = combineMapData.sortBy(_._2, false).take(10)
    hotTop10.foreach(println)

    //关闭Spark连接
    sc.stop()
  }
}
