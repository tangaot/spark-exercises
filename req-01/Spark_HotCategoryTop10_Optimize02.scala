package ca.tangaot.bigdata.spark.req

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object Spark_HotCategoryTop10_Optimize02 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")

    val sc = new SparkContext(sparkConf)

    // TODO Read data from file
    val fileData = sc.textFile("input/user_visit_action.txt")

    val flatData: RDD[(String, (Int, Int, Int))] = fileData.flatMap (
      data => {
        val splitData = data.split("_")
        if (splitData(6) != "-1") {
          List((splitData(6), (1, 0, 0)))
        } else if (splitData(8) != "null") {
          val cid = splitData(8).split(",")
          cid.map(
            id =>{
              (id, (0, 1, 0))
            }
          )
        } else if (splitData(10) != "null") {
          val cid = splitData(10).split(",")
          cid.map(
            id =>{
              (id, (0, 0, 1))
            }
          )
        } else {
          Nil
        }
      }
    )

    //TODO Sorting statistical results
    // Combine three behavior together
    // (Category ID, (1, 0 ,0))
    // (Category ID, (0, 1, 0))
    // (Category ID, (0, 0, 1))
    // Combine to => (Category ID, ( Click，Order，Payment ))

    val reducedData = flatData.reduceByKey(
      (t1, t2) => {
        (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
      }
    )
    val hotTop10 = reducedData.sortBy(_._2,false).take(10)
    hotTop10.foreach(println)
    //Close Spark Connection
    sc.stop()
  }
}
