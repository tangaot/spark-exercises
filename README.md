# Spark Exercises

In this exercise you will learn how to implement specific demands using Spark API.  These Requirements are real demands for an e-commerce website, so we have to get the data ready before we can implement the functionality.

## Explore Data

![image](https://github.com/tangaot/spark-exercises/blob/main/images/Snipaste_2021-06-03_16-03-32.png)

The above graph is a part of the data file, which represents the user behavior data of the e-commerce website, mainly containing four kinds of user behavior: search, click, order, and payment. The data rules are as follows.

* Each row of data uses underscores to separate the data.
* Each line of data represents a user's behavior, and this behavior can only be one of the four kinds of behavior.
* If the search keyword is null, that the data is not search data.
* If the clicked category ID and product ID is -1, it means the data is not clicked data.
* For the order line, you can order more than one product at a time, so the category ID and product ID can be more than one, id between the use of commas to separate, if this is not the order line, the data is null.
* Payment behavior and order line is similar

### Detailed field description:

| Index | Field Name         | Field Type | Field Meaning                            |
| ----- | ------------------ | ---------- | ---------------------------------------- |
| 1     | date               | String     | The date of the user's click action      |
| 2     | user_id            | Long       | ID of the user                           |
| 3     | session_id         | String     | ID of the session                        |
| 4     | page_id            | Long       | ID of the page                           |
| 5     | action_time        | String     | The time point of the action             |
| 6     | search_keyword     | String     | The keyword of the user's search         |
| 7     | click_category_id  | Long       | The ID of a product category             |
| 8     | click_product_id   | Long       | The ID of a product                      |
| 9     | order_category_ids | String     | The set of IDs of all categories in one order |
| 10    | order_product_ids  | String     | The set of IDs of all products in an order |
| 11    | pay_category_ids   | String     | The set of IDs of all categories in one  |
| 12    | pay_product_ids    | String     | The set of IDs of all products in one payment |
| 13    | city_id            | Long       | City ID                                  |

## Requirements 1 : Top10 Hot Categories

### Requirement Description

Category refers to the classification of products, large e-commerce website category is divided into multiple levels, our project in the category is only one level, different companies may have different definitions of popular. We count popular categories according to the amount of clicks, orders and payments of each category.

* Shoes ( Number of clicks, Number of orders, Number of payments )
* Clothes ( Number of clicks, Number of orders, Number of payments )
* Computers ( Number of clicks, Number of orders, Number of payments )

The requirements of this project are: first ranking according to the number of clicks, the top one will rank high (Descending order); if the number of clicks is the same, then compare the number of orders; if the number of orders is the same again, then compare the number of payments.

### Implmentation

The number of clicks, orders placed and payments made for each category are counted separately.

* (category, total number of clicks)
* (category, total number of orders placed)
* (category, total number of payments)

```scala
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

    //Close Spark Connection
    sc.stop()
  }
}

```

In method one, we use reduceBykey() many times, which causes many shuffles and affects performance. Besides, cogroup() will also make spark execute shuffle stage which will take a lot of memory and consume a lot of resources if the data is large.

### Optimization Ⅰ

We can map the data before aggregating it, thus avoiding multiple shuffle phases.

```scala
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

```



### Optimization Ⅱ

Use accumulator to improve performance.

```scala
package ca.tangaot.bigdata.spark.req

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object Spark_HotCategoryTop10_Accumulator {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10")
    val sc = new SparkContext(conf)
    // Read data from file
    val fileData = sc.textFile("input/user_visit_action.txt")
    val acc: HotCategoryAccumulator = new HotCategoryAccumulator()

    sc.register(acc, "HotCategory")

    // Add data into Accumulator
    fileData.foreach(
      line => {
        val data = line.split("_")
        if (data(6) != "-1") {
          acc.add((data(6),"click"))
        } else if (data (8) != "null"){
          val cid = data(8).split(",")
          cid.foreach(
            id => acc.add( ( id,"order") )
          )
        } else if (data (10) != "null"){
          val cid = data(10).split(",")
          cid.foreach(
            id => acc.add( ( id,"payment") )
          )
        }
      }
    )

    //Get results from Accumulator
    val resultMap: mutable.Map[String, BehaviorCnt] = acc.value
    val top10: immutable.Seq[BehaviorCnt] = resultMap.map(_._2).toList.sortWith(
      (left, right) => {
        if (left.clickCnt > right.clickCnt) {
          true
        } else if (left.clickCnt == right.clickCnt) {
          if (left.orderCnt > right.orderCnt) {
            true
          } else if (left.orderCnt == right.orderCnt) {
            left.paymentCnt > right.paymentCnt
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10) // get top 10
    //print result
    top10.foreach(println)
    sc.stop()
  }
  case class BehaviorCnt(var cid : String,
                          var clickCnt : Int,
                          var orderCnt : Int,
                          var paymentCnt : Int)

  // Define Accumulator
  // 1. Inherit AccumulatorV2
  // 2. Define the generic type
  // IN : (CategoryID, BehaviorType)
  // OUT : Map[CategoryID, ActionCnt]
  // 3. Override methods (3 + 3)

  class HotCategoryAccumulator extends AccumulatorV2[(String,String),mutable.Map[String,BehaviorCnt]] {
    private val map = mutable.Map[String,BehaviorCnt]()
    override def isZero: Boolean = {
      map.isEmpty
    }

    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, BehaviorCnt]] = {
      new HotCategoryAccumulator
    }

    override def reset(): Unit = {
      map.clear()
    }

    override def add(v: (String, String)): Unit = {
      val (cid , behaviorType) = v
      val hcc: BehaviorCnt = map.getOrElse(cid, BehaviorCnt(cid,0,0,0))
      behaviorType match {
        case "click" => hcc.clickCnt += 1
        case "order" => hcc.orderCnt += 1
        case "payment" => hcc.paymentCnt += 1
      }
      map.update(cid,hcc)
    }

    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, BehaviorCnt]]): Unit = {
      other.value.foreach{
        case(cid, otherHcc) => {
          val thisHcc: BehaviorCnt = map.getOrElse(cid, BehaviorCnt(cid,0,0,0))
          thisHcc.clickCnt += otherHcc.clickCnt
          thisHcc.orderCnt += otherHcc.orderCnt
          thisHcc.paymentCnt += otherHcc.paymentCnt
          map.update(cid,thisHcc)
        }
      }
    }

    override def value: mutable.Map[String, BehaviorCnt] = {
      map
    }
  }

}

```






