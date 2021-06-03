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
