import java.util.Date

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by MTL on 2019/11/26
  */
object AdverStat {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("adver").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    // 正常应该使用下面的方式创建
    //    val streamingContext = StreamingContext.getActiveOrCreate(checkpointDir, func)
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val kafka_brokers = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)

    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group1",
      // auto.offset.reset
      // latest: 先去Zookeeper获取offset, 如果有, 直接使用, 如果没有, 从最新的数据开始消费;
      // earliest: 先去Zookeeper获取offset, 如果有, 直接使用, 如果没有, 从最开始的数据开始消费;
      // none: 先去Zookeeper获取offset, 如果有, 直接使用, 如果没有, 直接报错;
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...] RDD[message] message:key value
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出了DStream里面的每一条数据的value值
    // adRead Time ValueD Stream: DStream[RDD RDD RDD ...] RDD[String]
    // String:timestamp province city userid adid
    val adReadTimeValueDStream = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream = adReadTimeValueDStream.transform {
      logRDD =>
        // blackListArray: Array[AdBlacklist] AdBlacklist: userId
        val blackListArray = AdBlacklistDAO.findAll()

        // userIdArray: Array[Long] [userId1, userId2, ...]
        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter {
          // log : timestamp province city userid adid
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }
    //    adReadlTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))

    streamingContext.checkpoint("./spark-streaming")

    adRealTimeFilterDStream.checkpoint(Duration(10000))

    // 需求一
    generateBlackList(adRealTimeFilterDStream)

    // 需求二: 各省市一天中的广告点击量
    val key2ProvinceCittyCountDStream = provinceCityClickStat(adRealTimeFilterDStream)

    // 需求三: 统计各省Top3 热门广告
    provinceTop3Adver(sparkSession, key2ProvinceCittyCountDStream)

    // 需求四: 最近一个消失广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream = adRealTimeFilterDStream.map{
      // log: timestamp province city userId adId
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // yyyyMMddHHmm
      val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))
        val adId = logSplit(4).toLong
        val key = timeMinute + "_" + adId
        (key, 1L)
    }

    val key2WindowDStream = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long, b:Long) => (a+b), Minutes(60), Minutes(1))
    key2WindowDStream.foreachRDD{
      rdd=> rdd.foreachPartition{
        items =>
          val trendArray = new ArrayBuffer[AdClickTrend]()
          for((key, count) <- items){
            val keySplit = key.split("_")
            // yyyyMMddHHmm
            val timeMinute = keySplit(0)
            val date = timeMinute.substring(0, 8)
            val hour = timeMinute.substring(8, 10)
            val minute = timeMinute.substring(10)
            val adId = keySplit(1).toLong

            trendArray += AdClickTrend(date, hour, minute, adId, count)
          }

          AdClickTrendDAO.updateBatch(trendArray.toArray)
      }
    }


  }

  def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCittyCountDStream: DStream[(String, Long)]) = {
    // key2ProvinceCittyCountDStream: [RDD[(key, count)]]
    // key: date_province_city_adId
    // newKey : date_province_adId
    val key2ProvinceCountDStreame = key2ProvinceCittyCountDStream.map{
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val province = keySplit(1)
        val adId = keySplit(3)

        val newKey = date + "_" + province + "_" + adId
        (newKey, count)
    }
    val key2ProvinceAggrCountDStream = key2ProvinceCountDStreame.reduceByKey(_+_)

    val top3DStream = key2ProvinceAggrCountDStream.transform{
      rdd =>
        //rdd:RDD[(key, count)]]
        // key: date_province_adId
        val basicDateRDD = rdd.map{
          case (key, count) =>
            val keySplit = key.split("_")
            val date = keySplit(0)
            val province = keySplit(1)
            val adId = keySplit(2).toLong

            (date, province, adId, count)
        }
        //做成临时表
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adId", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date, province, adId, count from (" +
          "select date, province, adId, count, " +
          "row_number() over(partition by date, province order by count DESC) rank from tmp_basic_info) t " +
          "where rank < 3"

        sparkSession.sql(sql).rdd
    }

    top3DStream.foreachRDD{
      //rdd: RDD[row]
      rdd =>
        rdd.foreachPartition{
          //items : row
          items =>
            val top3Array = new ArrayBuffer[AdProvinceTop3]()

            for(item <- items){
              val date = item.getAs[String]("date")
              val province = item.getAs[String]("province")
              val adId = item.getAs[Long]("adId")
              val count = item.getAs[Long]("count")

              top3Array += AdProvinceTop3(date, province, adId, count)
            }

            AdProvinceTop3DAO.updateBatch(top3Array.toArray)
        }
    }

  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream : DStream[RDD[String]] String -> log : timestamp province city userId adId
    // key2ProvinceCityDStream : DStream[RDD[key, 1L]]
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map{
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        // dateKey : yy-mm-dd
      val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val province = logSplit(1)
        val city = logSplit(2)
        val adId = logSplit(4)

        val key = dateKey + "_" + province + "_" + city + "_" + adId + "_"

        (key, 1L)
    }

    // key2StateDStream: 某一天一个省的一个城市中某一个广告的点击次数的累计
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long]{
      (values:Seq[Long], state:Option[Long]) =>
        var newValue = 0L
        if(state.isDefined)
          newValue = state.get
        for (value <- values){
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD{
      rdd =>
        rdd.foreachPartition{
          items =>
            val adStatArray = new ArrayBuffer[AdStat]()
            // key: date province city adId
            for((key, count) <- items){
              val keySplit = key.split("_")
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adId = keySplit(3).toLong

              adStatArray += AdStat(date, province, city, adId, count)
            }

            AdStatDAO.updateBatch(adStatArray.toArray)
        }
    }
    key2StateDStream

  }

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    // adRealTimeFilterDStream: DStream[RDD[String]]] String -> log : timestamp province city userid addid
    val key2NumDStream = adRealTimeFilterDStream.map {
      //log: timestamp province city userid adid
      case log =>
        val logSplit = log.split(" ")
        val timeStamp = logSplit(0).toLong
        val dateKey = DateUtils.formatDateKey(new Date(timeStamp))
        val userId = logSplit(3).toLong
        val adid = logSplit(4).toLong
        val key = dateKey + "_" + userId + "_" + adid
        (key, 1L)
    }

    val key2CountDStream = key2NumDStream.reduceByKey(_+_)

    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for((key, count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong

            clickCountArray += AdUserClickCount(date, userId, adid, count)
          }

          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }

    // key2BlackListDStream: DStream[RDD[(key, count)]]
    val key2BlackListDStream = key2CountDStream.filter{
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        clickCount > 100
    }

    // key2BlackListDStream.map: DStream[RDD[userId]]
    val userIdDStream = key2BlackListDStream.map{
      case (key, count) =>
        key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val userIdArray = new ArrayBuffer[AdBlacklist]()

          for(item <- items){
            userIdArray += AdBlacklist(item)
          }

          AdBlacklistDAO.insertBatch(userIdArray.toArray)
      }
    }

  }
}