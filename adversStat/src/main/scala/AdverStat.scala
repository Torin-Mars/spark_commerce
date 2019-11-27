import commons.conf.ConfigurationManager
import commons.constant.Constants
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

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
      "enable.auto.commit" -> (false:java.lang.Boolean)
    )

    // adRealTimeDStream: DStream[RDD RDD RDD ...] RDD[message] message:key value
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    // 取出了DStream里面的每一条数据的value值
    // adRead Time ValueD Stream: DStream[RDD RDD RDD ...] RDD[String]
    // String:timestamp province city userid adid
    val adReadTimeValueDStream = adRealTimeDStream.map(item=> item.value())

    val adReadTimeFilterDStream = adReadTimeValueDStream.transform{
      logRDD =>
        // blackListArray: Array[AdBlacklist] AdBlacklist: userId
        val blackListArray = AdBlacklistDAO.findAll()

        // userIdArray: Array[Long] [userId1, userId2, ...]
        val userIdArray = blackListArray.map(item => item.userid)

        logRDD.filter{
          // log : timestamp province city userid adid
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }
    adReadTimeFilterDStream.foreachRDD(rdd => rdd.foreach(println(_)))

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
