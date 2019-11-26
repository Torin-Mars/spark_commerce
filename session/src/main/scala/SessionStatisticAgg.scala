import java.util.{Date, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils._
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random


/**
  * Created by MTL on 2019/10/12
  */
object SessionStatisticAgg {

  def main(args: Array[String]): Unit = {

    // get the config
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS) //limit tasks
    // to json object
    val taskParam = JSONObject.fromObject(jsonStr)

    //get the only key
    val taskUUID = UUID.randomUUID().toString
    //create the config
    val sparkConf = new SparkConf().setAppName("app1").setMaster("local[*]")
    // create sparkSession
    val sparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    val actionRDD = getActionRDD(sparkSession, taskParam)

//    actionRDD.foreach(println(_))

    // sessionId2ActionRDD: rdd[(sid, UserVisitAction)]
    val sessionId2ActionRDD = actionRDD.map(
      item => (item.session_id, item)
    )

    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    //    sparkSession.sparkContext.setCheckpointDir()
    sessionId2GroupRDD.cache()
    //    sessionId2GroupRDD.checkpoint()

//    sessionId2ActionRDD.foreach(println(_))
//(0f4a12200bac497f9aaa6b191047a48b,sessionid=0f4a12200bac497f9aaa6b191047a48b|searchKeywords=苹果|clickCategoryIds=44,6,40,91|visitLength=2871|stepLength=12|startTime=2019-10-12 11:10:43|age...
    val sessionId2FullInfoRDD = getFullInfoData(sparkSession, sessionId2GroupRDD)

//    sessionId2FullInfoRDD.foreach(println(_))

    //创建自定义的累加器对象
    val sessionStatAccumulator = new SessionStatAccumulator
    sparkSession.sparkContext.register(sessionStatAccumulator, "sessionAccumulator")

    // filter user info
    val sessionId2FilterRDD = getFilteredData(taskParam, sessionStatAccumulator, sessionId2FullInfoRDD)

    sessionId2FilterRDD.foreach(println(_))

    for((k, v) <- sessionStatAccumulator.value){
      println("k: " + k + " v: " + v)
    }
    
    // 获取最终的统计结果
//    getFinalData(sparkSession, taskUUID, sessionStatAccumulator.value)

    // 新需求二 session 随机抽取
    sessionRandomExtract(sparkSession, taskUUID, sessionId2FilterRDD)

    // sessionID2ActionRDD: RDD[(session, action)]
    // sessionId2FilterRDD: RDD[(sessionId, fullInfo)]
    // sessionId2FilterActionRDD: join (sessionId, action)
    // 获取所有符合过滤条件的action
    val sessionId2FilterActionRDD = sessionId2ActionRDD.join(sessionId2FilterRDD).map{
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }

    // 需求3 前十类目排行
    // top10CategoryArray : Array[(sortKey, countInfo)[
    val top10CategoryArray = top10PopularCategories(sparkSession, taskUUID, sessionId2FilterActionRDD)

    //需求4 :
    // sessionId2FilterActionRDD:RDD[(sessionId, action)]
    top10ActionSession(sparkSession, taskUUID, sessionId2FilterActionRDD, top10CategoryArray)


  }

  def top10ActionSession(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], top10CategoryArray: Array[(SortKey, String)]): Unit = {
    // 过滤点击过top10 品类的action
    // 1: join
    /*  val cid2CountInfoRDD = sparkSession.sparkContext.makeRDD(top10CategoryArray).map{
        case (sortKey, countInfo) =>
          val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toString
          (cid, countInfo)
      }

      val cid2ActionRDD = sessionId2FilterActionRDD.map{
        case (sessionId, action) =>
          val cid = action.click_category_id.toString
          (cid, action)
      }

      cid2CountInfoRDD.join(cid2ActionRDD).map{
        case (cid, (countInfo, action)) =>
          val sid = action.session_id
          (sid, action)
      }*/
    // 2: filter
    // cidArray: Array[Long] cid
    val cidArray = top10CategoryArray.map{
      case (sortKey, countInfo)=>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }

    // 所有点击过热门分类的action
    // sessionId2ActionRDD: RDD[(sessionId, action)]
    val sessionId2ActionRDD = sessionId2FilterActionRDD.filter{
      case (sessionId, action)=>
      cidArray.contains(action.click_category_id)
    }

    // 聚合
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    val cid2SessionCountRDD = sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>
        val categoryCountMap = new mutable.HashMap[Long, Long]()
        for (action <- iterableAction){
          val cid = action.click_category_id
          if(!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }

    // RDD[(cid, iterableSessionCount)]
    // cid2GroupRDD: cid 对应的所有session的点击次数  sessionId=count
    val cid2GroupRDD = cid2SessionCountRDD.groupByKey()
    val top10SessionRdd = cid2GroupRDD.flatMap{
      case (cid, iterableSessionCount) =>
        // true:  item1
        val sorltList = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong> item2.split("=")(1).toLong
        }).take(10)

        val top10Session = sorltList.map{
          case item =>
            val sessionId = item.split("=")(0)
            val count = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)
        }
        top10Session
    }

    import sparkSession.implicits._
    top10SessionRdd.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "top10_session_0308")
      .mode(SaveMode.Append)
      .save()
  }

  def top10PopularCategories(sparkSession: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    // 1, 获取所有发生过股款, 下单, 支付的品类
    var cid2CidRDD = sessionId2FilterActionRDD.flatMap{
      case (sid, action) =>
        val categoryBuffer = new ArrayBuffer[(Long, Long)]()

        //点击行为
        if(action.click_category_id != -1){
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        }else if( action.order_category_ids != null){
          //下单行为
          for(orderCid <- action.order_category_ids.split(",")){
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
          }
        }else if(action.pay_category_ids != null){
          for(payCid <- action.pay_category_ids.split(",")){
            categoryBuffer += ((payCid.toLong, payCid.toLong))
          }
        }

        categoryBuffer
    }

    cid2CidRDD = cid2CidRDD.distinct()

    // 统计品类的点击次数
    val cid2ClickCountRDD = getClickCount(sessionId2FilterActionRDD)
    //    cid2ClickCountRDD.foreach(println(_))

    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    // (52, categoryid=52|orderCount=23...
    val cid2FullCountRDD = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
//    cid2PayInfoRDD.foreach(println(_))
    // 实现自定义二次排序key  SortKey
    val sortKey2FullCountRDD = cid2FullCountRDD.map{
      case(cid, countInfo) =>
        val clickCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }

    // 倒叙指定为false
    val top10Category = sortKey2FullCountRDD.sortByKey(false).take(10)

    val top10CategoryRDD = sparkSession.sparkContext.makeRDD(top10Category).map{
      case (sortKey, countInfo) =>
        val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        val clickCount = sortKey.clickCount
        val orderCount = sortKey.orderCount
        val payCount = sortKey.orderCount

        Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
    }

//    import sparkSession.implicits._
//    top10CategoryRDD.toDF().write
//      .format("jdbc")
//      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
//      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
//      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
//      .option("dbtable", "top10_category_0306")
//      .mode(SaveMode.Append)// 追加形式
//      .save()

    top10Category
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)], cid2ClickCountRDD: RDD[(Long, Long)], cid2OrderCountRDD: RDD[(Long, Int)], cid2PayCountRDD: RDD[(Long, Int)]) = {
    val cid2ClickInfoRDD = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map{
      case (cid, (categoryId, option)) =>
        //是否有数据
        val clickCount = if(option.isDefined) option.get else 0
        val aggrCount = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
    }
    val cid2OrderInfoRDD = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map{
      case (cid, (clickInfo, option)) =>
        val orderCount = if(option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" +
          Constants.FIELD_ORDER_COUNT + "=" + orderCount
        (cid, aggrInfo)
    }
    val cid2PayInfoRDD = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map{
      case (cid, (orderInfo, option)) =>
        val payCount = if(option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" +
          Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, aggrInfo)
    }

    cid2PayInfoRDD
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    val payFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.pay_category_ids != null)


    val payCountRDD = payFilterRDD.flatMap{
      case (sessionId, action) =>
        action.pay_category_ids.split(",").map(item => (item.toLong, 1))
    }

    payCountRDD.reduceByKey(_+_)
  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {

    val orderFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.order_category_ids != null)


    val orderCountRDD = orderFilterRDD.flatMap{
      case (sessionId, action) =>
        action.order_category_ids.split(",").map(item => (item.toLong, 1))
    }

    orderCountRDD.reduceByKey(_+_)
  }

  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
//    val clickFilterRDD = sessionId2FilterActionRDD.filter{
//      case (sessionId, action) => action.click_category_id != -1L
//    }
    // 先进行过滤, 对点击行为对应的action 保留下来
    val clickFilterRDD = sessionId2FilterActionRDD.filter(item => item._2.click_category_id != -1L)

    // 对类目进行整理统计
    val clickNumRDD = clickFilterRDD.map{
      case (sessionId, action) => (action.click_category_id, 1L)
    }

    clickNumRDD.reduceByKey(_+_)
  }

  def sessionRandomExtract(sparkSession: SparkSession, taskUUID: String, sessionId2FilterRDD: RDD[(String, String)]): Unit = {
    // RDD[(dateHour, fullInfo)]
    val dateHour2FullInfoRDD = sessionId2FilterRDD.map{
      case(sid, fullInfo) =>
        val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        // dateHour : yyyy-MM-dd_HH
        val dateHour = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
    }

    //hourCountMap: Map[(dateHour, count)]
    val hourCountMap = dateHour2FullInfoRDD.countByKey()

    // dateHourCountMap: Map[(date, Map[(dateHour, count)])]
    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for((dateHour, count) <- hourCountMap){
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }

    //解决问题一: 一共有多少天
    // 解决问题二: 一天游多少条
    val extractPerDay = 100 / dateHourCountMap.size
    // 一天有多少session  dateHourCountMap(date).value.sum
    // 一个小时有多少session: dateHourCountMap(date)(hour)

    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]


    // dateHourCountMap: Map[(date, Map[(hour, count)]]
    for((date, hourCountMap) <- dateHourCountMap){
      val dateSessionCount = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
      }

      // 我们获取的每个小时要抽取的session 的index

      //制作广播变量
      val dateHourExtractIndexListMapBd = sparkSession.sparkContext.broadcast(dateHourExtractIndexListMap)

      //dateHour2FullInfoRDD: RDD[(dateHour, fullInfo)]
      // dateHour2GroupRDD: RDD[(dateHour, iterableFullInfo)]
      val dateHour2GroupRDD = dateHour2FullInfoRDD.groupByKey()

      // extractSessionRDD: RDD[SessionRandomExtract]
      val extractSessionRDD = dateHour2GroupRDD.flatMap{
        case (dateHour, iterableFullInfo) =>
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)

          val extractList = dateHourExtractIndexListMapBd.value.get(date).get(hour)

          val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()

          var index = 0

          for(fullInfo <- iterableFullInfo){
            if(extractList.contains(index)){
              val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
              val startTime = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
              val searchKeywords = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
              val clickCategories = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

              val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)

              extractSessionArrayBuffer += extractSession

            }
            index += 1
          }
          extractSessionArrayBuffer
      }

      import sparkSession.implicits._
      extractSessionRDD.toDF().write
            .format("jdbc")
            .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
            .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
            .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
            .option("dbtable", "session_extract_0308")
            .mode(SaveMode.Append)// 追加形式
            .save()
    }

  }

  def generateRandomIndexList(extractPerDay:Long, daySessionCount:Long, hourCountMap:mutable.HashMap[String, Long], hourListMap: mutable.HashMap[String, ListBuffer[Int]]):Unit = {
    for((hour, count) <- hourCountMap){
      // 获取一个小时需要抽取的数量
      var hourExrCount = ((count/daySessionCount.toDouble) * extractPerDay).toInt
      // 避免一个小时抽取的数量超过这个小时的数量
      if(hourExrCount > count){
        hourExrCount = count.toInt
      }

      val random = new Random()

      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for(i <- 0 until hourExrCount){
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for(i <- 0 until hourExrCount){
            var index = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)){
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }

      }
    }
  }

  def getFinalData(sparkSession: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]): Unit = {

    // 获取所有符合条件的个数
    val session_count = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    // 不同范围访问时长的个数
    val visit_length_1s_3s = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    // 不同访问步长的个数
    val step_length_1_3 = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6 = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9 = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30 = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60 = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60 = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60 / session_count, 2)

    val stat = SessionAggrStat(taskUUID,
      session_count.toInt, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

//    val statRDD = sparkSession.sparkContext.makeRDD(Array(stat))
//    import sparkSession.implicits._
//    statRDD.toDF().write
//      .format("jdbc")
//      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
//      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
//      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
//      .option("dbtable", "session_stat_0115")
//      .mode(SaveMode.Append)// 追加形式
//      .save()
  }

  def calculateVisitLength(visitLength:Long, sessionStatAccumulator: SessionStatAccumulator):Unit = {
    if (visitLength >= 1 && visitLength <= 3) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
    } else if (visitLength >= 4 && visitLength <= 6) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
    } else if (visitLength >= 7 && visitLength <= 9) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
    } else if (visitLength >= 10 && visitLength <= 30) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
    } else if (visitLength > 30 && visitLength <= 60) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
    } else if (visitLength > 60 && visitLength <= 180) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
    } else if (visitLength > 180 && visitLength <= 600) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
    } else if (visitLength > 600 && visitLength <= 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
    } else if (visitLength > 1800) {
      sessionStatAccumulator.add(Constants.TIME_PERIOD_30m);
    }

  }

  def calculateStepLength(stepLength:Long, sessionStatAccumulator: SessionStatAccumulator):Unit ={
    if (stepLength >= 1 && stepLength <= 3) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_1_3);
    } else if (stepLength >= 4 && stepLength <= 6) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_4_6);
    } else if (stepLength >= 7 && stepLength <= 9) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_7_9);
    } else if (stepLength >= 10 && stepLength <= 30) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_10_30);
    } else if (stepLength > 30 && stepLength <= 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_30_60);
    } else if (stepLength > 60) {
      sessionStatAccumulator.add(Constants.STEP_PERIOD_60);
    }
  }

  def getFilteredData(taskParam: JSONObject, sessionStatAccumulator: SessionStatAccumulator ,sessionId2FullInfoRDD: RDD[(String, String)]) = {
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    // 先判断是不是为空
    var filterInfo = (if(startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if(endAge != null) Constants.PARAM_END_AGE +"=" + startAge + "|" else "") +
      (if(professionals != null) Constants.PARAM_PROFESSIONALS +"=" + startAge + "|" else "") +
      (if(cities != null) Constants.PARAM_CITIES +"=" + startAge + "|" else "") +
      (if(sex != null) Constants.PARAM_SEX +"=" + startAge + "|" else "") +
      (if(keywords != null) Constants.PARAM_KEYWORDS +"=" + startAge + "|" else "") +
      (if(categoryIds != null) Constants.PARAM_CATEGORY_IDS +"=" + startAge + "|" else "")

    //如果有 | 结尾, 就除去
    if(filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FilterRDD = sessionId2FullInfoRDD.filter{
      case(sessionId, fullInfo) =>
        var success = true

//        if(!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
//          success = false
//        if(!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
//          success = false
//        if(!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
//          success = false
//        if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
//          success = false
//        if(!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
//          success = false
//        if(!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
//          success = false

        if(success){
          // 只要进入此处, 就代表此 session符合过滤条件, 进行总数的计数
          sessionStatAccumulator.add(Constants.SESSION_COUNT)
          val visitLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong

          calculateVisitLength(visitLength, sessionStatAccumulator)
          calculateStepLength(stepLength, sessionStatAccumulator)

        }

        success
    }
    sessionId2FilterRDD
  }

  def getFullInfoData(sparkSession: SparkSession, sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    val userId2AggrInfoRDD = sessionId2GroupRDD.map {
      case (sid, iterableAction) =>
        var startTime: Date = null
        var endTime: Date = null

        var userId = -1L

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        var stepLength = 0

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          val actionTime = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime;
          }

          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime;
          }

          val searchKeyword = action.search_keyword
          val clickCategory = action.click_category_id

          if (StringUtils.isNotEmpty(searchKeyword) && !searchKeywords.toString.contains(searchKeyword)) {
            searchKeywords.append(searchKeyword + ",")
          }
          if (clickCategory != -1L && !clickCategories.toString.contains(clickCategory))
            clickCategories.append(clickCategory + ",")

          stepLength += 1
        }

        val searchKw = StringUtils.trimComma(searchKeywords.toString)
        val clickCg = StringUtils.trimComma(clickCategories.toString)

        val visitLength = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sid + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime)

        // to join table userInfo
        (userId, aggrInfo)
    }

    val sql = "select * from user_info"

    import sparkSession.implicits._

    val userInfoRDD = sparkSession.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))

    userId2AggrInfoRDD.join(userInfoRDD).map{
      case (userId, (aggrInfo, userInfo)) =>
        val age = userInfo.age
        val professional = userInfo.professional
        val sex = userInfo.sex
        val city = userInfo.city

        val fullInfo = aggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city + "|"

        val sessionId = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)

        (sessionId, fullInfo)
    }
  }

  def getActionRDD(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"

    // DataFrame  DataSet[Row]
    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd
  }
}
