import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable

/**
  * Created by MTL on 2019/11/12
  */
object PageConvertStat {

  def main(args: Array[String]): Unit = {

    // 获取配置参数
    val jsonStr = ConfigurationManager.config.getString(Constants.TASK_PARAMS);
    val taskParam = JSONObject.fromObject(jsonStr)

    // 获取唯一主键
    val taskUUID = UUID.randomUUID().toString

    // 创建sparkConfig
    val sparkConf = new SparkConf().setAppName("pageConvert").setMaster("local[*]")

    // 创建sparkSession
    val sparkSession = SparkSession.builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    // 获取用户行为数据
    val sessionId2ActionRDD =  getUserVisitAction(sparkSession, taskParam)

    // 获取需要统计的页面  targetPageFlow:"1,2,3,4,5,6,7"}
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)

    val pageFlowArray = pageFlowStr.split(",")
    // [1_2, 2_3, 3_4....]
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail)
      .map{
        case (page1, page2) =>
          page1 + "_" + page2
      }

    // sessionId2ActionRDD: RD[(sessionId, action)]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()
    // 过滤之后, (1_2, 1), (1_2, 1), (2_3, 1)....
    val pageSplitNumRDD = sessionId2GroupRDD.flatMap{
      case (sessionId, iterableAction) =>
        val sortList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        val pageList = sortList.map{
          case action => action.page_id
        }

        // pageList: 1_2, 4_5 .....
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail)
          .map{
            case (page1, page2) =>
              page1 + "_" + page2
          }

        val pageSplitFilter = pageSplit.filter{
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        pageSplitFilter.map((_, 1L))
    }

    // countBy key 是返回map 而不是rdd, (1_2, 4)....
    val pageSplitCountMap = pageSplitNumRDD.countByKey()
    // first page id
    val startPage = pageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter{
      case (sessionId, action) => {
        action.page_id == startPage
      }
    }.count()

    getPageConvert(sparkSession, taskUUID, targetPageSplit, startPageCount, pageSplitCountMap)

  }

  def getPageConvert(sparkSession: SparkSession, taskUUID: String, targetPageSplit: Array[String], startPageCount: Long, pageSplitCountMap: collection.Map[String, Long])= {
    val pageSplitRatio = new mutable.HashMap[String, Double]()

    var lastPageCount = startPageCount.toDouble

    for(pageSplit <- targetPageSplit){
      // 第一次循环
      val currentPageSplitCount = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio = currentPageSplitCount / lastPageCount

      pageSplitRatio.put(pageSplit, ratio)

      lastPageCount = currentPageSplitCount
    }

    val converStr = pageSplitRatio.map{
      case (pageSplit, ratio) =>
        pageSplit + "=" + ratio
    }.mkString("|")

    val pageSplit = PageSplitConvertRate(taskUUID, converStr)

    val pageSplitRatioRDD = sparkSession.sparkContext.makeRDD(Array(pageSplit))


    import sparkSession.implicits._
    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate")
      .mode(SaveMode.Append)
      .save()

  }

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from user_visit_action where date >= '"+startDate+"' and date <= '"+endDate+"'"

    import sparkSession.implicits._
    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

}
