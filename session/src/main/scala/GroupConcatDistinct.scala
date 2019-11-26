import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, StringType, StructField, StructType}

/**
  * Created by MTL on 2019/11/19
  */
class GroupConcatDistinct  extends  UserDefinedAggregateFunction{

  // UDAF: 输入类型为String
  override def inputSchema: StructType = StructType(StructField("cityInfo", StringType)::Nil)

  // 缓冲区类型
  override def bufferSchema: StructType = StructType(StructField("bufferCityInfo", StringType)::Nil)

  // 输出数据类型
  override def dataType: DataType = StringType

  // 保证输入一致输出一致
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = ""
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    var bufferCityInfo = buffer.getString(0)
    val cityInfo = input.getString(0)

    if (!bufferCityInfo.contains(cityInfo)){
      if("".equals(bufferCityInfo)){
        bufferCityInfo += cityInfo
      }else{
        bufferCityInfo += "," + cityInfo
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    var buferCityInfo1 = buffer1.getString(0)
    val buferCityInfo2 = buffer2.getString(0)
    // 判断去重
    for(cityInfo <- buferCityInfo2.split(",")){
      if (!buferCityInfo1.contains(cityInfo)){
        if ("".equals(buferCityInfo1)){
          buferCityInfo1 += cityInfo
        }else{
          buferCityInfo1 += "," + cityInfo
        }
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getString(0)
  }
}
