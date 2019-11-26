import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * Created by MTL on 2019/10/13
  */
class SessionStatAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]]{
  val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = countMap.isEmpty

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionStatAccumulator

    acc.countMap ++= this.countMap  // 把原始的map直接复制到新的map上
    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!countMap.contains(v)){
      countMap += (v -> 0)
    }
    countMap.update(v, countMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    // (0 /: (1 to 100))(_+_) 左折叠
    // (0 /:(1 to 100)){ case  (item1, item2) => item1 + item2}
    // (1 to 100).foldLeft(0)
    other match{
      case acc:SessionStatAccumulator =>
        // this.countMap /: acc.countMap
        // init value : this.countMap
        // foreach object aac.countMap (k , v)
        acc.countMap.foldLeft(this.countMap){
          case (map, (k, v)) =>{
            map += (k -> (map.getOrElse(k, 0) + v))
          }
        }
    }

  }

  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
