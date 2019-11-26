/**
  * Created by MTL on 2019/10/17
  */
case class SortKey(clickCount:Long, orderCount:Long, payCount:Long) extends Ordered[SortKey]{

  // this.compare(that)
  override def compare(that: SortKey): Int = {
    if(this.clickCount - that.clickCount != 0){
      return (this.clickCount - that.clickCount).toInt
    }else if(this.orderCount - that.orderCount != 0){
      return (this.orderCount - that.orderCount).toInt
    }else{
      return (this.payCount - that.orderCount).toInt
    }
  }

}
