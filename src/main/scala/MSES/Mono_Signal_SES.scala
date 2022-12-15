package MSES

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import java.sql.Timestamp
import scala.collection.mutable
import scala.reflect.io.Path

class WindowInterval(start:Timestamp , end:Timestamp){
  var low = start
  var up= end

  override def toString(): String = {
    return "[ "+ low +" , "+up+" ]";
  }
}

class Mono_Signal_SES (size: Int, beta:Int , ttp:Long, df:DataFrame){

  var Window_Manager: mutable.Map[WindowInterval, Int] = mutable.Map[WindowInterval, Int]()

  var PROPOGATION_STEP: Int =beta

  var WINDOW_SIZE: Int =size

  val FileToStoreResult: String = "result_ttp_10.txt"


  val TimeToPostpone:Long = ttp

  def createWindow:(Timestamp)=>Unit = (tran_time) =>{

    if ( tran_time.getTime() % PROPOGATION_STEP == 0)
    {

      var END: Timestamp  = new Timestamp(tran_time.getTime() + 1000 * WINDOW_SIZE)

      var key:WindowInterval = new WindowInterval(tran_time, END)

      Window_Manager += ( key -> 0)

    }

  }

  def InsertEvent(end_event_time : Timestamp): Unit = {


    for (  (window:WindowInterval, count:Int) <-Window_Manager)
    {

          //Insert events considering TTP as bound interval

          val DelayWindowRelease = new Timestamp(window.up.getTime() + TimeToPostpone*1000)

          // Allen predicate used  to assign event to ongoing windows.
          // Predicate checks if event ends inside current window , if so, then assign it, else ignore

          var PREDICATE: Boolean = end_event_time.before(DelayWindowRelease) && end_event_time.after(window.low)

          if(PREDICATE)
          {
              //Assigning event to the window.
              //Result is stored in partial aggregated state , therefore we maintain count of number of events for a window interval, by incrementing as and when, an event is inserted.
              Window_Manager(window)= Window_Manager(window)+1

          }

    }
  }


  def Release(currentTimestamp: Timestamp): Unit = {

    for (  (window:WindowInterval, count:Int) <- Window_Manager)
    {
      //If window has expired, then remove it.
      val DelayWindowRelease = new Timestamp(window.up.getTime() + TimeToPostpone * 1000)

      if (DelayWindowRelease.before(currentTimestamp))
        {
            Window_Manager -= (window)

            Path(FileToStoreResult).createFile().appendAll(window.toString()+" -> "+count+"  "+currentTimestamp+"\n")
        }

    }
  }


  def check:(Dataset[Row],Long)=>Unit = (df: Dataset[Row], batchId: Long) => {

    val rows = df.collect()

    rows.foreach(row => {

      val end = row.get(1).asInstanceOf[Timestamp]
      val start = row.get(0).asInstanceOf[Timestamp]
      val tran_time = row.get(2).asInstanceOf[Timestamp]

      createWindow(tran_time)

      InsertEvent(end)

      Release(tran_time)

    })
  }


  def run(): Unit = {
    df
      .writeStream
      .foreachBatch(check)
      .outputMode("append")
      .start()
  }
}
