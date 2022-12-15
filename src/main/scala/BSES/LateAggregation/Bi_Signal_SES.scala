package BSES.LateAggregation

import org.apache.spark.sql.{DataFrame, Dataset, Row}

import java.sql.Timestamp
import scala.collection.mutable
import scala.reflect.io.Path
import scala.util.control.Breaks.break

class WindowInterval(start:Timestamp , end:Timestamp){
  var low = start
  var up= end
  override def toString(): String = {
    return "[ "+ low +" , "+up+" ]";
  }
}
class Pair(cnt:Long, agg:Long)
{
  var count = cnt
  var max=agg
}
class Bi_Signal_SES (size: Int, beta:Int ,mes:Long , df:DataFrame)
{
  var Window_Manager: mutable.Map[WindowInterval, Pair] = mutable.Map[WindowInterval, Pair]()
  var Event_Manager: mutable.Map[String, Timestamp] = mutable.Map[String, Timestamp]()
  var MaxEventSize:Long= mes
  var PROPOGATION_STEP: Int = beta
  var WINDOW_SIZE: Int = size
  val FileToStoreResult="bses_1hr.txt"


  def createWindow: (Timestamp) => Unit = (tran_time) => {

    if (tran_time.getTime() % PROPOGATION_STEP == 0) {

      val END: Timestamp = new Timestamp(tran_time.getTime() + 1000 * WINDOW_SIZE)

      val key: WindowInterval = new WindowInterval(tran_time, END)

      val value:Pair = new Pair(0,0)

      Window_Manager += (key -> value)
    }

  }

  def InsertEvent(ad_id:String, tag:String,time: Timestamp): Unit =
  {
    if(tag.equals("start"))
    {
        //if start signal, then add to event manager.
        Event_Manager+= (ad_id-> time)
    }
    else
    {
        //if end signal , then check for matching start signal from event manager
        if(Event_Manager.contains(ad_id) )
        {
          //If starting signal found , then calculate it's valid time

          // st = 10
          // end = 30
          //mes = 5 , 10+5 = 15
          val start_timestamp = new Timestamp(Event_Manager(ad_id).getTime() + MaxEventSize*1000)
          val end_timestamp = time

          for ((window: WindowInterval, p:Pair) <- Window_Manager)
          {
              //checks if start signal is still valid , and ends here
              if ( !start_timestamp.before(window.low) &&  end_timestamp.before(window.up) && end_timestamp.after(window.low))
              {
                 val agg_max =  end_timestamp.getTime() - Event_Manager(ad_id).getTime()

                 val c:Long = Window_Manager(window).count + 1;

                val updated_val: Pair = new Pair( c  , Math.max(agg_max, p.max))

                 Window_Manager(window) = updated_val
              }

              //if signal is already expired, simply ignore, remove it from event manager
              else if(start_timestamp.before(window.low) )
              {
                  break;
              }
          }
          Event_Manager -= ad_id
        }
    }
  }

  def purge(tran_time:Timestamp): Unit =
  {
    //Purge all expired events from event manager
    for ((ad_id: String, start_time: Timestamp) <- Event_Manager)
    {
        val valid_life = new Timestamp(start_time.getTime() + MaxEventSize*1000)

        if (valid_life.before(tran_time)) //invalid
        {
            Event_Manager-=ad_id
        }
    }
  }

  def Release(currentTimestamp: Timestamp): Unit = {

    for ((window: WindowInterval, p:Pair) <- Window_Manager)
    {
      //If window has expired, then remove it.
      if (window.up.before(currentTimestamp))
      {
        Window_Manager -= (window)
        Path(FileToStoreResult).createFile().appendAll(window.toString() + " -> " + p.max + "  " + p.count+"  "+currentTimestamp + "\n")
      }

    }
  }


  def check: (Dataset[Row], Long) => Unit = (df: Dataset[Row], batchId: Long) => {

    val rows = df.collect()

    rows.foreach(row => {

      val ad_id = row.get(0).asInstanceOf[String]
      val tag = row.get(1).asInstanceOf[String]
      val start = row.get(2).asInstanceOf[Timestamp]
      val tran_time = row.get(3).asInstanceOf[Timestamp]

      createWindow(tran_time)
      purge(tran_time)
      InsertEvent(ad_id, tag, start)
      Release(tran_time)

    })
  }

  def run(): Unit = {
    df.writeStream.foreachBatch(check).outputMode("append")
      .start()
  }

}
