import scala.reflect.io.Path

object GenerateData extends App {
  for(i <- 0 to 300000)
  {
      Path("ADS.txt").createFile().appendAll(java.util.UUID.randomUUID.toString+"\n")
  }
}
