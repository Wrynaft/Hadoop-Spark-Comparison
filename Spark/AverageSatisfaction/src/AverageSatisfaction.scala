import org.apache.spark.{SparkConf, SparkContext}
import java.lang.management.ManagementFactory
 
object AverageSatisfaction {
  def main(args: Array[String]): Unit = {
    val inputPath  = if (args.length > 0) args(0) else "hdfs://quickstart.cloudera:8020/groupassignment/ai_usage.csv"
    val outputPath = if (args.length > 1) args(1) else "hdfs://quickstart.cloudera:8020/group-output-spark-avg-satisfaction"
 
    val conf = new SparkConf().setAppName("Spark Avg Satisfaction").setMaster("local[*]")
    val sc = new SparkContext(conf)
 
    val startTime = System.nanoTime()
 
    val lines = sc.textFile(inputPath)
    val header = lines.first()
    val data = lines.filter(_ != header)
 
    val recordCount = data.count()
 
    // (Discipline, (rating, 1)) then reduce to (sum, count) then avg
    val avgByDiscipline = data
      .flatMap { line =>
        val cols = line.split(",", -1).map(_.trim)
        if (cols.length > 10 && cols(2).nonEmpty && cols(10).nonEmpty) {
          try {
            Some((cols(2), (cols(10).toDouble, 1L)))
          } catch {
            case _: Throwable => None
          }
        } else None
      }
      .reduceByKey { case ((sum1, c1), (sum2, c2)) => (sum1 + sum2, c1 + c2) }
      .mapValues { case (sum, c) => sum / c }
      .sortByKey(ascending = true)
 
    avgByDiscipline.saveAsTextFile(outputPath)
 
    // METRICS
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
    println(s"Execution Time: $duration seconds")
    println(s"Record Count: $recordCount")
    println(s"Throughput: ${recordCount / duration} records per second")
 
    val rt = Runtime.getRuntime
    val usedMemory = (rt.totalMemory - rt.freeMemory) / (1024.0 * 1024.0)
    println(s"Used Memory: $usedMemory MB")
 
    val mx = ManagementFactory.getMemoryMXBean
    val heap = mx.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
    val nonHeap = mx.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0)
    println(s"Used Heap Memory: $heap MB")
    println(s"Used Non-Heap Memory: $nonHeap MB")
 
    sc.stop()
  }
}
