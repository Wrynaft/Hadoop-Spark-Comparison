import org.apache.spark.{SparkConf, SparkContext}
import java.lang.management.ManagementFactory
 
object UsedAgain {
  def main(args: Array[String]): Unit = {
    val inputPath  = if (args.length > 0) args(0) else "hdfs://quickstart.cloudera:8020/groupassignment/ai_usage.csv"
    val outputPath = if (args.length > 1) args(1) else "hdfs://quickstart.cloudera:8020/group-output-spark-usedagain"
 
    val conf = new SparkConf().setAppName("Spark UsedAgain Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
 
    val startTime = System.nanoTime()
 
    val lines = sc.textFile(inputPath)
    val header = lines.first()
    val data = lines.filter(_ != header)
 
    val recordCount = data.count()
 
    val counts = data
      .flatMap { line =>
        val cols = line.split(",", -1).map(_.trim)
        if (cols.length > 9 && cols(6).nonEmpty && cols(9).nonEmpty) {
          Some((s"${cols(6)}|${cols(9)}", 1))
        } else None
      }
      .reduceByKey(_ + _)
      .sortByKey(ascending = true)
 
    counts.saveAsTextFile(outputPath)
 
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
 
 
