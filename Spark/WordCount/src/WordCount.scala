import org.apache.spark.{SparkConf, SparkContext}
import java.lang.management.ManagementFactory
 
object WordCount {
 
  def main(args: Array[String]): Unit = {
 
    if (args.length < 2) {
      println(
        "Usage: WordCount <input_path> <output_path>\n" +
        "Example:\n" +
        "spark-submit --class WordCount wordcount-spark.jar " +
        "hdfs://quickstart.cloudera:8020/groupassignment/ai_usage " +
        "hdfs://quickstart.cloudera:8020/group-output-spark-wordcount"
      )
      System.exit(1)
    }
 
    val inputPath  = args(0)
    val outputPath = args(1)
 
    val conf = new SparkConf()
      .setAppName("Spark WordCount")
      .setMaster("local[*]")
 
    val sc = new SparkContext(conf)
 
    val startTime = System.nanoTime()
 
    val lines = sc.textFile(inputPath)
    val header = lines.first()
    val data = lines.filter(_ != header)
 
    val recordCount = data.count()
 
    val counts = data
      .flatMap(_.split(","))
      .map(word => (word.trim, 1))
      .reduceByKey(_ + _)
 
    counts.saveAsTextFile(outputPath)
 
    val endTime = System.nanoTime()
    val duration = (endTime - startTime) / 1e9d
 
    println(s"Execution Time: $duration seconds")
    println(s"Record Count: $recordCount")
    println(s"Throughput: ${recordCount / duration} records/sec")
 
    val runtime = Runtime.getRuntime
    val usedMemory = (runtime.totalMemory - runtime.freeMemory) / (1024.0 * 1024.0)
    println(s"Used Memory: $usedMemory MB")
 
    val mx = ManagementFactory.getMemoryMXBean
    println(s"Heap Memory Used: ${mx.getHeapMemoryUsage.getUsed / (1024.0 * 1024.0)} MB")
    println(s"Non-Heap Memory Used: ${mx.getNonHeapMemoryUsage.getUsed / (1024.0 * 1024.0)} MB")
 
    sc.stop()
  }
}

