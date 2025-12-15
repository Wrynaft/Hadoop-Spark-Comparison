import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSatisfaction {
	// Define a counter
	public static enum Counter{
		RECORD_COUNT
	}

  public static class AvgSatisRatingMapper
       extends Mapper<Object, Text, Text, DoubleWritable>{
	  
    private Text discipline = new Text();
    private DoubleWritable satisRating = new DoubleWritable();
    private int lineNumber = 0;

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// Skip header row
      if (lineNumber == 0){
    	  lineNumber++;
    	  return;
      }
      
      // Split input line into fields using commas
      String[] fields = value.toString().split(",");
      
      try{
    	  // Task type field is at index 6, satisfaction rating field is at index 10
    	  String disc = fields[2].trim();
    	  double ratings = Double.parseDouble(fields[10].trim());
    	  
    	  discipline.set(disc);
    	  satisRating.set(ratings);
    	  context.write(discipline, satisRating);
    	  
    	  context.getCounter(Counter.RECORD_COUNT).increment(1);
      } catch (Exception e){
    	  System.err.println("Error processing record: " + value.toString() + " - " + e.getMessage());
      }
      lineNumber++;
    }
  }

  public static class AvgSatisRatingReducer
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable avgRatings = new DoubleWritable();

    public void reduce(Text key, Iterable<DoubleWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      double sum = 0;
      int count = 0;
      for (DoubleWritable val : values) {
        sum += val.get();
        count++;
      }
      // Calculate average satisfaction rating
      avgRatings.set(sum / count);
      context.write(key, avgRatings);
    }
  }

  public static void main(String[] args) throws Exception {
	  long startTime = System.nanoTime();
	  Configuration conf = new Configuration();
	  Job job = Job.getInstance(conf, "word count");
	  job.setJarByClass(AverageSatisfaction.class);
	  job.setMapperClass(AvgSatisRatingMapper.class);
	  job.setCombinerClass(AvgSatisRatingReducer.class);
	  job.setReducerClass(AvgSatisRatingReducer.class);
	  job.setOutputKeyClass(Text.class);
	  job.setOutputValueClass(DoubleWritable.class);
	  FileInputFormat.addInputPath(job, new Path(args[0]));
	  FileOutputFormat.setOutputPath(job, new Path(args[1]));
	  // Wait for job to complete
	  boolean jobStatus = job.waitForCompletion(true);
	  
	  // Calculate metrics
	  long endTime = System.nanoTime();
	  double duration = (endTime - startTime) / 1e9d;
	  System.out.println("Exectuion Time: " + duration + " seconds");
	  long recordCount = job.getCounters().findCounter(Counter.RECORD_COUNT).getValue();
	  double throughput = recordCount / duration; // records per second
	  System.out.println("Throughput: " + throughput + " records per second");
	  
	  Runtime runtime = Runtime.getRuntime();
	  double usedMemory = (runtime.totalMemory() - runtime.freeMemory()) / (1024.0 * 1024.0); // in MB
	  System.out.println("Used Memory: " + usedMemory + " MB");
	  
	  MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
	  double usedHeapMemory = memoryMXBean.getHeapMemoryUsage().getUsed() / (1024.0 * 1024.0); // Used heap memory in MB
	  double usedNonHeapMemory = memoryMXBean.getNonHeapMemoryUsage().getUsed() / (1024.0 * 1024.0); // Used non-heap memory in MB
	  System.out.println("Used Heap Memory: " + usedHeapMemory + " MB");
	  System.out.println("Used Non-Heap Memory: " + usedNonHeapMemory + " MB");
	  
	  // Exit the job when it's finished
	  System.exit(jobStatus ? 0 : 1);
  }
}

