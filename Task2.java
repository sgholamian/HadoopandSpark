/*
  Code based on https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task2 {
    public static class TokenizerMapper 
       extends Mapper<Object, Text, NullWritable, IntWritable>
    {

        private static IntWritable sum = new IntWritable(0);
        private Text word = new Text();
        private Text name_of_movie = new Text();
        private ArrayList<Integer> rates = new ArrayList<Integer>();
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {

            String[] tokens = value.toString().split(",",-1);
            int tmp_sum =0; 
            for (int i = 1; i < tokens.length; i++) {
                if( !tokens[i].equals(""))
                {   
                    tmp_sum++;
                    
                        
                }
                else 
                    rates.add(0);
            }
            sum.set(tmp_sum);
            context.write(NullWritable.get(),sum);
        }
    }
  
    public static class IntSumReducer 
       extends Reducer<NullWritable,IntWritable,NullWritable,IntWritable> 
    {
        private IntWritable result = new IntWritable();

        public void reduce(NullWritable key, Iterable<IntWritable> values, 
                           Context context
                           ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(NullWritable.get(), result);
        }
    }

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "T2");
    job.setJarByClass(Task2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(IntWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
