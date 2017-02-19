/*
  Code based on https://github.com/facebookarchive/hadoop-20/blob/master/src/examples/org/apache/hadoop/examples/WordCount.java
*/

import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.nio.ByteBuffer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Task3 {
    public static class TokenizerMapper 
       extends Mapper<Object, Text, IntWritable, DoubleWritable>
    {

        private  static IntWritable userID = new IntWritable(0);
        private static DoubleWritable rate = new DoubleWritable(0);
        private Text word = new Text();
        private Text name_of_movie = new Text();
        ByteBuffer buffer = ByteBuffer.allocate(8);
          
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            
            int temp_rate=0;
            ArrayList<Integer> rates = new ArrayList<Integer>();
            StringBuilder stringBuilder = new StringBuilder();
            String[] tokens = value.toString().split(",");
            name_of_movie.set(tokens[0]);
            stringBuilder.append(name_of_movie);
            rates.add(0);  
            for (int i = 1; i < tokens.length; i++) {
                if( !tokens[i].equals(""))
                {   temp_rate = Integer.parseInt(tokens[i]);
                    rate.set((float)temp_rate);
                    userID.set(i);
                    context.write(userID,rate);
                    
                        
                }

            }
        }
    }
  
    public static class IntSumReducer 
       extends Reducer<IntWritable, DoubleWritable,IntWritable, Text> 
    {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(IntWritable key, Iterable<DoubleWritable> values, 
                           Context context
                           ) throws IOException, InterruptedException {
              double sum = 0;
              int size=0;
              for (DoubleWritable val : values) {
                sum += val.get();
                size++;
                
              }

              //double total = Math.round((sum / size * 100) * 10) / 10.0;
              //double total = round ( (sum / size), 1);
              double total =  (sum / size);
              String out_this = String.format("%1.1f",total);
              Text output_val = new Text(out_this);
              context.write(key, output_val );
        }
    }
    
    private static double round (double value, int precision) {
        int scale = (int) Math.pow(10, precision);
        return (double) Math.round(value * scale) / scale;
    }
    
/*     public static String fmt(double d)
    {
            System.out.println("Double value isssssssssssssssss: "+ d);
            return String.format("%1.1f",(long)d);

    } */

    public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapred.textoutputformat.separator", ",");
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(Task3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setNumReduceTasks(1);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(DoubleWritable.class);
    TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
    TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
