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

public class Task1 {
    public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, NullWritable>
    {

        //private final static IntWritable max = new IntWritable(0);
        private Text word = new Text();
        private Text name_of_movie = new Text();
          
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            int temp = 0;
            int max=0;
            ArrayList<Integer> rates = new ArrayList<Integer>();
            StringBuilder stringBuilder = new StringBuilder();
            String[] tokens = value.toString().split(",");
            name_of_movie.set(tokens[0]);
            stringBuilder.append(name_of_movie);
            rates.add(0);  
            for (int i = 1; i < tokens.length; i++) {
                if( !tokens[i].equals(""))
                {   temp = Integer.parseInt(tokens[i]);
                    rates.add(temp); 
                    if(temp > max)
                        max=temp;
                    
                        
                }
                else 
                    rates.add(0);
            }
            for(int i = 1; i < rates.size(); i++)
            {
                if(rates.get(i) == max)
                    stringBuilder.append(","+i);
                    
            }
            word=new  Text(stringBuilder.toString());
            if(!stringBuilder.toString().equals(""))
                context.write(word,NullWritable.get());
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
        Job job = new Job(conf, "word count");
        job.setJarByClass(Task1.class);
        job.setMapperClass(TokenizerMapper.class);
        //job.setCombinerClass(IntSumReducer.class);
       // job.setReducerClass(IntSumReducer.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        TextInputFormat.addInputPath(job, new Path(otherArgs[0]));
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
