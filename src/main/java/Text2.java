import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Text2 {
    public static class ForMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
        IntWritable intWritable=new IntWritable();
        Text text=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []str=value.toString().split(" ");

            for(String s:str){
                text.set(s);
                context.write(text,intWritable);
            }
        }
    }
  public static class ForReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum=0;

            for(IntWritable i:values){
                sum++;
            }
            intWritable.set(sum);
            context.write(key,intWritable);
        }
    }

    public  static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setReducerClass(ForReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
      // job.setOutputValueClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job, new Path("D://class12"));
        job.waitForCompletion(true);
    }
}

