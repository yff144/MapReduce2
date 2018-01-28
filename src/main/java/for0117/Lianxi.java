package for0117;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Lianxi {
    public static class ForMap extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String s=value.toString();
            Pattern pattern=Pattern.compile("[\\u2E80-\\u9FFF]");
            Matcher matcher=pattern.matcher(s);
            while(matcher.find()){
                String string=matcher.group();
                System.out.println(string);
                text.set(string);
                context.write(text,intWritable);
            }
        }
    }
    public static class ForReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
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
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class5"));
        job.waitForCompletion(true);
    }
}
