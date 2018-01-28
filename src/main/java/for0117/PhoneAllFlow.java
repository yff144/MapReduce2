
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

public class PhoneAllFlow {
    public static class ForMap extends Mapper<LongWritable,Text,Text,IntWritable>{
        Text text=new Text();
        IntWritable intWritable=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            String []strings=s.split("\t");
            int up=Integer.parseInt(strings[8]);
            int down=Integer.parseInt(strings[9]);
            int all=up+down;
            text.set(strings[1]);
            intWritable.set(all);
            context.write(text,intWritable);
        }
    }
    public static class ForReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        IntWritable intWritable=new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
           int sum=0;
            for(IntWritable i:values){
                sum+=i.get();
            }
            intWritable.set(sum);
            context.write(key,intWritable);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class6"));
        job.waitForCompletion(true);
    }
}
