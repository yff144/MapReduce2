package for0118;

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

public class ForIntOrderMR {
    public  static  class ForMap extends Mapper<LongWritable,Text,IntWritable,NullWritable>{
        IntWritable intWritable=new IntWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            int i=Integer.parseInt(s);
            intWritable.set(i);
            System.out.println(i);
            context.write(intWritable,NullWritable.get());
        }
    }
    public static class ForReduce extends Reducer<IntWritable,NullWritable,IntWritable,NullWritable>{
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            context.write(key,NullWritable.get());
        }
    }
    public static class MyInter extends IntWritable.Comparator{

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setSortComparatorClass(MyInter.class);
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class"));
        job.waitForCompletion(true);
    }
}
