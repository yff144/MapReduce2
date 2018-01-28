package for0119;

import for0117.Test;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TestCombinerForAvgMR {

    public static class ForMap extends Mapper<LongWritable,Text,Text,AvgEntity>{
        Text text=new Text();
        AvgEntity avgEntity=new AvgEntity();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            String []strings=s.split(" ");
            text.set(strings[0]);
            avgEntity.setHot(Integer.parseInt(strings[1]));
            context.write(text,avgEntity);
        }
    }
    public static class ForCombiner extends Reducer<Text,AvgEntity,Text,AvgEntity>{
        Text text=new Text();
        AvgEntity avgEntity=new AvgEntity();
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity a:values){
                count++;
                sum+=a.getHot();
            }
            avgEntity.setCount(count);
            avgEntity.setHot(sum);
            text.set(key);
            context.write(text,avgEntity);
        }
    }
    public static class ForReduce extends Reducer<Text,AvgEntity,Text, DoubleWritable>{
        Text text=new Text();
        DoubleWritable doubleWritable=new DoubleWritable();
        @Override
        protected void reduce(Text key, Iterable<AvgEntity> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int sum=0;
            for(AvgEntity a:values){
                count+=a.getCount();
                sum+=a.getHot();
            }
            text.set(key);
            doubleWritable.set(sum/count);
            context.write(text,doubleWritable);

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        job.setCombinerClass(ForCombiner.class);
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgEntity.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class3"));
        job.waitForCompletion(true);
    }
}
