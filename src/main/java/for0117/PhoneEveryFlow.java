package for0117;

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

public class PhoneEveryFlow {
    public static class ForMap extends Mapper<LongWritable,Text,Text,Flow>{
        Text text=new Text();
        Flow flow=new Flow();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []str=value.toString().split("\t");
            flow.setUp(Integer.parseInt(str[8]));
            flow.setDown(Integer.parseInt(str[9]));
            flow.setPhoneNumber(str[1]);
            flow.setAll(Integer.parseInt(str[8])+Integer.parseInt(str[9]));
            text.set(str[1]);
            context.write(text,flow);
        }
    }
    public static class ForReduce extends Reducer<Text,Flow,Flow,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Flow> values, Context context) throws IOException, InterruptedException {
           int upsum=0;
           int downsum=0;
            for(Flow i:values){
                upsum+=i.getUp();
                downsum+=i.getDown();
            }
            Flow flow=new Flow(key.toString(),upsum,downsum,upsum+downsum);
            context.write(flow,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flow.class);
        job.setOutputKeyClass(Flow.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job,new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class"));
        job.waitForCompletion(true);
    }
}
