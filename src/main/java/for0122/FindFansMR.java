package for0122;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FindFansMR {
    public static class ForMap extends Mapper<LongWritable,Text,Text,Text>{
        Text tkey=new Text();
        Text tvalue=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(":");
            String []strings1=strings[1].split(",");
            for(String s:strings1){
                tkey.set(s);
                tvalue.set(strings[0]);
            }
            context.write(tkey,tvalue);
        }
    }
    public static class ForReduce extends Reducer<Text,Text,Text,Text>{
        Text tkey=new Text();
        Text tvalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
           StringBuilder sb=new StringBuilder();
            for(Text v:values){
                sb.append(v+"\t");
            }
            tkey.set(key);
            tvalue.set(sb.toString());
            context.write(tkey,tvalue);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Job job = Job.getInstance();

        job.setMapperClass(ForMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ForReduce.class);

        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job, new Path("D://class6"));
        job.waitForCompletion(true);
    }
}
