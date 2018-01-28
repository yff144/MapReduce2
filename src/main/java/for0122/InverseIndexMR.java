package for0122;

import for0119.Phone;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Scanner;

public class InverseIndexMR {
    private static String in;

    static {
        Scanner s=new Scanner(System.in);
        in=s.next();
    }
    public static class ForMap extends Mapper<LongWritable,Text,Text,Text>{
        Text tkey=new Text();
        Text tvalue=new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String []strings=value.toString().split(" ");
            FileSplit fileSplit=(FileSplit) context.getInputSplit();
            String name=fileSplit.getPath().getName();
            for(String s:strings){
                if (s.equals(in)){
                    tkey.set(s+" "+name);
                    context.write(tkey,tvalue);
                }
                ;
            }
        }
    }
    public static class ForConbion extends Reducer<Text,Text,Text,Text>{
        Text tkey=new Text();
        Text tvalue=new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            int max=0;
            for(Text t:values){
                count++;
            }

            String []strings=key.toString().split(" ");

            tkey.set(strings[0]);
            tvalue.set(strings[1]+" "+count);
            context.write(tkey,tvalue);
        }
    }
    public static class ForReduce extends Reducer<Text,Text,Text,Text>{
        Text tkey=new Text();
        Text tvalue=new Text();
        int max;
        String sv;
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            tvalue.set(sv+max);
            context.write(tkey,tvalue);
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb=new StringBuilder();

            for(Text i:values){
                String []s=i.toString().split(" ");
                if(max<Integer.parseInt(s[1])){
                    max=Integer.parseInt(s[1]);
                    sv=s[0];
                }
            }
            tkey.set(key);


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();

        job.setMapperClass(ForMap.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ForReduce.class);
        job.setCombinerClass(ForConbion.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job, new Path("D://class3"));
        job.waitForCompletion(true);
    }
}
