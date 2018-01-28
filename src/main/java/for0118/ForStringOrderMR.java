package for0118;

import org.apache.commons.lang.ObjectUtils;
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

public class ForStringOrderMR {
    public static class ForMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        Text text=new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            text.set(s);
            System.out.println(s);
            context.write(text,NullWritable.get());
        }
    }
    public static class ForReduce extends Reducer<Text,NullWritable,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            System.out.println(key);
            context.write(key,NullWritable.get());
        }
    }
    public static class MyString extends Text.Comparator{
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            String str1=new String(b1,s1+1,l1-1);
            String str2=new String(b2,s2+1,l2-1);
            if(str1.length()==str2.length()){
                int a=Integer.parseInt(str1);
                int b=Integer.parseInt(str2);
                return a-b;
            }
//            System.out.println(str1+"=========");
//            System.out.println(str2+"-----------");
            return str1.length()-str2.length();
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setSortComparatorClass(MyString.class);
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class4"));
        job.waitForCompletion(true);
    }
}
