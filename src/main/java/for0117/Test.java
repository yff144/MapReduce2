package for0117;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Test {
    public static class ForMapper extends Mapper<LongWritable,Text,WordTimes,NullWritable> {
        private List<WordTimes> list=new ArrayList<WordTimes>(11);
//        {
//            for(int i=0;i<11;i++){
//                list.add(new WordTimes());
//            }
//        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line=value.toString();
            String str[] =line.split("\t");
            WordTimes wordTimes=new WordTimes(str[0],Integer.parseInt(str[1]));
            list.add(wordTimes);
            Collections.sort(list);
            list.remove(10);
        }


        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for(int i=0;i<10;i++){
                context.write(list.get(i),NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance();
        job.setMapperClass(ForMapper.class);
        job.setMapOutputKeyClass(WordTimes.class);
        job.setMapOutputValueClass(NullWritable.class);
        String path = "E://output";
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job, new Path("D://class2"));
        job.waitForCompletion(true);
    }
}
