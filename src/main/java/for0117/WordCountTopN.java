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
import java.util.Set;
import java.util.TreeSet;

public class WordCountTopN {
    public static class ForMap extends Mapper<LongWritable,Text,WordTimes,NullWritable>{
        Set<WordTimes> treeSet=new TreeSet<WordTimes>();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String str=value.toString();
            String []strings=str.split("\t");
            WordTimes wordTimes=new WordTimes(strings[0],Integer.parseInt(strings[1]));
            treeSet.add(wordTimes);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int t=0;
           for(WordTimes w:treeSet){
               if(t==3){
                  break;
               }
               context.write(w,NullWritable.get());
               t++;
           }
        }

        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Job job=Job.getInstance();
            job.setMapperClass(ForMap.class);
            job.setMapOutputKeyClass(WordTimes.class);
            job.setOutputValueClass(NullWritable.class);
            job.setOutputValueClass(WordTimes.class);
            job.setOutputValueClass(NullWritable.class);
            FileInputFormat.setInputPaths(job,new Path("D://a"));
            FileOutputFormat.setOutputPath(job,new Path("D://class5"));
            job.waitForCompletion(true);

        }
    }
}
