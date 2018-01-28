package for0118;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowOrderMR {
    public static class ForMap extends Mapper<LongWritable,Text,Text,FlowOrder>{
        Text text=new Text();
        FlowOrder flowOrder=new FlowOrder();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            String []strings=s.split("\t");
            text.set(strings[1]);
            flowOrder.setUp(Integer.parseInt(strings[8]));
            flowOrder.setDown(Integer.parseInt(strings[9]));
            flowOrder.setPhone(strings[1]);
            context.write(text,flowOrder);
        }
    }
    public static class ForReduce extends Reducer<Text,FlowOrder,FlowOrder,NullWritable> {
        FlowOrder flowOrder=new FlowOrder();
        @Override
        protected void reduce(Text key, Iterable<FlowOrder> values, Context context) throws IOException, InterruptedException {
            int upsum=0;
            int downsum=0;
            for(FlowOrder i:values){
                upsum+=i.getUp();
                downsum+=i.getDown();
            }
            int allsum=upsum+downsum;
            flowOrder.setUp(upsum);
            flowOrder.setDown(downsum);
            flowOrder.setAll(allsum);
            flowOrder.setPhone(key.toString());
            context.write(flowOrder,NullWritable.get());

        }
    }
    private static class My extends Partitioner<Text,FlowOrder>{

        public int getPartition(Text text, FlowOrder flowOrder, int i) {
            String s=text.toString();
            String str=s.substring(0,3);
            if(str.equals("137")){
                return 0;
            }
            else if(str.equals("135")){
                return 1;
            }
            else{
                return 2;
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowOrder.class);
        job.setOutputKeyClass(FlowOrder.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(My.class);
        job.setNumReduceTasks(3);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class6"));
        job.waitForCompletion(true);
    }
}
