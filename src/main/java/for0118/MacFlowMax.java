package for0118;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MacFlowMax {
    public static class ForMap extends Mapper<LongWritable,Text,Text,PhoneMacFlow> {
        Text text=new Text();
        PhoneMacFlow phoneMacFlow=new PhoneMacFlow();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            String []strings=s.split("\t");
            text.set(strings[1]);
            phoneMacFlow.setMac(strings[2]);
            phoneMacFlow.setFlow(Integer.parseInt(strings[8])+Integer.parseInt(strings[9]));
            context.write(text,phoneMacFlow);
        }
    }
    public static class ForReduce extends Reducer<Text,PhoneMacFlow,Text,PhoneMacFlow> {
        Text text=new Text();
        PhoneMacFlow phoneMacFlow=new PhoneMacFlow();


        @Override
        protected void reduce(Text key, Iterable<PhoneMacFlow> values, Context context) throws IOException, InterruptedException {
            Map<String,Integer> map=new HashMap<String, Integer>();
            int max=0;
            for(PhoneMacFlow p:values){
                String mac=p.getMac();
                int flow=p.getFlow();
                if(map.containsKey(mac)){
                    map.put(mac,map.get(mac)+flow);
                }else{
                    map.put(mac,flow);
                }
            }
            text.set(key);
            Set<Map.Entry<String ,Integer>> set=map.entrySet();
            for(Map.Entry<String ,Integer> s:set){
                if(max<s.getValue()) {
//                    PhoneMacFlow phoneMacFlow = new PhoneMacFlow();
//                    phoneMacFlow.setFlow(s.getValue());
//                    phoneMacFlow.setMac(s.getKey());
//                    context.write(text, phoneMacFlow);
                    max=s.getValue();
                    phoneMacFlow.setMac(s.getKey());
                }
            }
            phoneMacFlow.setFlow(max);
            context.write(text,phoneMacFlow);

        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job= Job.getInstance();
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneMacFlow.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneMacFlow.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class9"));
        job.waitForCompletion(true);
    }
}
