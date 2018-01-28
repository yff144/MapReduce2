package for0119;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.util.BeanUtil;

import java.beans.beancontext.BeanContext;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ForReduceJoinMR {

    public static class ForMap extends Mapper<LongWritable,Text,Text,Inpo>{
        Text text=new Text();
        Inpo inpo=new Inpo();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String s=value.toString();
            String []strings=s.split("\t");
            if(strings.length<3){
                return ;
            }
            FileSplit fileSplit=(FileSplit)context.getInputSplit();
            String fileName=fileSplit.getPath().getName();
            if("order.txt".equals(fileName)){
                text.set(strings[3]);
                inpo.setFlag(true);
                inpo.setOrderNumber(Integer.parseInt(strings[0]));
                inpo.setOrderTime(strings[1]);
                inpo.setOrserId(Integer.parseInt(strings[3]));
            }
            else if("product.txt".equals(fileName)){
                text.set(strings[0]);
                inpo.setProductId(Integer.parseInt(strings[0]));
                inpo.setProductName(strings[1]);
                inpo.setProduceMoney(Integer.parseInt(strings[3]));
            }else{
                try {
                    throw  new Exception("无此文件。。。");
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            context.write(text,inpo);
        }
    }
    public static class ForReduce extends Reducer<Text,Inpo,Inpo,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Inpo> values, Context context) throws IOException, InterruptedException {
           Inpo product=new Inpo();
            List<Inpo> list=new ArrayList<Inpo>();
            for(Inpo i:values){
                if(i.isFlag()){
                    list.add(i);
                }else{
                    try {
                        BeanUtils.copyProperties(product,i);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    } catch (InvocationTargetException e) {
                        e.printStackTrace();
                    }
                }
            }
            for(Inpo l:list){
                l.setProductId(product.getProductId());
                l.setProduceMoney(product.getProduceMoney());
                l.setProductName(product.getProductName());
                context.write(l,NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job=Job.getInstance();
        //job.setCombinerClass(ForCombiner.class);
        job.setMapperClass(ForMap.class);
        job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Inpo.class);
        job.setOutputKeyClass(Inpo.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a"));
        FileOutputFormat.setOutputPath(job,new Path("D://class5"));
        job.waitForCompletion(true);
    }
}
