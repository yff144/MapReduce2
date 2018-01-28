import for0119.Phone;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

public class Test4 {
    public static class ForMap extends Mapper<LongWritable,Text,Phone,NullWritable>{
    Map<String,String> map=new HashMap<String, String>();

    Phone phone=new Phone();
    @Override
    protected void setup(Mapper.Context context) throws IOException, InterruptedException {
        URI uri=context.getCacheFiles()[0];
        BufferedReader bufferedReader=new BufferedReader(new FileReader(new File(uri)));
        String s;
        while ((s=bufferedReader.readLine())!=null){
            String [] strings=s.split("\t");
            map.put(strings[0],strings[1]);

        }
    }

    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String s=value.toString();
        String []str=s.split("\t");
        for(Map.Entry<String,String> m:map.entrySet()){
            if(m.getKey().equals(str[2])){

                phone.setBrand(m.getValue());
                phone.setId(Integer.parseInt(str[0]));
                phone.setMac(str[2]);
                phone.setUser(str[1]);
                System.out.println(phone);
            }

        }
        context.write(phone,NullWritable.get());
    }
}

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Job job = Job.getInstance();

        job.setMapperClass(ForMap.class);
        // job.setReducerClass(ForReduce.class);
        job.setMapOutputKeyClass(Phone.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.addCacheFile(new URI("file:///D:/a/phoneinfo.txt"));
        job.setOutputValueClass(Phone.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.setInputPaths(job, new Path("D://a//userinfo.txt"));
        FileOutputFormat.setOutputPath(job, new Path("D://class2"));
        job.waitForCompletion(true);
    }
}
