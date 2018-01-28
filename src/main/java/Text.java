import org.apache.hadoop.fs.FileUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Text {


    public static void main(String[] args) {
        String s="hfdjahkjfhd  dhjfakj 还FAU俄护法尽快恢复";
        Pattern pattern=Pattern.compile("[\\u2E80-\\u9FFF]");
        Matcher matcher=pattern.matcher(s);
        while (matcher.find()){
            System.out.println(matcher.group());
        }
    }
}
