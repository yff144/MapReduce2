package for0117;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordTimes implements WritableComparable<WordTimes>{
    private String word="";
    private  int time;

    public WordTimes() {
    }

    public WordTimes(String word, int time) {
        this.word = word;
        this.time = time;
    }

    public int compareTo(WordTimes o) {
        return o.time-this.time;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(word);
        out.writeInt(time);
    }

    @Override
    public String toString() {
        return "WordTimes{" +
                "word='" + word + '\'' +
                ", time=" + time +
                '}';
    }

    public void readFields(DataInput in) throws IOException {

        this.word=in.readUTF();
        this.time=in.readInt();
    }
}
