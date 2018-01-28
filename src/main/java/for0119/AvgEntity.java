package for0119;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgEntity implements Writable {
    private int count;
    private int hot;

    @Override
    public String toString() {
        return "AvgEntity{" +
                "time='" + count + '\'' +
                ", hot=" + hot +
                '}';
    }

    public void setCount(int count) {
        this.count = count;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    public int getCount() {

        return count;
    }

    public int getHot() {
        return hot;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(count);
        out.writeInt(hot);
    }

    public void readFields(DataInput in) throws IOException {
        this.count=in.readInt();
        this.hot=in.readInt();
    }
}
