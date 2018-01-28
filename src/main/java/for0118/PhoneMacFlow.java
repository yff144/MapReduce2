package for0118;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PhoneMacFlow implements Writable{
    private String mac;
    private int flow;
    public void write(DataOutput out) throws IOException {
        out.writeUTF(mac);
        out.writeInt(flow);
    }

    public void readFields(DataInput in) throws IOException {
        this.mac=in.readUTF();
        this.flow=in.readInt();
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public void setFlow(int flow) {
        this.flow = flow;
    }

    public String getMac() {

        return mac;
    }

    public int getFlow() {
        return flow;
    }

    @Override
    public String toString() {
        return mac + "\t" +flow ;
    }
}
