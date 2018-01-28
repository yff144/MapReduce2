package for0117;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Flow implements Writable{
    private String phoneNumber;
    private int up;
    private int down;
    private int all;

    public Flow() {
    }

    public Flow(String phoneNumber, int up, int down, int all) {
        this.phoneNumber = phoneNumber;
        this.up = up;
        this.down = down;
        this.all = all;
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(phoneNumber);
        out.writeInt(up);
        out.writeInt(down);
        out.writeInt(all);
    }

    public void readFields(DataInput in) throws IOException {
        this.phoneNumber=in.readUTF();
        this.up=in.readInt();
        this.down=in.readInt();
        this.all=in.readInt();

    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public void setUp(int up) {
        this.up = up;
    }

    public void setDown(int down) {
        this.down = down;
    }

    public void setAll(int all) {
        this.all = all;
    }

    public String getPhoneNumber() {

        return phoneNumber;
    }

    public int getUp() {
        return up;
    }

    public int getDown() {
        return down;
    }

    public int getAll() {
        return all;
    }

    @Override
    public String toString() {
        return "Flow{" +
                "phoneNumber='" + phoneNumber + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", all=" + all +
                '}';
    }
}
