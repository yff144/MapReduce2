package for0118;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowOrder implements WritableComparable<FlowOrder> {
    private String phone="";
    private int up;
    private int down;
    private int all;

    @Override
    public String toString() {
        return "FlowOrder{" +
                "phone='" + phone + '\'' +
                ", up=" + up +
                ", down=" + down +
                ", all=" + all +
                '}';
    }

    public void setPhone(String phone) {
        this.phone = phone;
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

    public String getPhone() {

        return phone;
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

    public int compareTo(FlowOrder o) {
        if(this.all==o.all){
            return o.down-this.down;
        }
        return o.all-this.all;
    }

    public void write(DataOutput out) throws IOException {

        out.writeUTF(phone);
        out.writeInt(up);
        out.writeInt(down);
        out.writeInt(all);
    }

    public void readFields(DataInput in) throws IOException {
        this.phone=in.readUTF();
        this.up=in.readInt();
        this.down=in.readInt();
        this.all=in.readInt();
    }
}
