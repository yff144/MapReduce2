package for0119;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Phone implements WritableComparable<Phone> {
    private int id;
    private String user="";
    private String mac="";
    private String brand="";

    public Phone() {
    }

    public Phone(int id, String user, String mac,String brand) {
        this.id = id;
        this.user = user;
        this.mac = mac;
        this.brand=brand;
    }

    @Override
    public String toString() {
        return "Phone{" +
                "id=" + id +
                ", user='" + user + '\'' +
                ", mac='" + mac + '\'' +
                ", brand='" + brand + '\'' +
                '}';
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setMac(String mac) {
        this.mac = mac;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public int getId() {

        return id;
    }

    public String getUser() {
        return user;
    }

    public String getMac() {
        return mac;
    }

    public String getBrand() {
        return brand;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(user);
        out.writeUTF(mac);
        out.writeUTF(brand);
    }

    public void readFields(DataInput in) throws IOException {
        this.id=in.readInt();
        this.user=in.readUTF();
        this.mac=in.readUTF();
        this.brand=in.readUTF();
    }

    public int compareTo(Phone o) {
        return 0;
    }
}
