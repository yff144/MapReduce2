package for0119;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Inpo implements Writable {
    private int orderNumber;
    private String  orderTime="";
    private int orserId;
    //===========================
    private int productId;
    private String productName="";
    private int produceMoney;
    private boolean flag;

    public void write(DataOutput out) throws IOException {
        out.writeInt(orderNumber);
        out.writeUTF(orderTime);
        out.writeInt(orserId);
        out.writeInt(productId);
        out.writeUTF(productName);
        out.writeInt(produceMoney);
        out.writeBoolean(flag);
    }

    public void readFields(DataInput in) throws IOException {
        this.orderNumber=in.readInt();
        this.orderTime=in.readUTF();
        this.orserId=in.readInt();
        this.productId=in.readInt();
        this.productName=in.readUTF();
        this.produceMoney=in.readInt();
        this.flag=in.readBoolean();
    }

    @Override
    public String toString() {
        return "Inpo{" +
                "orderNumber=" + orderNumber +
                ", orderTime='" + orderTime + '\'' +
                ", orserId=" + orserId +
                ", productId=" + productId +
                ", productName='" + productName + '\'' +
                ", produceMoney=" + produceMoney +
                ", flag=" + flag +
                '}';
    }

    public void setOrderNumber(int orderNumber) {
        this.orderNumber = orderNumber;
    }

    public void setOrderTime(String orderTime) {
        this.orderTime = orderTime;
    }

    public void setOrserId(int orserId) {
        this.orserId = orserId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public void setProduceMoney(int produceMoney) {
        this.produceMoney = produceMoney;
    }

    public void setFlag(boolean flag) {
        this.flag = flag;
    }

    public int getOrderNumber() {

        return orderNumber;
    }

    public String getOrderTime() {
        return orderTime;
    }

    public int getOrserId() {
        return orserId;
    }

    public int getProductId() {
        return productId;
    }

    public String getProductName() {
        return productName;
    }

    public int getProduceMoney() {
        return produceMoney;
    }

    public boolean isFlag() {
        return flag;
    }
}
