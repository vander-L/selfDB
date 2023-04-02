package org.yujiabin.selfDB.CS.transport;

/**
 * [Flag][data]<br>
 * 如果flag为0，则代表发送的是数据；为1，则为异常Exception
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}
