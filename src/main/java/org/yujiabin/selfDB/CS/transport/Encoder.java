package org.yujiabin.selfDB.CS.transport;

import com.google.common.primitives.Bytes;
import org.yujiabin.selfDB.exception.InvalidPkgDataException;

import java.util.Arrays;

public class Encoder {

    /**
     * 对Package编码成二进制格式用于数据传输
     */
    public byte[] encode(Package pkg) {
        if(pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Internet server error!";
            if(err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    /**
     * 对二进制数据解码成Package对象
     */
    public Package decode(byte[] data) {
        if(data.length < 1) {
            throw new InvalidPkgDataException();
        }
        if(data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if(data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw new InvalidPkgDataException();
        }
    }

}
