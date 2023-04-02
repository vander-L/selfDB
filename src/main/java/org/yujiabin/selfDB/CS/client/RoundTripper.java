package org.yujiabin.selfDB.CS.client;

import org.yujiabin.selfDB.CS.transport.Package;
import org.yujiabin.selfDB.CS.transport.Packager;

public class RoundTripper {
    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    /**
     * 发送数据，并对结果进行接收
     */
    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }
}
