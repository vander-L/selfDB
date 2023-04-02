package org.yujiabin.selfDB.transport;

import org.junit.Test;
import org.yujiabin.selfDB.CS.transport.Encoder;
import org.yujiabin.selfDB.CS.transport.Package;
import org.yujiabin.selfDB.CS.transport.Packager;
import org.yujiabin.selfDB.CS.transport.Transporter;
import org.yujiabin.selfDB.utils.Panic;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

public class PackagerTest {
    @Test
    public void testPackager() throws Exception {
        new Thread(() -> {
            try {
                ServerSocket ss = new ServerSocket(9998);
                Socket socket = ss.accept();
                Transporter t = new Transporter(socket);
                Encoder e = new Encoder();
                Packager p = new Packager(t, e);
                Package one = p.receive();
                assert "pkg1 test".equals(new String(one.getData()));
                Package two = p.receive();
                assert "pkg2 test".equals(new String(two.getData()));
                p.send(new Package("pkg3 test".getBytes(), null));
                ss.close();
            } catch (Exception e) {
                Panic.panic(e);
            }
        }).start();
        Socket socket = new Socket("localhost", 9998);
        Transporter t = new Transporter(socket);
        Encoder e = new Encoder();
        Packager p = new Packager(t, e);
        p.send(new Package("pkg1 test".getBytes(), null));
        p.send(new Package("pkg2 test".getBytes(), null));
        Package three = p.receive();
        assert "pkg3 test".equals(new String(three.getData()));
    }
}
