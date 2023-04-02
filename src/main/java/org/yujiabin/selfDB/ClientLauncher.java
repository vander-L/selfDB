package org.yujiabin.selfDB;

import org.yujiabin.selfDB.CS.client.Client;
import org.yujiabin.selfDB.CS.client.Shell;
import org.yujiabin.selfDB.CS.transport.Encoder;
import org.yujiabin.selfDB.CS.transport.Packager;
import org.yujiabin.selfDB.CS.transport.Transporter;

import java.io.IOException;
import java.net.Socket;

public class ClientLauncher {
    public static void main(String[] args) throws IOException {
        Socket socket = new Socket("localhost", 9999);
        Encoder e = new Encoder();
        Transporter t = new Transporter(socket);
        Packager packager = new Packager(t, e);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }
}
