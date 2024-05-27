package me.suhyuk.yarn.netcat.v1;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import java.util.Properties;
public class NetCatApplication {
    public static void main(String[] args) {
        System.out.println("starting application");
        try {
            JSch jsch = new JSch();
            System.out.println("creating session " + args[0]);
            Session session = jsch.getSession("gfis", args[0], 22);
            session.setPassword("dpsTlqlrqmfh2403");
            // Configure to automatically accept host key
            Properties config = new Properties();
            config.put("StrictHostKeyChecking", "no");
            session.setConfig(config);
            session.connect();
            session.disconnect();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(99);
        }
        System.out.println("closing application");
        System.exit(0);
    }
}
