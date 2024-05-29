package me.suhyuk.yarn.netcat.v2;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import java.util.Properties;

public class NetCatApplication {
    public static void main(String[] args) {
        System.out.println("starting application");
        try {
            String username = args[0];
            String password = args[1];
            String hostname = args[2];
            int port = Integer.parseInt(args[3]);

            JSch jsch = new JSch();
            System.out.println(String.format("creating session at %s/%s@%s:%d", username, password, hostname, port));
            Session session = jsch.getSession(username, hostname, port);
            if (!"null".equalsIgnoreCase(password))
                session.setPassword(password);

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
