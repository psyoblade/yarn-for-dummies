package me.suhyuk.yarn.sshtools.v1;

import com.sshtools.client.SshClient;
import com.sshtools.common.ssh.SshException;

import java.io.IOException;

public class SshToolsApplication {

    public static void main(String[] args) {

        String sourceIpList = "172.19.217.92,172.19.217.90"; // args[0]; // 172.19.217.92,172.19.217.90
        String targetIpList = "172.19.217.92,172.19.217.90"; // args[1]; // 172.19.217.92,172.19.217.90
        String targetPort = "22"; // args[2];   // 22

//        String sourceHost = ""; // connect-hadoop-op-001";
        int sourcePort = 22;
//        String targetHost = "connect-hadoop-op-002";
//        int targetPort = 22;
        int timeout = 10;

        for (String sourceHost : sourceIpList.split(",")) {
            System.out.println(String.format("Connecting %s", sourceHost));
            connectTargetHostAndPort(sourcePort, targetIpList, targetPort, timeout, sourceHost);
        }
    }

    private static void connectTargetHostAndPort(int sourcePort, String targetIpList, String targetPort, int timeout, String sourceHost) {

        try (SshClient ssh = SshClient.SshClientBuilder.create()
                .withHostname(sourceHost)
                .withPort(sourcePort)
                .withUsername("gfis")
                .withPassword("****")
                .build()
        ) {
//            ssh.putFile(new File("package.deb"));
//            ssh.executeCommand("dpkg -i package.deb");
            for (String targetHost : targetIpList.split(",")) {
                String result = ssh.executeCommand(checkRemotePortOpenCommand(targetHost, targetPort, timeout));
                System.out.println(String.format("%s -> %s = %s", sourceHost, targetHost, result));
            }

        } catch (IOException | SshException e) {
            e.printStackTrace();
        }
    }

    private static String checkRemotePortOpenCommand(String host, String port, int timeout) {
        return String.format("/bin/nc -zvw%d %s %s", timeout, host, port);
    }
}
