package me.suhyuk.yarn.helloworld;

import me.suhyuk.yarn.helloworld.client.HelloWorldClient;

public class HelloWorld {

    public static void printUsage() {
        System.out.println("HelloWorld [workDir] [version]");
        System.out.println("HelloWorld . v1");
        System.out.println("HelloWorld . v2");
    }

    public static void main(String[] args) {
        HelloWorldClient helloWorldClient;
        try {
            if (args.length != 2) printUsage();

            String workDir = args[0];
            String version = args[1];
            helloWorldClient = new HelloWorldClient(workDir, version);
            helloWorldClient.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
