package me.suhyuk.yarn.helloworld.v1;

public class HelloWorld {

    public static void main(String[] args) {
        HelloWorldClient helloWorldClient = null;
        try {
            String workDir = ".";
            if (args.length > 0) workDir = args[0];
            helloWorldClient = new HelloWorldClient(workDir);
            helloWorldClient.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
