package me.suhyuk.yarn.netcat;

public class NetCat {

    public static void main(String[] args) {
        NetCatClient helloWorldClient = null;
        try {
            String workDir = ".";
            if (args.length > 0) workDir = args[0];
            helloWorldClient = new NetCatClient(workDir);
            helloWorldClient.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
