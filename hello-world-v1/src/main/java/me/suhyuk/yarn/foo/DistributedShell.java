package me.suhyuk.yarn.foo;

import me.suhyuk.yarn.foo.client.DistributedShellClient;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

public class DistributedShell {

    public static void main(String[] args) throws IOException, YarnException, InterruptedException {
        DistributedShellClient hello = new DistributedShellClient();
        hello.init();
        hello.checkClusterResources();
        hello.createSubmissionContext();
        hello.uploadShellScriptToHdfs();
        hello.uploadLocalResources();
        hello.setupApplicationClassPath();
        hello.setupApplicationMasterCommands();
        hello.createContainerLaunchContext();
        if (UserGroupInformation.isSecurityEnabled()) hello.createCredentials();
        hello.submitApplication();
        hello.waitUntilFinished(100);
        hello.killApplication();
    }
}
