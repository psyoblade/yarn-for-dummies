package me.suhyuk.yarn.helloworld.appmaster;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Map;

public interface HelloWorldAppMaster {

    Map<String, String> envs = System.getenv();
    String CONTAINER_ID = ApplicationConstants.Environment.CONTAINER_ID.name();

    static String getRelativePath(String appName, String appId, String fileDstPath) {
        return appName + "/" + appId + "/" + fileDstPath;
    }

    static void checkEnvironments() {
        if (!envs.containsKey(CONTAINER_ID))
            throw new IllegalArgumentException(CONTAINER_ID + " not set in the environment");
        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV))
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HOST.name()))
            throw new RuntimeException(ApplicationConstants.Environment.NM_HOST.name() + " not set in the environment");
        if (!envs.containsKey(ApplicationConstants.Environment.NM_HTTP_PORT.name()))
            throw new RuntimeException(ApplicationConstants.Environment.NM_HTTP_PORT + " not set in the environment");
        if (!envs.containsKey(ApplicationConstants.Environment.NM_PORT.name()))
            throw new RuntimeException(ApplicationConstants.Environment.NM_PORT.name() + " not set in the environment");
    }

    void init();
    void run() throws IOException, YarnException, InterruptedException;
    void finish();
    void cleanup();
}
