package me.suhyuk.yarn.netcat.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class AppMaster {

    private static final Logger LOG = Logger.getLogger(AppMaster.class);
    private static final Map<String, String> envs = System.getenv();
    private static final String CONTAINER_ID = Environment.CONTAINER_ID.name();

    // application related
    private Configuration conf;
    private ContainerId containerId;
    private ApplicationAttemptId attemptId;
    private ApplicationId applicationId;
    private int success = 0;
    private int failure = 0;

    // resource request
    private static final int appMemory = 16;
    private static final int appCores = 1;
    private static final int appPriority = 0;
    private static int totalContainers = 3;

    // user related
    private UserGroupInformation appSubmitterUgi;

    // amrm, nm client
    AMRMClient<AMRMClient.ContainerRequest> amrmClient;
    private String appMasterHostname;
    private int appMasterHostPort = -1;
    private String appMasterTrackingUrl = "";
    private Map<String, Resource> resourceProfiles;
    private FinalApplicationStatus appStatus;
    private String message;

    public AppMaster() {
        conf = new YarnConfiguration();
    }

    public static String getRelativePath(String appName, String appId, String fileDstPath) {
        return appName + "/" + appId + "/" + fileDstPath;
    }

    private String getLocalHostName() {
        String hostname = "";
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "Unknown";
        }
        return hostname;
    }

    public void init(String[] args) {
        checkEnvironments();
        containerId = ContainerId.fromString(envs.get(CONTAINER_ID));
        attemptId = containerId.getApplicationAttemptId();
        applicationId = attemptId.getApplicationId();
        appMasterHostname = getLocalHostName();
        LOG.info("Application master for app" + ", appId="
                + applicationId.toString() + ", clusterTimestamp="
                + attemptId.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + attemptId.getAttemptId());
    }

    private void checkEnvironments() {
        if (!envs.containsKey(CONTAINER_ID))
            throw new IllegalArgumentException(CONTAINER_ID + " not set in the environment");
        if (!envs.containsKey(ApplicationConstants.APP_SUBMIT_TIME_ENV))
            throw new RuntimeException(ApplicationConstants.APP_SUBMIT_TIME_ENV + " not set in the environment");
        if (!envs.containsKey(Environment.NM_HOST.name()))
            throw new RuntimeException(Environment.NM_HOST.name() + " not set in the environment");
        if (!envs.containsKey(Environment.NM_HTTP_PORT.name()))
            throw new RuntimeException(Environment.NM_HTTP_PORT + " not set in the environment");
        if (!envs.containsKey(Environment.NM_PORT.name()))
            throw new RuntimeException(Environment.NM_PORT.name() + " not set in the environment");
    }

    public void run() throws IOException, YarnException {
        appStatus = FinalApplicationStatus.UNDEFINED;
    }

    public void finish() {
        try {
            if (failure == 0) {
                appStatus = FinalApplicationStatus.SUCCEEDED;
                LOG.info(String.format("Succeeded with all success job %d", success));
            } else if (success > 0 && failure > 0) {
                appStatus = FinalApplicationStatus.ENDED; // application which has subtasks with multiple states
                LOG.error(String.format("Failed with failed job %d, success job %d", failure, success));
            } else {
                appStatus = FinalApplicationStatus.FAILED;
                LOG.error(String.format("Failed with failed all job %d", failure));
            }
            amrmClient.unregisterApplicationMaster(appStatus, message, null);
        } catch (YarnException | IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        amrmClient.stop();
        LOG.info("Hadoop Yarn Application has stopped");
    }

    public void cleanup() {
        LOG.info("Hadoop Yarn Application has cleaned up");
    }

    public static void main(String[] args) {
        AppMaster appMaster = null;
        try {
            appMaster = new AppMaster();
            appMaster.init(args);
            LOG.info("Hadoop Yarn Application initialized");

            appMaster.run();
            appMaster.finish();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (appMaster != null) {
                appMaster.cleanup();
            }
        }
    }
}
