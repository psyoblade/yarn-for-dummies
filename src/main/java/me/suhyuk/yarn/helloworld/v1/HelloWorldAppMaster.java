package me.suhyuk.yarn.helloworld.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HelloWorldAppMaster {

    private static final Logger LOG = Logger.getLogger(HelloWorldAppMaster.class);
    private static final Map<String, String> envs = System.getenv();
    private static final String CONTAINER_ID = Environment.CONTAINER_ID.name();

    // application related
    private Configuration conf;
    private ContainerId containerId;
    private ApplicationAttemptId attemptId;
    private ApplicationId applicationId;

    // user related
    private UserGroupInformation appSubmitterUgi;

    // amrm, nm client
    private AMRMClientAsync amrmClientAsync;
    private String appMasterHostname;
    private int appMasterHostPort = -1;
    private String appMasterTrackingUrl = "";
    private Map<String, Resource> resourceProfiles;
    private FinalApplicationStatus appStatus;
    private String message;

    public HelloWorldAppMaster() {
        conf = new YarnConfiguration();
    }

    public static String getRelativePath(String appName, String appId, String fileDstPath) {
        return appName + "/" + appId + "/" + fileDstPath;
    }

    public void init(String[] args) {
        checkEnvironments();
        containerId = ContainerId.fromString(envs.get(CONTAINER_ID));
        attemptId = containerId.getApplicationAttemptId();
        applicationId = attemptId.getApplicationId();
        LOG.info("Application master for app" + ", appId="
                + attemptId.getApplicationId().getId() + ", clusterTimestamp="
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

        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        String appSubmitterUserName = envs.get(Environment.USER.name());
        appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);
        LOG.info("User credentials is created");

        AMRMClientAsync.AbstractCallbackHandler allocationListener = newAMRMCallbackHandler();
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, allocationListener);
        amrmClientAsync.init(conf);
        amrmClientAsync.start();
        LOG.info("appMaster amrmClient has started");

        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response =
                amrmClientAsync.registerApplicationMaster(appMasterHostname, appMasterHostPort, appMasterTrackingUrl);
        resourceProfiles = response.getResourceProfiles();
        ResourceUtils.reinitializeResources(response.getResourceTypes());
        LOG.info("appMaster reinitialized resources");

        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                appStatus = FinalApplicationStatus.FAILED;
                message = "Application failed with interrupted exception " + e.getLocalizedMessage();
                e.printStackTrace();
            }
            LOG.info("Application '" + i + "' Job is Running... ");
        }
        appStatus = FinalApplicationStatus.ENDED;
    }

    private AMRMClientAsync.AbstractCallbackHandler newAMRMCallbackHandler() {

        return new AMRMClientAsync.AbstractCallbackHandler() {
            @Override
            public void onContainersCompleted(List<ContainerStatus> statuses) {

            }

            @Override
            public void onContainersAllocated(List<Container> containers) {

            }

            @Override
            public void onContainersUpdated(List<UpdatedContainer> containers) {

            }

            @Override
            public void onShutdownRequest() {

            }

            @Override
            public void onNodesUpdated(List<NodeReport> updatedNodes) {

            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public void onError(Throwable e) {

            }
        };
    }

    public void finish() {
        try {
            appStatus = FinalApplicationStatus.SUCCEEDED;
            amrmClientAsync.unregisterApplicationMaster(appStatus, message, null);
        } catch (YarnException | IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        amrmClientAsync.stop();
        LOG.info("HelloWorldAppMaster has stopped");
    }

    public void cleanup() {
        LOG.info("HelloWorldAppMaster has cleaned up");
    }

    public static void main(String[] args) {
        HelloWorldAppMaster appMaster = null;
        try {
            appMaster = new HelloWorldAppMaster();
            appMaster.init(args);
            LOG.info("HelloWorldAppMaster initialized");

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
