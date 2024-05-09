package me.suhyuk.yarn.helloworld.appmaster;

import lombok.extern.slf4j.Slf4j;
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

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * https://hadoop.apache.org/docs/r3.2.4/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
 * Yarn Application Async Version
 */
@Slf4j
public class HelloWorldAppMasterV2 extends AbstractAppMaster implements HelloWorldAppMaster {

    // amrm client
    private AMRMClientAsync amrmClientAsync;

    public HelloWorldAppMasterV2() {
        super();
    }

    @Override
    public void init() {
        HelloWorldAppMaster.checkEnvironments();
        containerId = ContainerId.fromString(envs.get(CONTAINER_ID));
        attemptId = containerId.getApplicationAttemptId();
        applicationId = attemptId.getApplicationId();
        log.info("Application master for app" + ", appId="
                + attemptId.getApplicationId().getId() + ", clusterTimestamp="
                + attemptId.getApplicationId().getClusterTimestamp()
                + ", attemptId=" + attemptId.getAttemptId());
    }

    @Override
    public void run() throws IOException, YarnException {
        appStatus = FinalApplicationStatus.UNDEFINED;

        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        String appSubmitterUserName = envs.get(Environment.USER.name());
        appSubmitterUgi = UserGroupInformation.createRemoteUser(appSubmitterUserName);
        appSubmitterUgi.addCredentials(credentials);
        log.info("User credentials is created");

        AMRMClientAsync.AbstractCallbackHandler allocationListener = newAMRMCallbackHandler();
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(1000, allocationListener);
        amrmClientAsync.init(conf);
        amrmClientAsync.start();
        log.info("appMaster amrmClient has started");

        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response =
                amrmClientAsync.registerApplicationMaster(appMasterHostname, appMasterHostPort, appMasterTrackingUrl);
        resourceProfiles = response.getResourceProfiles();
        ResourceUtils.reinitializeResources(response.getResourceTypes());
        log.info("appMaster reinitialized resources");

        for (int i = 0; i < 100; i++) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                appStatus = FinalApplicationStatus.FAILED;
                message = "Application failed with interrupted exception " + e.getLocalizedMessage();
                e.printStackTrace();
            }
            log.info("Application '" + i + "' Job is Running... ");
        }
        appStatus = FinalApplicationStatus.ENDED;
    }

    @Override
    public void finish() {
        try {
            appStatus = FinalApplicationStatus.SUCCEEDED;
            amrmClientAsync.unregisterApplicationMaster(appStatus, message, null);
        } catch (YarnException | IOException e) {
            log.error("Failed to unregister application", e);
        }
        amrmClientAsync.stop();
        log.info("HelloWorldAppMaster has stopped");
    }

    @Override
    public void cleanup() {
        log.info("HelloWorldAppMaster has cleaned up");
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

    public static void main(String[] args) {
        HelloWorldAppMasterV2 appMaster = null;
        try {
            appMaster = new HelloWorldAppMasterV2();
            appMaster.init();
            log.info("HelloWorldAppMaster initialized");

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
