package me.suhyuk.yarn.netcat;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class NetCatAppMaster {

    private static final Logger LOG = Logger.getLogger(NetCatAppMaster.class);
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
    AMRMClient<AMRMClient.ContainerRequest> amrmClient;
    private String appMasterHostname;
    private int appMasterHostPort = -1;
    private String appMasterTrackingUrl = "";
    private Map<String, Resource> resourceProfiles;
    private FinalApplicationStatus appStatus;
    private String message;

    public NetCatAppMaster() {
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

        amrmClient = AMRMClient.createAMRMClient();
        amrmClient.init(conf);
        amrmClient.start();
        LOG.info("appMaster amrmClient has started");

        RegisterApplicationMasterResponse amResponse =
                amrmClient.registerApplicationMaster(appMasterHostname, 0, appMasterTrackingUrl);
        resourceProfiles = amResponse.getResourceProfiles();
        ResourceUtils.reinitializeResources(amResponse.getResourceTypes());
        LOG.info("appMaster reinitialized resources");

        // TODO: application 통해서 `nc -zvw10 datanode 9862` 명령어 수행
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(16);
        capability.setVirtualCores(1);

        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        amrmClient.addContainerRequest(containerAsk);

        int success = 0;
        int failure = 0;
        int totalContainers = 10;
        int waitingContainers = totalContainers; // total-num-of-containers = 2
        int increment = 0;
        float progressIndicator;
        while (waitingContainers > 0) {
            float completedRatio = (float) (success + failure) / (float) totalContainers;
            float incrementValue = Float.MIN_VALUE * increment++;
            progressIndicator = completedRatio + incrementValue;
            AllocateResponse allocated = amrmClient.allocate(progressIndicator); // 왜 이런식으로 업데이트 해주는가?

            // 할당 받은 컨테이너 실행
            for (Container container : allocated.getAllocatedContainers()) { // 현재 시점에 할당된 컨테이너 전체
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                    Collections.singletonList(
                        String.format(
                            "/bin/nc -zvw1 datanode 9862 1>%s/stdout 2>%s/stderr",
                            NetCatAppMaster.class.getName(),
                            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                            ApplicationConstants.LOG_DIR_EXPANSION_VAR
                        )
                    )
                );
                nmClient.startContainer(container, ctx);
                waitingContainers -= 1;
            }

            // 실행 중인 컨테이너의 상태 확인
            for (ContainerStatus status : allocated.getCompletedContainersStatuses()) {
                if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
                    success += 1;
                } else {
                    failure += 1;
                }
            }

            // 잠시 대기
            try {
                LOG.info("Application is Running... ");
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                appStatus = FinalApplicationStatus.FAILED;
                message = "Application failed with interrupted exception " + e.getLocalizedMessage();
                e.printStackTrace();
            }
        }
        appStatus = FinalApplicationStatus.ENDED;
    }

    public void finish() {
        try {
            appStatus = FinalApplicationStatus.SUCCEEDED;
            amrmClient.unregisterApplicationMaster(appStatus, message, null);
        } catch (YarnException | IOException e) {
            LOG.error("Failed to unregister application", e);
        }
        amrmClient.stop();
        LOG.info("HelloWorldAppMaster has stopped");
    }

    public void cleanup() {
        LOG.info("HelloWorldAppMaster has cleaned up");
    }

    public static void main(String[] args) {
        NetCatAppMaster appMaster = null;
        try {
            appMaster = new NetCatAppMaster();
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
