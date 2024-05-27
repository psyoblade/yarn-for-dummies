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

public class NetCatAppMaster {

    private static final Logger LOG = Logger.getLogger(NetCatAppMaster.class);
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

    public NetCatAppMaster() {
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

        int waitingContainers = totalContainers;
        int increment = 0;
        float progressIndicator = 0;
        String[] nodes = getAllNodeManagerHosts();
        int sizeOfNodes = nodes.length;

//        assignRandomContainers();
        assignForeachContainers(nodes);

        int port = 80;
        while ((success + failure) < totalContainers) {
            float completedRatio = (float) (success + failure) / (float) totalContainers;
            float incrementValue = Float.MIN_VALUE * increment++;
            progressIndicator = completedRatio + incrementValue;
            LOG.info(String.format("progress indicator %f, waiting containers %d", progressIndicator, waitingContainers));
            AllocateResponse allocated = amrmClient.allocate(progressIndicator); // 왜 이런식으로 업데이트 해주는가?

            // 할당 받은 컨테이너 실행
            for (Container container : allocated.getAllocatedContainers()) { // 현재 시점에 할당된 컨테이너 전체
                waitingContainers -= 1; // 이 인덱스가 nodes[waitingContainers] 접근하여 노드 별 접근이 가능하다
                String netCat = "/bin/nc -zvw10 namenode %d 1>%s/stdout 2>%s/stderr";
                String hostname = nodes[waitingContainers];
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);

                // TODO: 모든 노드에서 특정 스크립트(check all target databases)를 수행
                // TODO: 임의의 스크립트 결과가 현재 executor 로그로 출력이 되는지 확인이 필요함
                String remoteNetCat = "ssh gfis@%s \"/bin/nc -zvw10 big-ingest-rc-001 %d\" 1>%s/stdout 2>%s/stderr";
                String commands = String.format(
                        remoteNetCat,
                        hostname,
                        port,
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                        ApplicationConstants.LOG_DIR_EXPANSION_VAR
                );
                ctx.setCommands(Collections.singletonList(commands));
                LOG.info(String.format("executing '%s'", commands));
                nmClient.startContainer(container, ctx);
//                port += 1; // for intentional error
            }

            // 실행 중인 컨테이너의 상태 확인
            for (ContainerStatus status : allocated.getCompletedContainersStatuses()) {
                if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
                    success += 1;
                } else {
                    failure += 1;
                }
                LOG.info(String.format("status of container '%s' - success : %d, failure: %d with exit status %d",
                        status.getContainerId().toString(), success, failure, status.getExitStatus()));
            }

            // 잠시 대기
            try {
                LOG.info("Application is Running... ");
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                appStatus = FinalApplicationStatus.FAILED;
                message = "Application failed with interrupted exception " + e.getLocalizedMessage();
                e.printStackTrace();
            }
        }
    }

    private ResourceRequest getNodeRequest(String resourceName) {
        ResourceRequest nodeRequest = Records.newRecord(ResourceRequest.class);
        nodeRequest.setResourceName(resourceName);
        nodeRequest.setCapability(Resource.newInstance(appMemory, appCores));
        nodeRequest.setPriority(Priority.newInstance(appPriority));
        nodeRequest.setNumContainers(1);
        return nodeRequest;
    }

    private String[] getAllNodeManagerHosts() throws IOException, YarnException {
        try (YarnClient yarnClient = YarnClient.createYarnClient()){
            yarnClient.init(conf);
            yarnClient.start();
            List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
            String[] hosts = nodeReports.stream().map(node -> node.getNodeId().getHost()).toArray(String[]::new);
            for (String host : hosts) {
                System.out.println("host = " + host);
                LOG.info(String.format("host '%s' has found", host));
            }
            yarnClient.stop();
            return hosts;
        }
    }

    // TODO: 모든 node 정보를 추가하더라도 임의의 노드에 모든 작업이 기동된다 node 정보는 수행 가능한 노드를 지정하는 역할처럼 보인다
    // TODO: 도커 환경에서 nodemanager 포트가 달라서 디버깅이 어렵다
    // TODO: 노드에 하나씩만 익스큐터 띄우는 게 어려워서 노드 수 만큼 ssh 통해서 netcat 하는 방식 테스트
    private void assignForeachContainers(String[] nodes) throws IOException, YarnException {
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(appMemory);
        capability.setVirtualCores(appCores);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(appPriority);

        String[] racks = null;
        boolean relaxLocality = false;
        for (int i = 0; i < totalContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, nodes, racks, priority, relaxLocality);
            amrmClient.addContainerRequest(containerAsk);
            LOG.info(String.format("add container with node '%s'", containerAsk));
        }
    }

    private void assignRandomContainers() {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(appPriority);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(appMemory);
        capability.setVirtualCores(appCores);

        for (int i = 0; i < totalContainers; i++) {
            AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
            amrmClient.addContainerRequest(containerAsk);
            LOG.info(String.format("container requested '%d'", i));
        }
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
        NetCatAppMaster appMaster = null;
        try {
            appMaster = new NetCatAppMaster();
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
