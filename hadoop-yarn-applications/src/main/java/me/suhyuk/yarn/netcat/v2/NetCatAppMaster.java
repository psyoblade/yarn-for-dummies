package me.suhyuk.yarn.netcat.v2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.yarn.util.ConverterUtils;
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
    private static final String appName = "hadoop-yarn-applications";
    private static final Map<String, String> envs = System.getenv();
    private static final String CONTAINER_ID = Environment.CONTAINER_ID.name();
    private static final String userName = "root"; // live:gfis, dev:root
    private static final String password = "null"; // live:dpsTlqlrqmfh2403, dev: null
    private static final String port = "8041"; // live:22, dev:8041

    // application related
    private Configuration conf;
    private ContainerId containerId;
    private ApplicationAttemptId attemptId;
    private ApplicationId applicationId;
    private int success = 0;
    private int failure = 0;

    // resource request
    private static String appClassName = "me.suhyuk.yarn.netcat.v2.NetCatApplication";
    private static final int appMemory = 16;
    private static final float appMemRatio = 0.7f;
    private static final int appMaxDirect = 5;
    private static final int appCores = 1;
    private static final int appPriority = 0;
    private static int totalContainers = 5;

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

        for (String key : envs.keySet()) {
            LOG.info(String.format("AM:envs - %s:%s", key, envs.get(key)));
        }
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

    private String getJavaSshApplicationCommands(String username, String password, String hostname, String port) {
        Vector<CharSequence> vargs = new Vector<>(10);
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + (int) Math.ceil(appMemory * appMemRatio) + "m");
        vargs.add("-XX:MaxDirectMemorySize=" + appMaxDirect + "m");
        vargs.add(appClassName);
        vargs.add(username);
        vargs.add(password);
        vargs.add(hostname);
        vargs.add(port);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Application.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/Application.stderr");
        return String.join(" ", vargs);
    }

    private static LocalResource createLocalResource(Configuration conf, String resourcePath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(resourcePath);
        FileStatus fileStatus = fs.getFileStatus(hdfsPath);

        LocalResource localResource = Records.newRecord(LocalResource.class);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(hdfsPath));
        localResource.setSize(fileStatus.getLen());
        localResource.setTimestamp(fileStatus.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.APPLICATION);

        return localResource;
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

        // TODO: application `nc -zvw10 datanode 9862`
        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        int increment = 0;
        float progressIndicator = 0;
        String[] nodes = getAllNodeManagerHosts();
        totalContainers = nodes.length;
        int waitingContainers = totalContainers;

        int result = 0;
        // assign foreach container and execute job
        for (String hostname : getAllNodeManagerHosts()) {
            result = 0;
            increment = 0;
            assignSingleContainer(hostname);

            // while running single container
            while (result == 0) { // 0: ready, 1: success, 2: failure
                AllocateResponse allocated = amrmClient.allocate(increment);
                for (Container container : allocated.getAllocatedContainers()) { // all allocated containers at now
                    ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
                    String commands = getJavaSshApplicationCommands(userName, password, hostname, port);
                    context.setCommands(Collections.singletonList(commands));
                    context.setEnvironment(envs);
                    LOG.info(String.format("executing '%s'", commands));

                    // add client.jar to local resource
                    Map<String, LocalResource> localResources = new HashMap<>();
                    String resourcePath = String.format("/user/%s/hadoop-yarn-applications/%s/%s.jar", userName, applicationId, appName);
                    localResources.put("app.jar", createLocalResource(conf, resourcePath));
                    context.setLocalResources(localResources);

                    nmClient.startContainer(container, context);
                }

                // checking acquired containers and release
                for (ContainerStatus status : allocated.getCompletedContainersStatuses()) {
                    LOG.info(String.format("Container '%s' exit status is '%d'", status.getContainerId().toString(), status.getExitStatus()));
                    if (status.getExitStatus() == ContainerExitStatus.SUCCESS) {
                        success += 1;
                        result = 1;
                    } else {
                        failure += 1;
                        result = 2;
                    }
                    LOG.info(String.format("status of container '%s' - success : %d, failure: %d with exit status %d",
                            status.getContainerId().toString(), success, failure, status.getExitStatus()));

                    releaseUsedContainer(status.getContainerId());
                }

                // waiting 3 seconds
                try {
                    LOG.info("Application is Running... ");
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    appStatus = FinalApplicationStatus.FAILED;
                    message = "Application failed with interrupted exception " + e.getLocalizedMessage();
                    e.printStackTrace();
                }
                increment += 1;
            }

        }

    }

    private void releaseUsedContainer(ContainerId containerId) {
        amrmClient.releaseAssignedContainer(containerId);
        LOG.info(String.format("Acquired container %s has released.", containerId.toString()));
    }

    private void assignSingleContainer(String node) {
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemorySize(appMemory);
        capability.setVirtualCores(appCores);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(appPriority);

        String[] racks = null;
        String[] nodes = { node };
        boolean relaxLocality = false;
        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, nodes, racks, priority, relaxLocality);
        amrmClient.addContainerRequest(containerAsk);
        LOG.info(String.format("add container with node '%s'", containerAsk));
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

    private void assignForeachContainers(String[] nodes) {
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
