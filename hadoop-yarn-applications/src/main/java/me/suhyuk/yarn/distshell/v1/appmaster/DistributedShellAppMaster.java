package me.suhyuk.yarn.distshell.v1.appmaster;

import me.suhyuk.yarn.distshell.v1.application.LaunchContainerRunnable;
import me.suhyuk.yarn.distshell.v1.common.DSConstants;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.log4j.Logger;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class DistributedShellAppMaster {

    private static Logger LOG = Logger.getLogger(DistributedShellAppMaster.class);

    private static final String AM_CONTAINER_ID_ENV = "AM_CONTAINER_ID";
    private static final int amrmHeartbeatMillis = 1000;

    private static ContainerId containerId;

    // assigned or completed num of containers
    private AtomicInteger numAllocatedContainers = new AtomicInteger();
    private AtomicInteger numCompletedContainers = new AtomicInteger();

    private static String appMasterHostname = "";
    private static int appMasterRpcPort = 0;
    private static String appMasterTrackingUrl = "";

    private static int numTotalContainers = 1;
    private static long containerMemory = 10;
    private static int containerVCores = 1;

    private AMRMClientAsync.AbstractCallbackHandler allocationListener;
    private NMClientAsync.AbstractCallbackHandler containerListener;
    private List<Thread> launchThreads = new ArrayList<>();

    private ApplicationAttemptId appAttemptId;
    private Configuration conf;
    private AMRMClientAsync amrmClientAsync;
    private NMClientAsync nmClientAsync;

    // shell file path
    private static final String shellCommandPath = "shellCommands";
    private static final String shellArgsPath = "shellArgs";
    private long shellScriptPathTimestamp = 0;
    private long shellScriptPathLen = 0;

    // shell
    private String shellCommand = "";
    private String shellArgs = "";
    private String scriptPath = "";
    private Map<String, String> shellEnv = new HashMap<>();

    public static void main(String[] args) throws IOException, YarnException, ParseException {
        DistributedShellAppMaster appMaster = new DistributedShellAppMaster();
        appMaster.init(args);
        appMaster.startAMRMClient();
        appMaster.startNMClient();
        appMaster.registerAppMaster();
    }

    /**
     * parsing parameters and initialize
     *
     * @param args
     * @throws IOException
     */
    private void init(String[] args) throws IOException, ParseException {
        Options opts = new Options();
        opts.addOption("shell_env", true,
                "Environment for shell script. Specified as env_key=env_val pairs");
        opts.addOption("container_memory", true,
                "Amount of memory in MB to be requested to run the shell command");
        opts.addOption("container_vcores", true,
                "Amount of virtual cores to be requested to run the shell command");
        opts.addOption("container_resources", true,
                "Amount of resources to be requested to run the shell command. " +
                        "Specified as resource type=value pairs separated by commas. " +
                        "E.g. -container_resources memory-mb=512,vcores=1");
        opts.addOption("num_containers", true,
                "No. of containers on which the shell command needs to be executed");
        opts.addOption("priority", true, "Application Priority. Default 0");
        opts.addOption("container_retry_policy", true,
                "Retry policy when container fails to run, "
                        + "0: NEVER_RETRY, 1: RETRY_ON_ALL_ERRORS, "
                        + "2: RETRY_ON_SPECIFIC_ERROR_CODES");
        opts.addOption("localized_files", true, "List of localized files");
        opts.addOption("homedir", true, "Home Directory of Job Owner");

        opts.addOption("help", false, "Print usage");
        CommandLine cliParser = new GnuParser().parse(opts, args);

        Map<String, String> envs = System.getenv();
        String containerIdName = ApplicationConstants.Environment.CONTAINER_ID.name();
        if (envs.containsKey(containerIdName)) {
            containerId = ContainerId.fromString(containerIdName);
        } else {
            throw new IllegalArgumentException("ContainerId not set in th environment");
        }
        appAttemptId = containerId.getApplicationAttemptId();
        conf = new Configuration();

        // read shell commands on hdfs
        if (!fileExists(shellCommandPath) && envs.get(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LOCATION).isEmpty()) {
            throw new IllegalArgumentException("No shell scripts for running on app-master using environment");
        }

        // shell commands and arguments
        if (fileExists(shellCommandPath)) shellCommand = readContents(shellCommandPath);
        if (fileExists(shellArgsPath)) shellArgs = readContents(shellArgsPath);

        if (cliParser.hasOption("shell_env")) {
            String shellEnvs[] = cliParser.getOptionValues("shell_env");
            for (String env : shellEnvs) {
                env = env.trim();
                int index = env.indexOf('=');
                if (index == -1) {
                    shellEnv.put(env, "");
                    continue;
                }
                String key = env.substring(0, index);
                String val = "";
                if (index < (env.length() - 1)) {
                    val = env.substring(index + 1);
                }
                shellEnv.put(key, val);
            }
        }

        // getting path for shell script
        if (envs.containsKey(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LOCATION)) {
            scriptPath = envs.get(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LOCATION);

            if (envs.containsKey(DSConstants.DISTRIBUTED_SHELL_SCRIPT_TIMESTAMP)) {
                shellScriptPathTimestamp = Long.parseLong(envs.get(DSConstants.DISTRIBUTED_SHELL_SCRIPT_TIMESTAMP));
            }
            if (envs.containsKey(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LEN)) {
                shellScriptPathLen = Long.parseLong(envs.get(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LEN));
            }

            if (!scriptPath.isEmpty() &&
                    (shellScriptPathTimestamp <=0 || shellScriptPathLen <= 0)) {
                LOG.error("Errors on shell script : '" + scriptPath + "', timestamp: '" + shellScriptPathTimestamp +
                        "', length: '" + shellScriptPathLen + "'");
                throw new IllegalArgumentException("Errors on shell script path environment");
            }
        }
    }

    private boolean fileExists(String filePath) {
        return new File(filePath).exists();
    }

    private String readContents(String filePath) throws IOException {
        try (DataInputStream dis = new DataInputStream(
                new FileInputStream(filePath))) {
            return dis.readUTF();
        }
    }

    /**
     * start AMRMClientAsync
     */
    private void startAMRMClient() {

        allocationListener = new AMRMClientAsync.AbstractCallbackHandler() {

            @Override
            public void onContainersCompleted(List<ContainerStatus> statuses) {

            }

            /**
             *
             * one of the containers which is requested by app-master
             * has created, this method will be called
             *
             * each containers are working for each thread, be-ware to be blocked by main-thread
             * @param allocatedContainers
             */
            @Override
            public void onContainersAllocated(List<Container> allocatedContainers) {
                LOG.info("From resource-manager total '" + allocatedContainers.size() + "' containers are allocated");
                numAllocatedContainers.addAndGet(allocatedContainers.size());
                for (Container allocatedContainer : allocatedContainers) {
                    LaunchContainerRunnable runnableLaunchContainer =
                            new LaunchContainerRunnable(allocatedContainer, containerListener,
                                    shellCommand, shellArgs, scriptPath, shellEnv);
                    Thread launchThread = new Thread(runnableLaunchContainer);
                    launchThreads.add(launchThread);
                    launchThread.start();
                }
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

            /**
             * Heart beat
             *
             * @return completed containers / total containers
             */
            @Override
            public float getProgress() {
                float progress = (float) numCompletedContainers.get() / numTotalContainers;
                return progress;
            }

            @Override
            public void onError(Throwable e) {

            }
        };
        amrmClientAsync = AMRMClientAsync.createAMRMClientAsync(amrmHeartbeatMillis, allocationListener);
        amrmClientAsync.init(conf);
        amrmClientAsync.start();
    }

    private void startNMClient() {
        containerListener = new NMClientAsync.AbstractCallbackHandler() {

            @Override
            public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {

            }

            @Override
            public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

            }

            @Override
            public void onContainerStopped(ContainerId containerId) {

            }

            @Override
            public void onStartContainerError(ContainerId containerId, Throwable t) {

            }

            @Override
            public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {

            }

            @Override
            public void onContainerResourceUpdated(ContainerId containerId, Resource resource) {

            }

            @Override
            public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

            }

            @Override
            public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {

            }

            @Override
            public void onUpdateContainerResourceError(ContainerId containerId, Throwable t) {

            }

            @Override
            public void onStopContainerError(ContainerId containerId, Throwable t) {

            }
        };
        nmClientAsync = NMClientAsync.createNMClientAsync(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();
    }

    /**
     * register app-master for alerting alive or not
     * returns information about cluster resources anc capacity
     * request each container until all containers are requested
     */
    private void registerAppMaster() throws IOException, YarnException {
        appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amrmClientAsync.registerApplicationMaster(
                appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);

        Resource maximumResource = response.getMaximumResourceCapability();
        long maxMemory = maximumResource.getMemorySize();
        int maxVCores = maximumResource.getVirtualCores();
        LOG.info("Max memory(" + String.valueOf(maxMemory) + ") and vcores(" + String.valueOf(maxVCores) +
                ") is in this cluster");

        if (containerMemory > maxMemory) {
            containerMemory = maxMemory;
            LOG.info("Requested memory (" + containerMemory + ") is bigger then maximum memory threshold(" + maxMemory + "), " +
                    "Modify maximum-size could be");
        }
        if (containerVCores > maxVCores) {
            containerVCores = maxVCores;
            LOG.info("Requested core (" + containerVCores + ") is bigger then maximum core threshold (" + maxVCores + "), " +
                    "Modify maximum-size could be");
        }
        List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
        LOG.info(appAttemptId + " received " + previousAMRunningContainers.size()
                + " previous attempts' running containers on AM registration.");
        numAllocatedContainers.addAndGet(previousAMRunningContainers.size());

        int numTotalContainersToRequest = numTotalContainers - previousAMRunningContainers.size();
        for (int i = 0; i < numTotalContainersToRequest; ++i) {
            AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
            amrmClientAsync.addContainerRequest(containerAsk);
        }
    }

    private AMRMClient.ContainerRequest setupContainerAskForRM() {
        int requestPriority = 0;
        Priority priority = Priority.newInstance(requestPriority);
        Resource capability = Resource.newInstance(containerMemory, containerVCores);
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, priority);
        return request;
    }

}
