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

    // 할당 및 완료된 컨테이너의 갯수
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
            throw new IllegalArgumentException("앱마스터에 의해 수행될 쉘 스크립트가 환경변수로 전달되지 않았습니다");
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

        // 쉘 파일 경로를 가져옵니다
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
                LOG.error("쉘 실행정보에 오류가 있습니다. 쉘: '" + scriptPath + "', 타임스탬프: '" + shellScriptPathTimestamp +
                        "', 길이: '" + shellScriptPathLen + "'");
                throw new IllegalArgumentException("쉘 스크리브 경로 환경변수에 오류가 있습니다");
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
     * 리소스 할당에 대한 리스너를 AMRMClientAsync 통해서 생성 및 시작합니다
     */
    private void startAMRMClient() {

        allocationListener = new AMRMClientAsync.AbstractCallbackHandler() {

            @Override
            public void onContainersCompleted(List<ContainerStatus> statuses) {

            }

            /**
             * 앱마스터에 의해 요청된 컨테이너 가운데, 할당된 컨테이너가 비동기적으로 생성되었을 때에 할당된 컨테이너와 함께 호출됩니다
             * 개별 컨테이너들을 별도의 스레드로 동작하여 메인 스레드가 블록되지 않도록 합니다
             *
             * @param allocatedContainers
             */
            @Override
            public void onContainersAllocated(List<Container> allocatedContainers) {
                LOG.info("리소스 매니저로부터 총 '" + allocatedContainers.size() + "'개의 컨테이너를 할당 받았습니다");
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
             * Heart beat 시에 호출되는 애플리케이션 진행을 나타내는 핸들러
             *
             * @return 완료된 컨테이너 수 / 전체 컨테이너 수
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

    /**
     * 컨테이너 생성에 대한 리스너를 NMClientAsync 통해서 생성 및 시작합니다
     */
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
     * 앱마스터가 리소스매니저에게 자신이 살아있음을 알리기 위해 등록 합니다
     * 등록결과를 통해 현재 클러스터의 리소스 용량을 확인할 수 있습니다
     * 요청해야 하는 컨테이너 수를 계산하여 모든 컨테이너 수를 채울때 까지 컨테이너를 요청합니다
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
            LOG.info("요청한 메모리의 크기(" + containerMemory + ")가 컨테이너 최대 메모리 임계치(" + maxMemory + ")보다 " +
                    "크기 때문에, 설정가능한 최대 크기로 설정됩니다");
        }
        if (containerVCores > maxVCores) {
            containerVCores = maxVCores;
            LOG.info("요청한 코어의 수(" + containerVCores + ")가 컨테이너 최대 코어 임계치(" + maxVCores + ")보다 " +
                    "크기 때문에, 설정가능한 최대 크기로 설정됩니다");
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
        LOG.info("컨테이너 요청 : " + request.toString());
        return request;
    }

}
