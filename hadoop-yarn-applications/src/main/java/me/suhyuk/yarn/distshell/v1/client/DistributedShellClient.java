package me.suhyuk.yarn.distshell.v1.client;

import me.suhyuk.yarn.distshell.v1.common.DSConstants;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.logging.Logger;

public class DistributedShellClient {

    public static final String SCRIPT_PATH = "ExecScript";

    private static final Logger logger = Logger.getLogger(String.valueOf(DistributedShellClient.class));
    private static final String appName = "WordCountYarnApplication";

    private static final Priority amPriority = Priority.newInstance(0);
    private static final String amQueue = "default";

    private static final long containerMemory = 64;
    private static final int containerVCores = 1;
    private static final long numContainers = 1;

    private static final int amMemory = 16;
    private static final int amVCores = 1;

    // 로컬 리소스로 전달될 변수 및 파일명
    private String shellCommand = "";
    private static final String shellCommandPath = "shellCommands";
    private static final String shellArgsPath = "shellArgs";
    private static final String appMasterJarPath = "AppMaster.jar";
    private static final String log4jPath = "log4j.properties";
    private static final String shellScriptPath = "./wordcount.sh";

    Configuration conf = new Configuration();
    YarnClient yarnClient = YarnClient.createYarnClient();
    boolean keepContainers = true;

    YarnClientApplication app;
    ApplicationSubmissionContext appContext;
    ApplicationId appId;
    FileSystem fs;
    ContainerLaunchContext amContainer;

    Map localResources = new HashMap<String, LocalResource>();
    Map<String, String> env = new HashMap<>();
    List<String> commands = new ArrayList<>();

    public void init() throws IOException, YarnException {
        yarnClient.init(conf);
        yarnClient.start();
        app = yarnClient.createApplication();
        fs = FileSystem.get(conf);
        logger.info("yarnClient initialized");
    }

    /**
     * 클러스터의 리소스의 상태를 확인합니다
     */
    public void checkClusterResources() {
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();
        Resource resource = appResponse.getMaximumResourceCapability();
        logger.info(resource.toString());
    }

    /**
     * 애플리케이션 제출을 위한 애플리케이션 아이디를 생성합니다
     */
    public void createSubmissionContext() {
        appContext = app.getApplicationSubmissionContext();
        appId = appContext.getApplicationId();
        appContext.setKeepContainersAcrossApplicationAttempts(keepContainers);
        appContext.setApplicationName(appName);
    }

    /**
     * 앱마스터에서 필요하지 않아 로컬리소스 사용하지 않는 경우는 하둡(/user/psyoblade/wordcount/application_1234/ExecScript)에 업로드
     * 앱마스터가 실행할 애플리케이션 관련 정보를 환경변수에 저장합니다
     */
    public void uploadShellScriptToHdfs() throws IOException {
        String hdfsShellScriptLocation = "";
        long hdfsShellScriptLen = 0;
        long hdfsShellScriptTimestamp = 0;
        if (!shellScriptPath.isEmpty()) {
            Path shellSrc = new Path(shellScriptPath);
            String shellPathSuffix = appName + "/" + appId.toString() + "/" + SCRIPT_PATH;
            Path shellDst = new Path(fs.getHomeDirectory(), shellPathSuffix);
            fs.copyFromLocalFile(false, true, shellSrc, shellDst);
            hdfsShellScriptLocation = shellDst.toUri().toString();
            FileStatus shellFileStatus = fs.getFileStatus(shellDst);
            hdfsShellScriptLen = shellFileStatus.getLen();
            hdfsShellScriptTimestamp = shellFileStatus.getModificationTime();
        }

        env.put(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LOCATION, hdfsShellScriptLocation);
        env.put(DSConstants.DISTRIBUTED_SHELL_SCRIPT_TIMESTAMP, Long.toString(hdfsShellScriptTimestamp));
        env.put(DSConstants.DISTRIBUTED_SHELL_SCRIPT_LEN, Long.toString(hdfsShellScriptLen));
    }

    /**
     * 애플리케이션 수행시에 필요한 appMaster.jar, log4j.properties 및 shell 파일 등을 추가합니다
     * @throws IOException
     */
    public void uploadLocalResources() throws IOException {
        String appMasterJarSrc = "";
        addToLocalResources(fs, appMasterJarSrc, appMasterJarPath, appId.toString(), localResources, null);

        String log4jPropSrc = "";
        addToLocalResources(fs, log4jPropSrc, log4jPath, appId.toString(), localResources, null);

        if (!shellCommand.isEmpty()) { // 수행할 쉘 명령어를 별도의 파일로 저장합니다
            addToLocalResources(fs, null, shellCommandPath, appId.toString(), localResources, shellCommand);
        }

        String[] shellArgs = new String[] { "" };
        String joinedShellArgs = StringUtils.join(shellArgs, " ");
        if (shellArgs.length > 0) {
            addToLocalResources(fs, null, shellArgsPath, appId.toString(), localResources, joinedShellArgs);
        }
    }

    /**
     * 애플리케이션 수행을 위해 전달할 모든 로컬 리소스를 localResources 추가
     * @param fs
     * @param fileSrcPath
     * @param fileDstPath
     * @param appId
     * @param localResources
     * @param resources
     * @throws IOException
     */
    private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath, String appId,
                                     Map<String, LocalResource> localResources, String resources) throws IOException {
        String suffix = appName + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            FSDataOutputStream ostream = null;
            try {
                ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
                ostream.writeUTF(resources);
            } finally {
                IOUtils.closeQuietly(ostream);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        }
        FileStatus fileStatus = fs.getFileStatus(dst);
        LocalResource localResource = LocalResource.newInstance(
                URL.fromURI(dst.toUri()),
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                fileStatus.getLen(),
                fileStatus.getModificationTime()
        );
        localResources.put(fileDstPath, localResource);
    }

    /**
     * 앱마스터 Jar 위치를 클래스패스에 추가하고, 하둡 관련 클래스패스는 환경변수에 넣지 않고, 별도로 설정합니다
     * CLASSPATH.$$ 통해서 크로스플랫폼 수행 시에도 환경변수에 문제가 없도록 구성합니다
     * 즉, {{CLASSPATH}} 변수를 윈도우에서는 %CLASSPATH% 로 리눅스에서는 $CLASSPATH 로 변경합니다
     */
    public void setupApplicationClassPath() {
        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
    }

    /**
     * ArrayList 대신 Vector 를 사용하고 있는데, 언제 동기적인 사용이 필요한가?
     */
    public void setupApplicationMasterCommands() {
        String appMasterMainClass = "";
        int shellCmdPriority = 0;

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + Integer.toString(amMemory) + "m");
        vargs.add(appMasterMainClass);
        vargs.add("--container_memory " + String.valueOf(containerMemory));
        vargs.add("--container_vcores " + String.valueOf(containerVCores));
        vargs.add("--num_containers " + String.valueOf(numContainers));
        vargs.add("--priority " + String.valueOf(shellCmdPriority));

        Map<String, String> shellEnv = Collections.emptyMap();
        for (Map.Entry<String, String> entry : shellEnv.entrySet()) {
            vargs.add("--shell_env " + entry.getKey() + "=" + entry.getValue());
        }
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str: vargs) {
            command.append(str).append(" ");
        }

        commands.add(command.toString());
    }

    /**
     * 앱마스터 실행을 위한 컨테이너 런치 컨텍스트를 생성합니다
     */
    public void createContainerLaunchContext() {
        amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);
    }

    public void createCredentials() throws IOException {
        Credentials credentials = new Credentials();
        String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
        if (tokenRenewer == null || tokenRenewer.length() == 0) {
            throw new IOException("Can't get Master Kerberos principal form RM");
        }

        final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
        if (tokens != null) {
            for (Token<?> token : tokens) {
                logger.info("Got dt for " + fs.getUri() + "; " + token);
            }
        }
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        amContainer.setTokens(fsTokens);
    }

    public void submitApplication() throws IOException, YarnException {
        appContext.setPriority(amPriority);
        appContext.setQueue(amQueue);
        yarnClient.submitApplication(appContext);
    }

    /**
     * 애플리케이션이 종료될 때까지 대기합니다
     */
    public void waitUntilFinished(int seconds) throws IOException, YarnException, InterruptedException {
        for (int i = 0; i < seconds; i++) {
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            String trackingUrl = report.getTrackingUrl();
            System.out.println(trackingUrl);
            Thread.sleep(1000);
        }
    }

    public void killApplication() throws IOException, YarnException {
        yarnClient.killApplication(appId);
    }
}
