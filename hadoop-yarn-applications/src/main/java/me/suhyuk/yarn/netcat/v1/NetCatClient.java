package me.suhyuk.yarn.netcat.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * https://www.ibm.com/docs/en/sdk-java-technology/8?topic=options-xxmaxdirectmemorysize
 */
public class NetCatClient {

    private static final Logger LOG = Logger.getLogger(NetCatClient.class);
    private static final String appName = "hadoop-yarn-applications";

    // am related settings
    private static final String amClassName = "me.suhyuk.yarn.netcat.v1.NetCatAppMaster";
    private static final float amMemRatio = 0.7f;
    private static final int amMemory = 10;
    private static final int amCores = 1;
    private static final int amMaxDirect = 5;
    private static final int amPriority = 0;
    private static final String amQueue = "default";

    private String workDir;
    private Configuration conf;
    private ApplicationId applicationId;

    private String appMasterJar = "";
    private String appMasterJarPath = appName + ".jar";
    private String log4jJar = "";
    private String log4jJarPath = "log4j.properties";


    public NetCatClient(String workDir) {
        this(new YarnConfiguration(), workDir);
    }

    public NetCatClient(Configuration conf, String workDir) {
        this.conf = conf;
        this.workDir = workDir;
        appMasterJar = workDir + File.separator + appMasterJarPath;
        log4jJar = workDir + File.separator + log4jJarPath;
    }

    /**
     * yarnClient 를 통해서 수행할 작업을 submit 합니다
     * - 실행을 위한 명령어 및 환경변수는 ApplicationSubmissionContext 객체에 담아서 전달하며
     * - 실행에 필요한 파일은 하둡에 업로드하고
     *
     * @throws IOException
     * @throws YarnException
     */
    public void run() throws IOException, YarnException {

        try (YarnClient yarnClient = YarnClient.createYarnClient()) { // init yarn-client
            LOG.info("YarnClient has created");

            addResources();
            LOG.info("Custom Configuration(core, hdfs, yarn-site.xml) has set");

            yarnClient.init(conf);
            yarnClient.start();
            LOG.info("YarnClient initialized and started");

            YarnClientApplication app = yarnClient.createApplication();
            LOG.info("YarnClientApplication has created");

            ApplicationSubmissionContext appContext = newApplicationSubmissionContext(app);
            LOG.info("ApplicationSubmissionContext has created with appId '" + applicationId + "'");

            yarnClient.submitApplication(appContext);
        }

    }

    private ApplicationSubmissionContext newApplicationSubmissionContext(YarnClientApplication app) throws IOException {
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        applicationId = appContext.getApplicationId();
        appContext.setApplicationName(appName);
        appContext.setAMContainerSpec(newContainerLaunchContext());
        appContext.setPriority(Priority.newInstance(amPriority));
        appContext.setQueue(amQueue);
        appContext.setResource(Resource.newInstance(amMemory, amCores));
        return appContext;
    }

    private ContainerLaunchContext newContainerLaunchContext() throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Map<String, LocalResource> localResources = new HashMap<>();
        Map<String, String> envs = Collections.singletonMap("CLASSPATH", getClasspathEnvs());
        List<String> commands = Collections.singletonList(getCommands());
        addToLocalResources(fs, appMasterJar, appMasterJarPath, applicationId.toString(), localResources, null);
        addToLocalResources(fs, log4jJar, log4jJarPath, applicationId.toString(), localResources, null);
        return ContainerLaunchContext.newInstance(localResources, envs, commands, null, null, null);
    }

    private String getClasspathEnvs() {
        StringBuilder envs = new StringBuilder(Environment.CLASSPATH.$$())
            .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            envs.append(ApplicationConstants.CLASS_PATH_SEPARATOR)
                    .append(c.trim());
        }
        envs.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
        return envs.toString();
    }

    private String getCommands() {
        Vector<CharSequence> vargs = new Vector<>(10);
        vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
        vargs.add("-Xmx" + (int) Math.ceil(amMemory * amMemRatio) + "m");
        vargs.add("-XX:MaxDirectMemorySize=" + amMaxDirect + "m");
        vargs.add(amClassName);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");
        return String.join(" ", vargs);
    }

    /**
     * 원본 파일이 없다면, 대상 경로만 생성하고, 있다면 로컬 파일을 원격지로 복사합니다
     * @param fs
     * @param fileSrcPath
     * @param fileDstPath
     * @param appId
     * @param localResources
     * @param resources
     * @throws IOException
     */
    private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath,
                                     String appId, Map<String, LocalResource> localResources,
                                     String resources) throws IOException {
        String suffix = NetCatAppMaster.getRelativePath(appName, appId, fileDstPath);
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        if (fileSrcPath == null) {
            try (FSDataOutputStream ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710))) {
                ostream.writeUTF(resources);
            }
        } else {
            fs.copyFromLocalFile(new Path(fileSrcPath), dst);
            LOG.info("copyFromLocalFile '" + fileSrcPath.toString() + "' to '" + dst.toString() + "'");
        }
        FileStatus dstFileStatus = fs.getFileStatus(dst);
        LocalResource dstResource = LocalResource.newInstance(
                URL.fromURI(dst.toUri()),
                LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION,
                dstFileStatus.getLen(),
                dstFileStatus.getModificationTime()
        );
        localResources.put(fileDstPath, dstResource);
        LOG.info("addResource '" + fileDstPath.toString() + "' at '" + dstResource.getResource() + "'");
    }

    private void addResources() {
        conf.addResource("conf/hadoop/core-site.xml");
        conf.addResource("conf/hadoop/hdfs-site.xml");
        conf.addResource("conf/hadoop/yarn-site.xml");
    }

    private static void printUsageAndExitWithError() {
        System.err.println("Illegal Arguments Length - exit with errno 1");
        System.err.println("Usage NetCatClient [work-dir]");
        System.exit(1);
    }

    public static void main(String[] args) {
        try {
            String workDir = ".";
            if (args.length == 1) {
                workDir = args[0];
            } else {
                printUsageAndExitWithError();
            }
            NetCatClient netcatClient = new NetCatClient(workDir);
            netcatClient.run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
