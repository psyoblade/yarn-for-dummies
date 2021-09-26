package me.suhyuk.yarn.foo.application;

import me.suhyuk.yarn.foo.client.DistributedShellClient;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.log4j.Logger;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

public class LaunchContainerRunnable implements Runnable {

    private static final Logger LOG = Logger.getLogger(LaunchContainerRunnable.class);

    private static final String ExecBatScriptStringPath = DistributedShellClient.SCRIPT_PATH + ".bat";
    private static final String ExecShellScriptStringPath = DistributedShellClient.SCRIPT_PATH + ".sh";

    private Container container;
    private NMClientAsync.AbstractCallbackHandler listener;
    private String command = ""; // 쉘 명령어
    private String args = "";    // 쉘 입력인자
    private String path = "";
    private Map<String, String> envs;

    private final String linux_bash_command = "bash";
    private final String windows_command = "cmd /c";

    public LaunchContainerRunnable(Container allocatedContainer, NMClientAsync.AbstractCallbackHandler containerListener,
                                   String shellCommand, String shellArgs, String scriptPath, Map<String, String> shellEnv) {
        this.container = allocatedContainer;
        this.listener = containerListener;
        this.command = shellCommand;
        this.args = shellArgs;
        this.path = scriptPath;
        this.envs = shellEnv;
    }

    /**
     * 컨테이너 런칭 스레드는 NMs 상의 컨테이너를 launches 하는 것이며, AM 을 통하여 allocated 된 이후 ContainerLaunchContext 를 생성
     * 별도의 컨테이너를 통해 실제 쉘 프로그램이 기동되는 환경을 준비하는 과정
     */
    @Override
    public void run() {

        Map<String, LocalResource> localResources = new HashMap<>();

        // 업로드된 스크립트를 애플리케이션에서도 사용하기 위해 로컬 리소스로 다시 넣는 작업
        if (!path.isEmpty()) {
            Path renamedScriptPath = null;
            if (Shell.WINDOWS) {
                renamedScriptPath = new Path(path + ".bat");
            } else {
                renamedScriptPath = new Path(path + ".sh");
            }

            try {
                renameScriptFile(renamedScriptPath);
            } catch (Exception e) {
                LOG.error("failed to rename script feil");
            }

            URL yarnUrl = null;
            try {
                yarnUrl = URL.fromURI(new URI(renamedScriptPath.toString()));
            } catch (URISyntaxException e) {
                LOG.error("failed parse url from uri");
                // incrementCompletedContainer
                // incrementFailedContainer
                return;
            }

//            LocalResource localResource = LocalResource.newInstance(yarnUrl, LocalResourceType.FILE,
//                    LocalResourceVisibility.APPLICATION, shellScriptPathLen, shellScriptPathTimestamp);
//            localResources.put(Shell.WINDOWS ? ExecBatScriptStringPath : ExecShellScriptStringPath, localResource);
//            command = Shell.WINDOWS ? windows_command : linux_bash_command;
        }

        // 로컬라이즈드 된 파일이 존재한다면 해당 파일에 대한 처리가 필요하며

        // 최종 애플리케이션 실행을 위한 커맨드 및 리소스 정보를 전달하여 애플리케이션을 수행합니다
        Vector<CharSequence> vargs = new Vector<>(5);
        vargs.add(command);
        if (!path.isEmpty()) {
            vargs.add(Shell.WINDOWS ? ExecBatScriptStringPath : ExecShellScriptStringPath);
        }
        vargs.add(args);
        vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
        vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }

        List<String> commands = new ArrayList<>();
        commands.add(command.toString());

        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, envs, commands, null, null, null);
//        listener.addContainer(container.getId(), ctx);

    }

    private void renameScriptFile(final Path renamedScriptPath) {
//        appSubmitterUgi.doAs(new PrivilegedExceptionAction<Void>() {
//            @Override
//            public Void run() throws IOException {
//                FileSystem fs = renamedScriptPath.getFileSystem(conf);
//                fs.rename(new Path(scriptPath), renamedScriptPath);
//                return null;
//            }
//        })
    }
}
