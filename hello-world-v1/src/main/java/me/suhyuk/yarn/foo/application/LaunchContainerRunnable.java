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
    private String command = ""; // shell commands
    private String args = "";    // shell args
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
     * launch containers on node-manager using allocated containers with app-master
     */
    @Override
    public void run() {

        Map<String, LocalResource> localResources = new HashMap<>();

        // add local-resources by uploading scripts
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

        // TODO: handling localized files
        // run application using command and resources
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
