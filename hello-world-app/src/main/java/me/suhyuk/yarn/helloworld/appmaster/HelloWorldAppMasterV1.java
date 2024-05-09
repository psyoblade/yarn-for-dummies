package me.suhyuk.yarn.helloworld.appmaster;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.AllocateResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

/**
 * Yarn Application Sync Version
 */

@Slf4j
public class HelloWorldAppMasterV1 extends AbstractAppMaster implements HelloWorldAppMaster {

    private AMRMClient<AMRMClient.ContainerRequest> amrmClient;

    public HelloWorldAppMasterV1() {
        super();
    }

    @Override
    public void init() {
    }

    @Override
    public void run() throws IOException, YarnException, InterruptedException {
        // AMRMClient
        amrmClient = AMRMClient.createAMRMClient();
        amrmClient.init(conf);
        amrmClient.start();
        amrmClient.registerApplicationMaster("", 0, "");

        NMClient nmClient = NMClient.createNMClient();
        nmClient.init(conf);
        nmClient.start();

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        AMRMClient.ContainerRequest containerAsk = new AMRMClient.ContainerRequest(capability, null, null, priority);
        amrmClient.addContainerRequest(containerAsk);

        boolean allocatedContainer = false;
        while (!allocatedContainer) {
            AllocateResponse response = amrmClient.allocate(0);
            for (Container container : response.getAllocatedContainers()) {
                allocatedContainer = true;

                ContainerLaunchContext context = Records.newRecord(ContainerLaunchContext.class);
                context.setCommands(
                    Collections.singletonList(
                        String.format("%s 1>%s/stdout 2>%s/stderr",
                            "/usr/bin/vmstat",
                            ApplicationConstants.LOG_DIR_EXPANSION_VAR,
                            ApplicationConstants.LOG_DIR_EXPANSION_VAR)
                    )
                );
                nmClient.startContainer(container, context);
            }
            TimeUnit.SECONDS.sleep(1);
        }

        boolean completedContainer = false;
        while (!completedContainer) {
            AllocateResponse response = amrmClient.allocate(0);
            for (ContainerStatus status : response.getCompletedContainersStatuses()) {
                completedContainer = true;
            }
            TimeUnit.SECONDS.sleep(1);
        }

        amrmClient.unregisterApplicationMaster(
            FinalApplicationStatus.SUCCEEDED, "", ""
        );
    }

    @Override
    public void finish() {}

    @Override
    public void cleanup() {}

    public static void main(String[] args) {
        HelloWorldAppMaster appMaster = null;
        try {
            appMaster = new HelloWorldAppMasterV1();
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
