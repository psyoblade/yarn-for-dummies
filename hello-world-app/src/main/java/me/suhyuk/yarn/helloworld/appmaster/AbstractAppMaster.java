package me.suhyuk.yarn.helloworld.appmaster;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.Map;

public class AbstractAppMaster {

    // application related
    Configuration conf;
    ContainerId containerId;
    ApplicationAttemptId attemptId;
    ApplicationId applicationId;

    // user related
    UserGroupInformation appSubmitterUgi;

    // amrm, nm client
    String appMasterHostname;
    int appMasterHostPort = -1;
    String appMasterTrackingUrl = "";
    Map<String, Resource> resourceProfiles;
    FinalApplicationStatus appStatus;
    String message;

    public AbstractAppMaster() {
        conf = new YarnConfiguration();
    }
}
