package net.anzix.yarn;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Client {
    public static void main(String[] args) throws Exception {
        new Client().run(args[0], args[1]);
    }

    private void run(String yarnHost, String hdfsHost) throws Exception {
        YarnClient yarnClient = YarnClient.createYarnClient();
        Configuration configuration = new Configuration();
        configuration.set("yarn.resourcemanager.hostname", yarnHost);
        configuration.set("fs.default.name", "hdfs://" + hdfsHost + ":9000");
        yarnClient.init(configuration);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

        ApplicationSubmissionContext context = app.getApplicationSubmissionContext();
        context.setApplicationName("test1");

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        FileSystem fs = FileSystem.get(configuration);
        Path amJar = fs.makeQualified(new Path("/am.jar"));

        FileStatus status = fs.getFileStatus(amJar);
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(amJar));
        appMasterJar.setSize(status.getLen());
        appMasterJar.setTimestamp(status.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);


        Map<String, String> env = new HashMap<String, String>();

        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("am.jar");
        for (String c : configuration.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        List<String> commands = ImmutableList.of(
                ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java" +
                        " -Xmx256m -cp " + classPathEnv + " net.anzix.yarn.ApplicationMaster " +
                        yarnHost + " " + hdfsHost + " "
                        + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
                        + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");


        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

        Resource capability = Resource.newInstance(2096, 1);
        context.setResource(capability);
        context.setAMContainerSpec(amContainer);


        amContainer.setLocalResources(Collections.singletonMap("am.jar", appMasterJar));


        yarnClient.submitApplication(context);
        System.out.println(appResponse);
        yarnClient.close();


    }
}
