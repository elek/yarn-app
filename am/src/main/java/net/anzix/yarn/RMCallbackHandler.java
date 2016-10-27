package net.anzix.yarn;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;


public class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

    private Configuration configuration;

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
        System.out.println("container completed" + statuses);
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        try {

            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

            FileSystem fs = FileSystem.get(configuration);
            Path amJar = fs.makeQualified(new Path("/worker.jar"));

            FileStatus status = fs.getFileStatus(amJar);
            LocalResource appMasterJar = Records.newRecord(LocalResource.class);
            appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(amJar));
            appMasterJar.setSize(status.getLen());
            appMasterJar.setTimestamp(status.getModificationTime());
            appMasterJar.setType(LocalResourceType.FILE);
            appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);


            Map<String, String> env = new HashMap<String, String>();

            StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                    .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("worker.jar");
            for (String c : configuration.getStrings(
                    YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                    YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(c.trim());
            }

            List<String> commands = ImmutableList.of(
                    ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java" +
                            " -cp " + classPathEnv + " net.anzix.yarn.Worker -Xmx256m"
                            + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
                            + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");


            ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    @Override
    public void onShutdownRequest() {
        System.out.println("shutdown request");
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        e.printStackTrace();
    }
}
