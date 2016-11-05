package net.anzix.yarn;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler {

    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;

    private NMClientAsyncImpl nmClientAsync;
    private final Configuration conf;

    public static void main(String[] args) throws Exception {
        new ApplicationMaster(args[0], args[1]).run();
    }

    public ApplicationMaster(String yarnHost, String hdfsHost) {

        conf = new Configuration();
        conf.set("yarn.resourcemanager.hostname", yarnHost);
        conf.set("fs.default.name", "hdfs://" + hdfsHost + ":9000");

    }

    private void run() throws Exception {

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, this);

        amRMClient.init(conf);
        amRMClient.start();

        nmClientAsync = new NMClientAsyncImpl(new NMCallbackHandler());
        nmClientAsync.init(conf);
        nmClientAsync.start();

        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, -1, "http://index.hu");

        System.out.println(response.getQueue());
        System.out.println("AHOJ");

        for (int i = 0; i < 10; i++) {
            requestContainer();
            Thread.sleep(5000);
        }

        Thread.sleep(650000);
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "ahoj", "http://hup.hu");
        Thread.sleep(1000);
        nmClientAsync.stop();
        amRMClient.stop();

    }

    private void requestContainer() {
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        //capability.setVirtualCores(1);
        AMRMClient.ContainerRequest containerReq = new AMRMClient.ContainerRequest(
                capability,
                null /* hosts String[] */,
                null /* racks String [] */,
                priority);


        amRMClient.addContainerRequest(containerReq);
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {

    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {

            try {
                Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

                FileSystem fs = FileSystem.get(conf);
                Path amJar = fs.makeQualified(new Path("/worker.jar"));

                FileStatus status = fs.getFileStatus(amJar);
                LocalResource workerJar = Records.newRecord(LocalResource.class);
                workerJar.setResource(ConverterUtils.getYarnUrlFromPath(amJar));
                workerJar.setSize(status.getLen());
                workerJar.setTimestamp(status.getModificationTime());
                workerJar.setType(LocalResourceType.FILE);
                workerJar.setVisibility(LocalResourceVisibility.APPLICATION);

                localResources.put("worker.jar",workerJar);

                Map<String, String> env = new HashMap<String, String>();

                StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("worker.jar");
                for (String c : conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                    classPathEnv.append(c.trim());
                }

                List<String> commands = ImmutableList.of(
                        ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java" +
                                " -cp " + classPathEnv + " net.anzix.yarn.Worker -Xmx256m "

                                + "1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout "
                                + "2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");


                ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

                nmClientAsync.startContainerAsync(container, amContainer);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    @Override
    public void onShutdownRequest() {

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

    }
}
