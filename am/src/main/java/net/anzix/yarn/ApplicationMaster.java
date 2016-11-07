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
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class ApplicationMaster implements AMRMClientAsync.CallbackHandler, NMClientAsync.CallbackHandler {

    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;

    private NMClientAsyncImpl nmClientAsync;

    private final Configuration conf;

    private Map<NodeId, ContainerId> startedContainers = new ConcurrentHashMap<>();

    private Executor executor = Executors.newSingleThreadExecutor();

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


        Thread.sleep(650000);
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "ahoj", "http://hup.hu");
        Thread.sleep(1000);
        nmClientAsync.stop();
        amRMClient.stop();

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

                localResources.put("worker.jar", workerJar);

                Map<String, String> env = new HashMap<String, String>();

                StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$())
                        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("worker.jar");
                for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH, YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
                    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                    classPathEnv.append(c.trim());
                }
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(ApplicationConstants.Environment.JAVA_HOME.$$() + "/lib/tools.jar");
                classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
                classPathEnv.append(ApplicationConstants.Environment.JAVA_HOME.$$() + "/../lib/tools.jar");


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
        executor.execute(new StartMissingContainers(updatedNodes, startedContainers, amRMClient));
    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable e) {

    }

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
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {

    }
}
