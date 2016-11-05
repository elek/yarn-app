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
