package net.anzix.yarn;

import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

import java.nio.ByteBuffer;
import java.util.Map;


public class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    @Override
    public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
        System.out.println("Container started " + containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {
        System.out.println("Container status receiverd " + containerId + " " + containerStatus);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        System.out.println("Container is stopped " + containerId);
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        System.out.println("onStartContainerError " + containerId + " " + t.getMessage());
    }

    @Override
    public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
        System.out.println("Container status error " + containerId + " " + t.getMessage());

    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {

    }
}
