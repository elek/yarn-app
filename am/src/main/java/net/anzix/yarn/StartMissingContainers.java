package net.anzix.yarn;

import com.google.common.base.Functions;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.util.Records;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StartMissingContainers implements Runnable {

    private final List<NodeReport> updatedNodes;

    private final Map<NodeId, ContainerId> startedContainers;

    private final AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;

    public StartMissingContainers(List<NodeReport> updatedNodes, Map<NodeId, ContainerId> startedContainers, AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient) {
        this.updatedNodes = updatedNodes;
        this.startedContainers = startedContainers;
        this.amRMClient = amRMClient;
    }

    @Override
    public void run() {
        updatedNodes.stream().collect(Collectors<NodeReport,NodeId,Map<NodeId,NodeReport>>.toMap(, Functions.identity()));
                Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(1024);
        capability.setVirtualCores(1);
        AMRMClient.ContainerRequest containerReq = new AMRMClient.ContainerRequest(
                capability,
                new String[] /* hosts String[] */,
                null /* racks String [] */,
                priority);


        amRMClient.addContainerRequest(containerReq);


    }
}
