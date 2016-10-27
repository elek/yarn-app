package net.anzix.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.util.Records;

import java.util.*;

public class ApplicationMaster {

    private AMRMClientAsync<AMRMClient.ContainerRequest> amRMClient;
    private NMClientAsyncImpl nmClientAsync;

    public static void main(String[] args) throws Exception {
        new ApplicationMaster().run(args[0]);
    }

    private void run(String host) throws Exception {
        Configuration conf = new Configuration();
        conf.set("yarn.resourcemanager.hostname", host);
        conf.set("fs.default.name", "hdfs://" + host + ":9000");
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();

        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, allocListener);

        amRMClient.init(conf);
        amRMClient.start();

        nmClientAsync = new NMClientAsyncImpl(new NMCallbackHandler());
        nmClientAsync.init(conf);
        nmClientAsync.start();

        String appMasterHostname = NetUtils.getHostname();
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(appMasterHostname, -1, "");

        System.out.println(response.getQueue());
        System.out.println("AHOJ");

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

        Thread.sleep(30000);
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "ahoj", "");
        nmClientAsync.stop();
        amRMClient.stop();

    }
}
