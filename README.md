One of the most simple YARN application.

It creates a new ApplicationMaster which starts a container (10 sec wait + println).

The ApplicationMaster stops after 30 secs.

To run:

1. Build the project
2. Upload am.jar and worker.jar to the hdfs root
3. Start the client with the hostname of the HDFS/resourcemanager as the parameter.
