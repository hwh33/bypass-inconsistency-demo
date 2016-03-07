# bypass-inconsistency-demo

A demonstration of an apparent inconsistency in HBase's ObserverContext.bypass() method.

To see the logic behind this test, look to src/main/java/demonstration/BypassDemonstration.java.

# Running this demonstration

To run the demonstration for yourself:

1. Clone the repository to your machine:
    `git clone git@github.com:hwh33/bypass-inconsistency-demo.git`
2. Use gradle to run the main method of BypassDemonstration:
    `gradle run`
    
Gradle will download any dependencies you are missing, compile the project, and run the demonstration.
You will see the final output "Test complete, all assertions held true." which indicates that the
assertions (which demonstrate inconsistencies) held true.

# Running this demonstration against an HBase cluster

Simply using the 'gradle run' command runs the demo using HBase's in-memory testing utility. To run
the demo against an actual HBase cluster, you need to specify "against-cluster" mode and provide the
path to the Hadoop configuration file for the cluster.

The command should look like:
    `gradle run -Pargs=against-cluster,/path/to/config-file`

