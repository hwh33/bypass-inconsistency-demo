package demonstration;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import util.MyHBaseCluster;
import util.MyRegionObserver;

/**
 * Demonstrates an inconsistency in {@link ObserverContext#bypass()}.
 *
 * @author Harry Harpham
 *
 */
public class BypassDemonstration {

	// Set to true to suppress logging output.
	private static final boolean QUIET_MODE = true;

	/**
	 * The first argument sets the mode and should be one of the following:
	 * <ul>
	 * <li>in-memory
	 * <li>against-cluster
	 * </ul>
	 * If "against-cluster" is supplied, then the second argument should be the
	 * path to the HBase configuration file (probably something that looks like
	 * /etc/hbase/conf/hbase-site.xml).
	 * <p>
	 * Fails fast on any exceptions encountered.
	 */
	public static void main(String... args) {
		// running modes
		final String IN_MEMORY = "in-memory";
		final String AGAINST_CLUSTER = "against-cluster";

		// Default to in-memory.
		String mode = IN_MEMORY;
		if (args.length > 1 && args[0].equals(AGAINST_CLUSTER)) {
			mode = AGAINST_CLUSTER;
		}

		if (QUIET_MODE) {
			Logger.getRootLogger().setLevel(Level.OFF);
		}

		System.out.println("Beginning in mode: " + mode);

		// Set up the cluster.
		MyHBaseCluster cluster;
		try {
			if (mode.equals(IN_MEMORY)) {
				System.out.println("Setting up in-memory cluster");
				cluster = new MyHBaseCluster();
			} else {
				String pathToConfig = args[1];
				Configuration config = HBaseConfiguration.create();
				config.addResource(new FileInputStream(new File(pathToConfig)));

				System.out.println("Establishing connection to cluster");
				cluster = new MyHBaseCluster(config);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Configuration file not found; exiting");
			System.exit(1);
			return;
		} catch (Exception e) {
			System.err.println("Fatal failure occured; exiting");
			System.exit(1);
			return;
		}

		System.out.println("Running test");
		try {
			doDemo(cluster);
		} catch (Exception e) {
			System.err.println("Failure occurred:");
			e.printStackTrace(System.err);
			System.err.println("Exiting");
			System.exit(1);
		}

		System.out.println("Test complete, all assertions held true.");
		System.exit(0);
	}

	private static void doDemo(MyHBaseCluster cluster) throws Exception {
		// row keys
		final byte[] BYPASSED_PUT_RK = "bypassed put".getBytes();
		final byte[] NON_BYPASSED_PUT_RK = "non bypassed put".getBytes();

		final byte[] QUALIFIER = "qualifier".getBytes();

		// Get the region constructs from the cluster.
		HRegion region = cluster.region();
		MyRegionObserver regionObserver = cluster.regionObserver();

		// Create two puts. The only difference is their row key (the rest of
		// the put is irrelevant as far as this test is concerned).
		Put nonBypassedPut = new Put(NON_BYPASSED_PUT_RK);
		nonBypassedPut.addColumn(MyHBaseCluster.FAMILY_NAME, QUALIFIER, "value".getBytes());
		Put bypassedPut = new Put(BYPASSED_PUT_RK);
		bypassedPut.addColumn(MyHBaseCluster.FAMILY_NAME, QUALIFIER, "value".getBytes());

		// Set the region observer's bypass function. This will cause the
		// region observer to bypass anything with a row key equal to
		// BYPASSED_PUT_RK.
		regionObserver.setBypassFunction(p -> Bytes.equals(p.getRow(), BYPASSED_PUT_RK));

		// Batch 4 puts. Only one of these is not bypassed. It does not matter
		// where in the batch this put is.
		Put[] puts = new Put[] { bypassedPut, nonBypassedPut, bypassedPut, bypassedPut };
		region.batchMutate(puts);

		// Despite bypassing 3 of the puts, we will see that all 4 of them
		// resulted in calls to postPut.
		assert(regionObserver.prePutCalls() == 4 && regionObserver.postPutCalls() == 4);

		// Batch 4 puts. All of these are bypassed.
		puts = new Put[] { bypassedPut, bypassedPut, bypassedPut, bypassedPut };
		region.batchMutate(puts);

		// If all of the puts in the batch are bypassed, we will see that none
		// of them result in calls to postPut. This seems inconsistent with the
		// above behavior. Why is the behavior of one put dependent on what
		// happens to other puts in the batch?
		assert(regionObserver.prePutCalls() == 8 && regionObserver.postPutCalls() == 4);

		cluster.close();
	}

}
