package util;

import java.io.IOException;
import java.util.function.Supplier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionCoprocessorHost;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Helper class.
 * 
 * We use this to abstract away the difference between running this test in
 * memory against an {@link HBaseTestingUtility} and running against an actual
 * HBase cluster.
 * 
 * We'll just let it throw any exceptions and fail fast.
 * 
 * @author Harry Harpham
 * 
 */
public class MyHBaseCluster implements AutoCloseable {

	// The table is set up specifically for this test and destroyed after.
	public static final TableName TABLE_NAME = TableName.valueOf("BypassTestTable");
	public static final byte[] FAMILY_NAME = Bytes.toBytes("f");

	private HRegion region;
	private MyRegionObserver regionObserver;

	// Kind of an abuse of supplier, but eh.
	private Supplier<Exception> shutDownProcedure;

	/**
	 * In-memory instance.
	 */
	public MyHBaseCluster() throws Exception {
<<<<<<< Updated upstream
		HBaseTestingUtility testingUtility = new HBaseTestingUtility();
=======
		Configuration config = HBaseConfiguration.create();
		// This is simply to resolve a name-space issue in creating the JAR.
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		HBaseTestingUtility testingUtility = new HBaseTestingUtility(config);
>>>>>>> Stashed changes

		try {
			testingUtility.startMiniCluster();
			Configuration config = testingUtility.getConfiguration();
			HTableDescriptor htd = initializeTable(config);
			this.region = initializeRegion(htd, config);

			testingUtility.createTable(TABLE_NAME.getName(), FAMILY_NAME);
			this.regionObserver = (MyRegionObserver) region.getCoprocessorHost()
					.findCoprocessor(MyRegionObserver.class.getName());
		} catch (Exception initializationException) {
			System.err.println("Error occurred in initialization:");
			initializationException.printStackTrace();
			System.err.println("Shutting down test cluster");
			try {
				testingUtility.shutdownMiniHBaseCluster();
				throw initializationException;
			} catch (Exception shutDownException) {
				System.err.println("Shut down error occurred:");
				shutDownException.printStackTrace();
				throw shutDownException;
			}
		}

		shutDownProcedure = () -> {
			try {
				testingUtility.shutdownMiniCluster();
			} catch (Exception e) {
				return e;
			}
			return null;
		};
	}

	/**
	 * Uses an actual cluster.
	 *
	 * @param config
	 *            the configuration for the cluster
	 */
	public MyHBaseCluster(Configuration config) throws Exception {
		// This is simply to resolve a name-space issue in creating the JAR.
		config.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		config.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

		shutDownProcedure = () -> {
			try {
				Admin admin = ConnectionFactory.createConnection(config).getAdmin();
				admin.disableTable(TABLE_NAME);
				admin.deleteTable(TABLE_NAME);
			} catch (Exception e) {
				return e;
			}
			return null;
		};

		try {
			HTableDescriptor htd = initializeTable(config);
			this.region = initializeRegion(htd, config);

			ConnectionFactory.createConnection(config).getAdmin().createTable(htd);
			this.regionObserver = (MyRegionObserver) region.getCoprocessorHost()
					.findCoprocessor(MyRegionObserver.class.getName());
		} catch (Exception initializationException) {
			System.err.println("Error occurred in initialization:");
			initializationException.printStackTrace();
			System.err.println("Deleting test table");
			Exception shutDownException = shutDownProcedure.get();
			if (shutDownException != null) {
				System.err.println("Deletion error occurred:");
				shutDownException.printStackTrace();
				throw shutDownException;
			} else {
				throw initializationException;
			}
		}

	}

	@Override
	public void close() throws Exception {
		// Look, it's Go in Java!
		Exception e = shutDownProcedure.get();
		if (e != null) {
			throw e;
		}
	}

	public HRegion region() {
		return region;
	}

	public MyRegionObserver regionObserver() {
		return regionObserver;
	}

	private static HTableDescriptor initializeTable(Configuration config) throws IOException {
		Admin hbaseAdmin = ConnectionFactory.createConnection(config).getAdmin();
		if (!hbaseAdmin.tableExists(TABLE_NAME)) {
			HColumnDescriptor usersFamily = new HColumnDescriptor(FAMILY_NAME);
			HTableDescriptor tableDescriptor = new HTableDescriptor(TABLE_NAME);
			tableDescriptor.addFamily(usersFamily);
			return tableDescriptor;
		} else {
			return hbaseAdmin.getTableDescriptor(TABLE_NAME);
		}
	}

	private static HRegion initializeRegion(HTableDescriptor htd, Configuration config) throws Exception {
		HRegionInfo info = new HRegionInfo(TABLE_NAME, null, null, false);
		Path rootDir = new Path("\\");
		HRegion region = HRegion.createHRegion(info, rootDir, config, htd);
		htd.addCoprocessor(MyRegionObserver.class.getName());
		RegionCoprocessorHost host = new RegionCoprocessorHost(region, null, config);
		region.setCoprocessorHost(host);
		host.load(MyRegionObserver.class, Coprocessor.PRIORITY_HIGHEST, config);
		return region;
	}
}
