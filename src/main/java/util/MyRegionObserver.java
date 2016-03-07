package util;

import java.io.IOException;
import java.util.function.Function;

import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * This is a simple region observer with two counters. Every time prePut is
 * called, putsIn is incremented. Every time postPut is called, putsOut is
 * incremented. Additionally, a bypass function can be set to selectively bypass
 * puts based on some predicate.
 * 
 * @author Harry Harpham
 * 
 */
public class MyRegionObserver extends BaseRegionObserver {
	private int prePutCalls = 0;
	private int postPutCalls = 0;

	// Behavior: if function.apply(put) evaluates to true, the put will be
	// bypassed. We initially set this to never bypass anything.
	private Function<Put, Boolean> bypassFn = p -> false;

	@Override
	public void prePut(final ObserverContext<RegionCoprocessorEnvironment> ctx, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {
		prePutCalls++;
		if (bypassFn.apply(put)) {
			ctx.bypass();
		}
	}

	@Override
	public void postPut(final ObserverContext<RegionCoprocessorEnvironment> ctx, final Put put, final WALEdit edit,
			final Durability durability) throws IOException {
		postPutCalls++;
	}

	public void setBypassFunction(Function<Put, Boolean> bypassFunction) {
		this.bypassFn = bypassFunction;
	}

	public int prePutCalls() {
		return prePutCalls;
	}

	public int postPutCalls() {
		return postPutCalls;
	}
}
