package com.gleasy.library.cloud.util;

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.gleasy.util.SystemExitListener;

public class ZkPrimative implements Watcher {
	private static final Logger logger = Logger.getLogger(ZkPrimative.class);

	protected ZooKeeperRetry zk = null;
	protected List<ACL> acl = ZooDefs.Ids.OPEN_ACL_UNSAFE;
	protected String root;
	protected static String nodeName = "";
	protected String schema;
	private boolean inited = false;

	public ZkPrimative() {
	}

	public ZkPrimative(String schema) {
		this.schema = schema;
		initZk(false);
		initRoot();
	}

	public ZkPrimative(String schema, boolean forceNew) {
		this.schema = schema;
		initZk(forceNew);
		initRoot();
	}

	protected void initZk(boolean forceNew) {
		if (inited)
			return;
		if (SystemExitListener.isOver()) {
			zk = null;
			return;
		}
		zk = null;
		try {
			zk = ZkFactory.getZk(schema, forceNew);
			initHook();
			return;
		} catch (IOException e) {
			logger.error("连接zookeeper失败:" + e, e);
		} catch (CloudException e) {
			logger.error("连接zookeeper失败:" + e, e);
		}
	}

	protected void initHook() {

	}

	protected void initRoot() {
		if (zk == null)
			return;
		if (inited)
			return;
		try {
			root = ZkFactory.getRoot(schema);
			ensurePathExists(root);
		} catch (CloudException e) {
			logger.error("error:" + e, e);
		}
		nodeName = System.getProperty("nodeName", nodeName);
		if (nodeName == null) {
			nodeName = System.getenv("nodeName");
		}
	}

	public void process(WatchedEvent arg0) {

	}

	/**
	 * Ensures that the given path exists with no data, the current ACL and no
	 * flags
	 * 
	 * @param path
	 */
	protected void ensurePathExists(String path) {
		ensureExists(path, null, acl, CreateMode.PERSISTENT);
	}

	/**
	 * Ensures that the given path exists with no data, the current ACL and no
	 * flags
	 * 
	 * @param path
	 */
	protected void ensurePathExists(String path, final CreateMode flags) {
		ensureExists(path, null, acl, flags);
	}

	/**
	 * Ensures that the given path exists with the given data, ACL and flags
	 * 
	 * @param path
	 * @param acl
	 * @param flags
	 */
	protected void ensureExists(final String path, final byte[] data, final List<ACL> acl, final CreateMode flags) {
		try {
			Stat stat = zk.exists(path, false);
			if (stat != null) {
				return;
			}
			zk.create(path, data, acl, flags);
			return;
		} catch (KeeperException e) {
			logger.warn("Caught: " + e, e);
		} catch (InterruptedException e) {
			logger.warn("Caught: " + e, e);
		}
	}
}
