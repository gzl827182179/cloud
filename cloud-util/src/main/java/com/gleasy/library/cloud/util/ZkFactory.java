package com.gleasy.library.cloud.util;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import com.gleasy.util.Config;
import com.gleasy.util.ExitHandler;
import com.gleasy.util.SystemExitListener;
import com.gleasy.util.Util;

public class ZkFactory {
	private static Map<String,ZooKeeperRetry> zkMap = new ConcurrentHashMap<String,ZooKeeperRetry>();
	private static Map<String,ZooKeeperRetry> newZkMap = new ConcurrentHashMap<String,ZooKeeperRetry>();
	private static Object mutex = new Object();
	private static Object newMutex = new Object();

	static{
		SystemExitListener.addTerminateListener(new ExitHandler(){
			public void run() {
				Set<String> keyset = zkMap.keySet();
				for(String schema : keyset){
					ZooKeeperRetry zk = zkMap.get(schema);
					if(zk != null && !zk.isClosed()){
						try {
							zk.close();
						} catch (InterruptedException e) {
						}
					}
				}
				
				keyset = newZkMap.keySet();
				for(String schema : keyset){
					ZooKeeperRetry zk = newZkMap.get(schema);
					if(zk != null && !zk.isClosed()){
						try {
							zk.close();
						} catch (InterruptedException e) {
						}
					}
				}
			}
		});
	}
	
	public synchronized static ZooKeeperRetry getZk(String schema) throws IOException, CloudException{
		return getZk(schema,false);
	}

	public static ZooKeeperRetry getZk(String schema,boolean forceNew) throws IOException, CloudException{
		if(schema == null){
			throw new IOException("schema未设置");
		}
		ZooKeeperRetry zk = zkMap.get(schema);
		
		if(zk == null){
			synchronized(mutex){
				zk = zkMap.get(schema);
				if(zk == null){
					zk = createAndConnectZk(schema);
					zkMap.put(schema, zk);
				}
			}
		}
		
		if(forceNew){
			if(zk == null || zk.isClosed()){
				return reCreateZk(schema);
			}
		}
		
		return zk;
	}
	
	private static ZooKeeperRetry reCreateZk(String schema) throws IOException, CloudException{
		if(schema == null){
			throw new IOException("schema未设置");
		}
		ZooKeeperRetry zk = newZkMap.get(schema);		
		if(zk == null || zk.isClosed()){
			synchronized(newMutex){
				zk = newZkMap.get(schema);
				if(zk == null || zk.isClosed()){
					zk = createAndConnectZk(schema);
					newZkMap.put(schema, zk);
				}
			}
		}
		return zk;
	}
	
	private static Watcher emptyWather = new Watcher(){
		@Override
		public void process(WatchedEvent arg0) {
		}
	};
	
	private static ZooKeeperRetry createAndConnectZk(String schema) throws CloudException, IOException{
		ZooKeeperRetry zk = new ZooKeeperRetry(getAddress(schema),getTimeout(schema),emptyWather);
		if(!zk.waitForConnect(3*60*1000)){
			throw new CloudException("无法连接上zk");
		}
		return zk;
	}
	
	public static String getAddress(String schema) throws CloudException{
		if(schema == null){
			throw new CloudException("schema未设置");
		}
		return Config.get("zk:addr:"+schema);
	}
	
	public static void setAddress(String schema,String address) throws CloudException{
		if(schema == null){
			throw new CloudException("schema未设置");
		}
		Config.set("zk:addr:"+schema,address);
	}

	public static String getRoot(String schema) throws CloudException{
		if(schema == null){
			throw new CloudException("schema未设置");
		}
		return Config.get("zk:root:"+schema);
	}
	
	public static void setRoot(String schema,String root){
		Config.set("zk:root:"+schema,root);
	}	
	
	public static int getTimeout(String schema) throws CloudException{
		if(schema == null){
			throw new CloudException("schema未设置");
		}
		String to = Config.get("zk:timeout:"+schema);
		if(Util.isEmpty(to)){
			return 30000;
		}
		int t =  Util.parseInt(to);
/*		if(t < 30000){
			t = 30000;
		}*/
		return t;
	}
	
	public static void setTimeout(String schema,int to) throws CloudException{
		if(schema == null){
			throw new CloudException("schema未设置");
		}
		Config.set("zk:timeout:"+schema,""+to);
	}	
}
