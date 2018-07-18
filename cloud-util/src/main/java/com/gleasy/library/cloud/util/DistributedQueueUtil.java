package com.gleasy.library.cloud.util;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.zookeeper.KeeperException;

import com.gleasy.util.ExitHandler;
import com.gleasy.util.SystemExitListener;

public class DistributedQueueUtil {
	
	
	private static Map<String,DistributedQueueUtil> instances = new ConcurrentHashMap();
	private static Map<String,Object> mutexLockMap = new ConcurrentHashMap();
	
	private static Object utilMutex = new Object();
	private static Object mutexLock = new Object();
	
	public static DistributedQueueUtil getInstance(String schema){
		DistributedQueueUtil u = instances.get(schema);
		if(u==null){
			synchronized(utilMutex){
				u = instances.get(schema);
				if(u == null){
					u = new DistributedQueueUtil(schema);
					instances.put(schema, u);
				}
			}
		}
		return u;
	}
	
	private Map<String,DistributedQueueProxy> cache  = new ConcurrentHashMap<String,DistributedQueueProxy>();

	private String schema;
	public DistributedQueueUtil(String schema){
		this.schema = schema;
		SystemExitListener.addListener(new ExitHandler(){
			public void run() {
				terminateAll();
			}
		});
	}
	
	private Object getMutex(String key){
		Object mx = mutexLockMap.get(key);
		if(mx == null){
			synchronized(mutexLock){
				mx = mutexLockMap.get(key);
				if(mx==null){
					mx = new Object();
					mutexLockMap.put(key,mx);
				}
			}
		}
		return mx;
	}
	
	private DistributedQueueProxy getQueue(String key){
		DistributedQueueProxy queue = cache.get(key);
		if(queue == null){
			synchronized(getMutex(key)){
				queue = cache.get(key);
				if(queue == null){
					queue = new DistributedQueueProxy(key,schema);
					cache.put(key, queue);
				}
			}
		}
		return queue;
	}
	
	public byte[] take(String key) throws KeeperException, InterruptedException{
		return getQueue(key).take();
	}

	
	public byte[] take(String key,long timeout) throws KeeperException, InterruptedException{
		return getQueue(key).take(timeout);
	}

	public byte[] take(String key,long timeout,boolean returnWhenSessionExpire) throws KeeperException, InterruptedException{
		return getQueue(key).take(timeout,returnWhenSessionExpire);
	}
	
	public byte[] waitTake(String key,long timeout,boolean returnWhenSessionExpire) throws KeeperException, InterruptedException{
		long p1 = System.currentTimeMillis();
		byte[] result = getQueue(key).take(timeout,returnWhenSessionExpire);
		if(result != null) return result;
		long ww = timeout - (System.currentTimeMillis() - p1);
		if(ww > 1000){
			Thread.sleep(ww);
		}
		return null;
	}
	
	public void terminate(String key){
		getQueue(key).terminate();
	}
	
	public synchronized void terminateAll(){
		Set<String> keyset = cache.keySet();
		for(String key : keyset){
			DistributedQueueProxy p = cache.get(key);
			if(p != null){
				p.terminate();
			}
		}
	}
	
	public byte[] peek(String key) throws KeeperException, InterruptedException{
		return getQueue(key).peek();
	}
	
	public byte[] poll(String key) throws KeeperException, InterruptedException{
		return getQueue(key).poll();
	}
	
	public boolean offer(String key,byte[] c) throws KeeperException, InterruptedException{
		return getQueue(key).offer(c);
	}	
}
