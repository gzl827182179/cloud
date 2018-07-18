package com.gleasy.library.cloud.mq.util;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.gleasy.library.redis.client.GlobalRedisClient;
import com.gleasy.library.redis.client.LocalRedisClient;
import com.gleasy.library.redis.shard.ConfigLoader;
import com.gleasy.library.redis.shard.ShardCluster;
import com.gleasy.util.Util;

public class RedisFactory {
	private static Map<String,GlobalRedisClient> messageGlobalClient = new ConcurrentHashMap();
	private static Map<String,LocalRedisClient> messageLocalClient = new ConcurrentHashMap();
	//private static Map<String,LocalRedisClient> topicLocalClient = new HashMap();
	
	private static Map<String,Integer> partitionMap = new ConcurrentHashMap();
	
	private static Map<String,Object> lockMap = new ConcurrentHashMap();
	private static Object lock = new Object();
	
	private static Object getSchemLock(String topic){
		Object schemaLock  = lockMap.get(topic);
		if(schemaLock == null){
			synchronized(lock){
				schemaLock = lockMap.get(topic);
				if(schemaLock == null){
					schemaLock = new Object();
					lockMap.put(topic,schemaLock);
				}
			}
		}
		return lock;
	}
	
	public static GlobalRedisClient getMessageGlobalClient(String zkSchema, String topic){
		GlobalRedisClient c = messageGlobalClient.get(topic);
		if(c != null) return c;
		synchronized(getSchemLock(topic)){
			c = messageGlobalClient.get(topic);
			if(c != null) return c;
			c = new GlobalRedisClient(MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMessageStoreSchema());
			messageGlobalClient.put(topic, c);
			return c;
		}
	}
	public synchronized static LocalRedisClient getMessageLocalClient(String zkSchema,String topic){
		LocalRedisClient c = messageLocalClient.get(topic);
		if(c != null) return c;
		synchronized(getSchemLock(topic)){
			c = messageLocalClient.get(topic);
			if(c != null) return c;
			c = new LocalRedisClient(MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMessageStoreSchema());
			messageLocalClient.put(topic, c);
			return c;
		}
	}
	
/*	public synchronized static LocalRedisClient getTopicLocalClient(String topic){
		if(topicLocalClient.get(topic) != null) return topicLocalClient.get(topic);
		synchronized(getSchemLock(topic)){
			if(topicLocalClient.get(topic) != null) return topicLocalClient.get(topic);
			LocalRedisClient c = new LocalRedisClient(MqConfigUtil.getInstance().getConfig(topic).getTopicStoreSchema());
			topicLocalClient.put(topic, c);
			return c;
		}
	}
	
	public static int getTopicPartitionNum(String topic){
		List<ShardCluster> clusters = ConfigLoader.getInstance().getClusters(MqConfigUtil.getInstance().getConfig(topic).getTopicStoreSchema());
		if(Util.isEmpty(clusters)) return 0;
		
		Long max = 1l;
		for(ShardCluster cluster:clusters){
			if(cluster.getMax() > max){
				max = cluster.getMax();
			}
		}
		return max.intValue();
	}*/
	
	public static int getMessagePartitionNum(String zkSchema,String topic){
		Integer p = partitionMap.get(topic);
		if(p != null){
			return p;
		}
		
		synchronized(getSchemLock(topic)){
			p = partitionMap.get(topic);
			if(p != null){
				return p;
			}
			
			List<ShardCluster> clusters = ConfigLoader.getInstance().getClusters(MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMessageStoreSchema());
			if(Util.isEmpty(clusters)) return 0;
			
			Long max = 1l;
			for(ShardCluster cluster:clusters){
				if(cluster.getMax() > max){
					max = cluster.getMax();
				}
			}
			p = max.intValue();
			partitionMap.put(topic, p);
			return p;
		}
	}
}
