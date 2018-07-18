package com.gleasy.library.cloud.mq.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.gleasy.library.redis.client.lock.RedisLockUtil;
import com.gleasy.library.redis.client.lock.RedisLockUtil.LockStat;

public class MqLockUtil{
	private static Logger logger = Logger.getLogger(MqLockUtil.class);
	
	private static Map<String,MqLockUtil> instances = new ConcurrentHashMap<String,MqLockUtil>();
	private static Object mutex = new Object();
	
	public static MqLockUtil getInstance(String schema){
		MqLockUtil u = instances.get(schema);
		if(u==null){
			synchronized(mutex){
				u = instances.get(schema);
				if(u == null){
					u = new MqLockUtil(schema);
					instances.put(schema, u);
				}
			}
		}
		return u;
	}
	
	private String schema;
	private String messageLockPrefix = "cloud:mq:lock:";
	private String partitionLockPrefix = "cloud:mq:msg:p:l:";
	private String subPartitionLockPrefix = "cloud:mq:msg:s:p:l:";
	
	private MqLockUtil(String schema){
		this.schema = schema;
	}
	
	public LockStat lockMessage(String topic, String uuid){
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		LockStat stat = RedisLockUtil.getInstance(redisSchema).lock(messageLockPrefix+topic+":"+uuid);
		return stat;	
	}
	
	public void unlockMessage(String topic, String uuid, LockStat stat) {
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		RedisLockUtil.getInstance(redisSchema).unlock(messageLockPrefix+topic+":"+uuid,stat);
	}
	
	public LockStat lockPartition(String topic, Long partition){
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		LockStat stat = RedisLockUtil.getInstance(redisSchema).lock(partitionLockPrefix+topic+":"+partition);
		return stat;	
	}
	
	public void unlockPartition(String topic, Long partition, LockStat stat) {
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		RedisLockUtil.getInstance(redisSchema).unlock(partitionLockPrefix+topic+":"+partition,stat);
	}
	
	
	public LockStat lockSubscriberPartition(String subscriber, String topic, Long partition){
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		LockStat stat = RedisLockUtil.getInstance(redisSchema).lock(subPartitionLockPrefix+":"+subscriber+":"+topic+":"+partition);
		return stat;	
	}
	
	public void unlockSubscriberPartition(String subscriber,String topic, Long partition, LockStat stat) {
		String redisSchema = MqConfigUtil.getInstance(schema).getConfig(topic).getMessageStoreSchema();
		RedisLockUtil.getInstance(redisSchema).unlock(subPartitionLockPrefix+":"+subscriber+":"+topic+":"+partition,stat);
	}
}
