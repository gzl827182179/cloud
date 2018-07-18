package com.gleasy.library.cloud.mq.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;

import com.gleasy.library.cloud.mq.MqException;
import com.gleasy.library.cloud.util.ZkPrimative;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;


public class MqConfigUtil2 extends ZkPrimative{
	private static final Logger logger =  Logger.getLogger(MqConfigUtil2.class);
	
	private static Map<String,MqConfigUtil2> instances = new ConcurrentHashMap<String,MqConfigUtil2>();
	private static Object mmutex = new Object();
	
	public static MqConfigUtil2 getInstance(String schema){
		MqConfigUtil2 u = instances.get(schema);
		if(u==null){
			synchronized(mmutex){
				u = instances.get(schema);
				if(u == null){
					u = new MqConfigUtil2(schema);
					instances.put(schema, u);
				}
			}
		}
		return u;
	}

	
	private String configPath;	
	private Map<String,TopicConfig> topicConfigCache = new ConcurrentHashMap();
	private Map<String,Boolean> topicDoing = new ConcurrentHashMap();
	private Object mutex = new Object();
	
	private MqConfigUtil2(String schema) {
		super(schema);
		
		configPath = root + "/mqconfig";
		ensurePathExists(configPath);
	}

	public void process(WatchedEvent event) {
		if(event.getPath()!=null && event.getPath().startsWith(configPath)){
			String topic = event.getPath().substring((configPath+"/").length());
			doLoadConfig(topic);
		}
	}
	
	public TopicConfig getConfig(String topic) throws MqException{
		if(topicConfigCache.get(topic) != null){
			return topicConfigCache.get(topic);
		}
		doLoadConfig(topic);
		return topicConfigCache.get(topic);
	}
	
	private void watchTopicConfig(String topic){
		String topicConfigPath = configPath + "/" +topic;
		try {
			zk.getData(topicConfigPath, this, null);
		} catch (KeeperException e) {
        	logger.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
        	logger.warn("Caught: " + e, e);
        }
	}
	
	private void doLoadConfig(String topic) throws MqException{
		try{
			synchronized(mutex){
				Boolean d = topicDoing.get(topic);
				if(d!=null  && d) return;
				topicDoing.put(topic, true);
			}
			logger.info("reloading config:"+topic);
			String topicConfigPath = configPath + "/" +topic;
			try {
				if(zk.exists(topicConfigPath, null) == null){
					throw new MqException("加载topic配置失败,topic配置不存在");
				}
				byte[] config = zk.getData(topicConfigPath, false, null);
				if(config == null){
					throw new MqException("加载topic配置失败,topic配置不正确");
				}
				try {
					TopicConfig c = SerializationUtil.jsonDeserialize(new String(config), TopicConfig.class);
					logger.info(c);
					topicConfigCache.put(topic, c);
				} catch (Exception e) {
					throw new MqException("加载topic配置失败,topic配置不正确");
				}
			} catch (KeeperException e) {
	        	logger.warn("Caught: " + e, e);
	        	throw new MqException("加载topic配置失败");
	        } catch (InterruptedException e) {
	        	logger.warn("Caught: " + e, e);
	        	throw new MqException("加载topic配置失败");
	        } finally{
	        	topicDoing.put(topic, false);
	        }
		}finally{
			watchTopicConfig(topic);
		}
	}
	
	
	public void configTopic(String topic, TopicConfig config){
		String topicConfigPath = configPath + "/" +topic;
		ensurePathExists(topicConfigPath);
		String data;
		try {
			data = SerializationUtil.jsonSerialize(config);
			zk.setData(topicConfigPath, data.getBytes(), -1);
		}catch (Exception e) {
        	logger.warn("Caught: " + e, e);
        	throw new MqException("设置topic配置失败");
        }
	}
}
