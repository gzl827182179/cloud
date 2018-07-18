package com.gleasy.library.cloud.mq.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.gleasy.library.cloud.mq.MqException;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;

public class MqConfigUtil {
	private static final Logger logger =  Logger.getLogger(MqConfigUtil.class);
	private static MqConfigUtil obj = new MqConfigUtil();
	public static MqConfigUtil getInstance(String schema){
		return obj;
	}

	public static MqConfigUtil getInstance(){
		return obj;
	}
	
	private Map<String,TopicConfig> topicConfigCache = new ConcurrentHashMap();
	private Map<String,Boolean> topicDoing = new ConcurrentHashMap();
	private String keyprefix = "cloud:mq:conf:";
	private Object mutex = new Object();
	private MqConfigUtil(String schema) {
	}
	private MqConfigUtil() {
	}
	public TopicConfig getConfig(String topic) throws MqException{
		if(topicConfigCache.get(topic) != null){
			return topicConfigCache.get(topic);
		}
		doLoadConfig(topic);
		return topicConfigCache.get(topic);
	}
	
	
	private void doLoadConfig(String topic) throws MqException{
		synchronized(mutex){
			Boolean d = topicDoing.get(topic);
			if(d!=null  && d) return;
			topicDoing.put(topic, true);
		}
		logger.info("reloading config:"+topic);
		String key = keyprefix+topic;
		try {
			String v = Config.get(key);
			TopicConfig c = SerializationUtil.jsonDeserialize(v, TopicConfig.class);
			logger.info(c);
			topicConfigCache.put(topic, c);
		} catch (Exception e) {
        	logger.warn("Caught: " + e, e);
        	throw new MqException("加载topic配置失败");
        } finally{
        	topicDoing.put(topic, false);
        }
	}
	
	public void configTopic(String topic, TopicConfig config){
		String key = keyprefix+topic;
		String data;
		try {
			data = SerializationUtil.jsonSerialize(config);
			Config.set(key,data);
			System.out.println("写入 : "+key +" 成功");
		}catch (Exception e) {
        	logger.warn("Caught: " + e, e);
        	System.out.println("写入 : "+key +" 失败");
        	throw new MqException("设置topic配置失败");
        }
	}
}

