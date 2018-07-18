package com.gleasy.library.cloud.mq.util;


public class TopicConfig {
	public static String MODE_TOPIC = "topic";
	public static String MODE_QUEUE = "queue";
	
	private String messageStoreSchema;
	private String mode = "topic";//mode=[topic,queue]
	private int messageLifetime = 60*60*48;//topic消息存活时间,默认为48小时(即2天);如果mode=queue,此设置无效
	
	public String getMessageStoreSchema() {
		return messageStoreSchema;
	}
	public void setMessageStoreSchema(String messageStoreSchema) {
		this.messageStoreSchema = messageStoreSchema;
	}


	public String getMode() {
		return mode;
	}
	public void setMode(String mode) {
		this.mode = mode;
	}
	public int getMessageLifetime() {
		if(MODE_QUEUE.equals(mode)){
			return -1;
		}
		return messageLifetime;
	}
	public void setMessageLifetime(int messageLifetime) {
		this.messageLifetime = messageLifetime;
	}
	
}
