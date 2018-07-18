package com.gleasy.library.cloud.mq.domain;

import java.io.Serializable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.gleasy.library.cloud.mq.MqException;



public class Message implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4256351839693655538L;
	
	protected Long id;
	protected String topic;
	protected byte[] body;
	protected Long partition;
	private long tid;
	protected CountDownLatch retCountdown = new CountDownLatch(1);
	
	public Message(String topic,byte[] body){
		this.topic = topic;
		this.body = body;
	}
	public Long getId() {
		return id;
	}
	public void setId(Long id) {
		this.id = id;
	}
	public String getTopic() {
		return topic;
	}
	public void setTopic(String topic) {
		this.topic = topic;
	}

	public byte[] getBody() {
		return body;
	}
	public void setBody(byte[] body) {
		this.body = body;
	}
	
	
	public long getTid() {
		return tid;
	}
	public void setTid(long tid) {
		this.tid = tid;
	}
	public Long getPartition() {
		return partition;
	}
	public void setPartition(Long partition) {
		this.partition = partition;
	}
	
	public void notice(){
		retCountdown.countDown();
	}
	
	public void waitResult(long seconds){
		try {
			boolean res = retCountdown.await(seconds, TimeUnit.SECONDS);
			if(!res){
				throw new MqException("保存消息超时");
			}
		} catch (InterruptedException e) {
		}
	}
	@Override
	public String toString() {
		return "Message [id=" + id + ", topic=" + topic + ", partition=" + partition + "]";
	}


}
