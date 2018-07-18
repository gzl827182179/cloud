package com.gleasy.library.cloud.mq.domain;

public class DelayMessage extends Message{
	/**
	 * 
	 */
	private static final long serialVersionUID = -4362208090889639897L;
	private String uuid;//唯一标识本消息的id
	private Long latency;//延迟时间,ms为单位
	
	public DelayMessage(String uuid, byte[] body, long latency) {
		super("",body);
		this.uuid = uuid;
		this.latency = latency;
	}

	public String getUuid() {
		return uuid;
	}

	public void setUuid(String uuid) {
		this.uuid = uuid;
	}

	public Long getLatency() {
		return latency;
	}
	public void setLatency(Long latency) {
		this.latency = latency;
	}
}
