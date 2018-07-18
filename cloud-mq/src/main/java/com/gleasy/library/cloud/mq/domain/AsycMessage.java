package com.gleasy.library.cloud.mq.domain;

public class AsycMessage extends Message{
	private static final long serialVersionUID = -8069301649269049860L;
	
	private static long asycIdSeed = 0;
	private static Object mutex = new Object();
	
	private long asycId = 0;
	public AsycMessage(String topic,byte[] body){
		super(topic,body);
		synchronized(mutex){
			asycIdSeed++;
			asycId = asycIdSeed;
		}
	}
	public long getAsycId() {
		return asycId;
	}
	
}
