package com.gleasy.library.cloud.mq;

public class MqException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8462065566290322550L;

	public MqException(String msg){
		super(msg);
	}
	
	public MqException(String msg,Exception e){
		super(msg,e);
	}
	
	public MqException(Exception e){
		super(e);
	}	
	
}
