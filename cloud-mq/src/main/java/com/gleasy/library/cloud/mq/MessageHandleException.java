package com.gleasy.library.cloud.mq;

public class MessageHandleException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8462065566290322550L;

	public MessageHandleException(String msg){
		super(msg);
	}
	
	public MessageHandleException(String msg,Exception e){
		super(msg,e);
	}
	
	public MessageHandleException(Exception e){
		super(e);
	}	
	
}
