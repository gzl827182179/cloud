package com.gleasy.library.cloud.util;

public class CloudException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -8462065566290322550L;

	public CloudException(String msg){
		super(msg);
	}
	
	public CloudException(String msg,Exception e){
		super(msg,e);
	}
	
	public CloudException(Exception e){
		super(e);
	}	
	
}
