package com.gleasy.library.cloud.mq.util;

public class ZkConfig {
	private String schema;
	public void setSchema(String schema){
		this.schema = schema;
	}
	public ZkConfig(String schema){
		setSchema(schema);
	}
	public String getSchema(){
		return schema;
	}
}
