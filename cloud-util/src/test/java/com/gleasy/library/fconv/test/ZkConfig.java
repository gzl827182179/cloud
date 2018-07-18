package com.gleasy.library.fconv.test;

import com.gleasy.library.cloud.util.ZkFactory;
import com.gleasy.util.Config;

public class ZkConfig {
	public static void main(String[] args) throws Exception{
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
	
		//ZkFactory.setSchema("jobtest");
		ZkFactory.setAddress("jobtest","192.168.0.7:2181");
		ZkFactory.setRoot("jobtest", "/jobtest");
		
		System.out.println(ZkFactory.getAddress("jobtest"));
		System.out.println(ZkFactory.getRoot("jobtest"));
		System.exit(1);
	}
}
