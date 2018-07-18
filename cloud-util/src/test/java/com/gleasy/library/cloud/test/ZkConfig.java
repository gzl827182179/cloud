package com.gleasy.library.cloud.test;

import com.gleasy.library.cloud.util.ZkFactory;
import com.gleasy.util.Config;

public class ZkConfig {
	public static void main(String[] args) throws Exception{
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
	
		//ZkFactory.setSchema("jobtest");
		ZkFactory.setAddress("locktest","192.168.0.11:2181");
		ZkFactory.setRoot("locktest", "/locktest");
		ZkFactory.setTimeout("locktest", 10000);
		System.out.println(ZkFactory.getAddress("locktest"));
		System.out.println(ZkFactory.getRoot("locktest"));
		System.out.println(ZkFactory.getTimeout("locktest"));
		System.exit(1);
	}
}
