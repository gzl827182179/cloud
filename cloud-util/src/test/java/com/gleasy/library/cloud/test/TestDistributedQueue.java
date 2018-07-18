package com.gleasy.library.cloud.test;

import org.apache.zookeeper.KeeperException;

import com.gleasy.library.cloud.util.DistributedQueueUtil;
import com.gleasy.util.Config;

public class TestDistributedQueue {
	public static void main(String[] args){
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
		final String schema = "locktest";
		
		new Thread(new Runnable(){
			@Override
			public void run() {		
				for(int i=0;i<100;i++){
					try {
						DistributedQueueUtil.getInstance(schema).offer("/locktest/dylanqueue", "a".getBytes());
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
					}
				}
			}			
		}).start();	
		
		for(int i=0;i<10000;i++){
			byte[] o = null;
			try {
				o = DistributedQueueUtil.getInstance(schema).take("/locktest/dylanqueue",1000);
				System.out.println(o);
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		

		System.exit(1);
	}
}
