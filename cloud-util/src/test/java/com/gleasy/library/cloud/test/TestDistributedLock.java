package com.gleasy.library.cloud.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.gleasy.library.cloud.util.DistributedLockUtil;
import com.gleasy.util.Config;

public class TestDistributedLock {
	public static void main(String[] args){
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
		final String key = "/locktest/dyla1n";
		//final DistributedReentrantLock lock = new DistributedReentrantLock(key);
		
		int total = 40;
		final CountDownLatch doneSignal = new CountDownLatch(total);
		for(int i=0;i<total;i++){
			final int j = i;
			new Thread(new Runnable(){
				@Override
				public void run() {
					System.out.println(j+"start");
					DistributedLockUtil.LockStat stat = null;
					try {
						stat = DistributedLockUtil.getInstance("locktest").lock(key);
					}catch (Exception e) {
						e.printStackTrace();
						System.out.println(j+"加锁失败");
					}
					if(stat != null){
						for(int k = 0; k< 10;k++){
							System.out.println(j+":"+k);
						}
						
						try {
							Thread.sleep(15000);
						} catch (InterruptedException e1) {
						}
						try {
							DistributedLockUtil.getInstance("locktest").unlock(key,stat);
						} catch (KeeperException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
					doneSignal.countDown();
				}			
			}).start();		
		}
		try {
			doneSignal.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.exit(1);
	}
}
