package com.gleasy.library.fconv.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.gleasy.library.cloud.util.DistributedLockUtil;
import com.gleasy.library.cloud.util.ZkFactory;

public class TestDistributedLock {
	public static void main(String[] args){
		//ZkFactory.setSchema("jobtest");
		final String key = "/root/dylan";
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
						stat = DistributedLockUtil.getInstance("jobtest").lock(key);
					}catch (Exception e) {
						e.printStackTrace();
					}
					for(int k = 0; k< 10;k++){
						System.out.println(j+":"+k);
					}
					try {
						DistributedLockUtil.getInstance("jobtest").unlock(key,stat);
					} catch (KeeperException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
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
