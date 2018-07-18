package com.gleasy.library.cloud.test;

import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;

import com.gleasy.library.cloud.util.DistributedLockUtil;
import com.gleasy.util.Config;

public class TestDistributedLock2 {
	public static void main(String[] args){
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
		final String key = "/locktest/dyla1n1";
		//final DistributedReentrantLock lock = new DistributedReentrantLock(key);
		
		int total = 100;
		final CountDownLatch doneSignal = new CountDownLatch(total);
		for(int i=0;i<total;i++){
			final int j = i;
			new Thread(new Runnable(){
				@Override
				public void run() {
					System.out.println("lock:"+j%3);
					DistributedLockUtil.LockStat stat = null;
					try {
						stat = DistributedLockUtil.getInstance("locktest").lock(key+j%3);
						
						System.out.println("ok:"+j+":"+stat);
						
						//Thread.sleep(300);
					}catch (Exception e) {
						e.printStackTrace();
						System.out.println(j+"加锁失败");
					}finally{
						try {
							System.out.println("unlock:"+j);
							DistributedLockUtil.getInstance("locktest").unlock(key+j%3,stat);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} catch (KeeperException e) {
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
