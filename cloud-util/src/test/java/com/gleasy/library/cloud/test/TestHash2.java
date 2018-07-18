package com.gleasy.library.cloud.test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.gleasy.library.cloud.util.DistributedQueueUtil;
import com.gleasy.util.Util;

public class TestHash2 {
	private static Map<String,DistributedQueueUtil> instances = new HashMap();

	private static Object utilMutex = new Object();
	private static Object mutexLock = new Object();
	private static Map<String,Object> mutexLockMap = new HashMap();
	
	private static Object getMutex(String key){
		Object mx = mutexLockMap.get(key);
		if(mx == null){
			synchronized(mutexLock){
				mx = mutexLockMap.get(key);
				if(mx==null){
					mx = new Object();
					mutexLockMap.put(key,mx);
				}
			}
		}
		return mx;
		//return DistributedLockUtil.class.getName()+"."+key;
	}
	
	public static DistributedQueueUtil getInstance(String schema){
		DistributedQueueUtil u = instances.get(schema);
		if(u==null){
			synchronized(utilMutex){
				u = instances.get(schema);
				if(u == null){
					u = new DistributedQueueUtil(schema);
					instances.put(schema, u);
				}
			}
		}
		return u;
	}
	
	public static void main(String[] args){

       Executor executor = Executors.newFixedThreadPool(1000);
       final List<Long> timeList = new ArrayList();
      
       final int clients = 1000;
       final int total = 8000000;
       final CountDownLatch doneSignal = new CountDownLatch(clients);
       
       final String seed = Util.genUniqueId("dd");
       long start = System.currentTimeMillis();
       for(int i=0;i<clients;i++){
    	   executor.execute(new Runnable(){
				public void run() {	
					for(int k=0;k<total/clients;k++){
						String  j = seed + ":"+((int)Math.round(Math.random()*100000))+":"+seed;
						//System.out.println(j);
						DistributedQueueUtil gg = TestHash2.getInstance(j);
						//System.out.println(gg.hashCode());
						if(k%5 == 0){
							instances.remove(j);
						}
						
					}
					doneSignal.countDown();
				}
    	   });
       }
       
       
		try {
			doneSignal.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		long avg = 0;
		for(Long time : timeList){
			if(time == null){
				avg = avg + 0;
			}else{
				avg = avg + time;
			}
		}
		
		long end = System.currentTimeMillis();
		System.out.println("Hessian 总时间："+(avg)+"；平均:"+((double)avg/total));
		System.out.println("Hessian 实际总时间："+(end - start)+"; 平均："+((double)(end - start)/total));
		
		System.exit(1);
		
		
	}
}
