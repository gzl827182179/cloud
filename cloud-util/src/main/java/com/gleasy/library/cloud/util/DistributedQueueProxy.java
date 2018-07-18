package com.gleasy.library.cloud.util;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;



public class DistributedQueueProxy  {
    private static final Logger logger =  Logger.getLogger(DistributedQueueProxy.class);
   
    private DistributedQueue queue;
    private String dir;
    private String schema;

    private final ExitListener exitListener = new ExitListener(){
		@Override
		public void execute() {
			initQueue();
		}
	};
	
	private synchronized void initQueue(){
		logger.debug("初始化queue...");
		
		queue = new DistributedQueue(dir,schema);
		
		if(queue != null && queue.zk != null){
			queue.zk.addExitListener(exitListener);
		}
	}
	
    public DistributedQueueProxy(String dir,String schema){
    	this.dir = dir;
    	this.schema = schema;
    	initQueue();
    }
    
	public byte[] take() throws KeeperException, InterruptedException{
		return queue.take();
	}

	
	public byte[] take(long timeout) throws KeeperException, InterruptedException{
		return queue.take(timeout);
	}
	
	public byte[] take(long timeout,boolean returnWhenSessionExpire) throws KeeperException, InterruptedException{
		return queue.take(timeout,returnWhenSessionExpire);
	}
	
	public void terminate(){
		queue.terminate();
	}
	
	public byte[] peek() throws KeeperException, InterruptedException{
		return queue.peek();
	}
	
	public byte[] poll() throws KeeperException, InterruptedException{
		return queue.poll();
	}
	
	public boolean offer(byte[] c) throws KeeperException, InterruptedException{
		return queue.offer(c);
	}	 
}
