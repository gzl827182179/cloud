package com.gleasy.library.cloud.util;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;



public class DistributedQueue extends ZkPrimative {
    private static final Logger logger =  Logger.getLogger(DistributedQueue.class);
    
    private final String dir;
    private final String prefix = "qn-";
    private final long timeout = -1;
    private final Integer mutex = new Integer(1);
    
    public DistributedQueue(String dir,String schema){
    	super(schema,true);
        this.dir = dir;
    }
    
    /**
     * Returns a Map of the children, ordered by id.
     * @param watcher optional watcher on getChildren() operation.
     * @return map from id to child name for all children
     */
    private TreeMap<Long,String> orderedChildren(Watcher watcher) throws KeeperException, InterruptedException {
        TreeMap<Long,String> orderedChildren = new TreeMap<Long,String>();
        if(zk == null || zk.isClosed()){
    		return orderedChildren;
    	}
        
        List<String> childNames = null;
        try{
            childNames = zk.getChildren(dir, watcher);
        }catch (KeeperException.NoNodeException e){
            throw e;
        }

        for(String childName : childNames){
            try{
                //Check format
                if(!childName.regionMatches(0, prefix, 0, prefix.length())){
                    logger.warn("Found child node with improper name: " + childName);
                    continue;
                }
                String suffix = childName.substring(prefix.length());
                Long childId = new Long(suffix);
                orderedChildren.put(childId,childName);
            }catch(NumberFormatException e){
            	logger.warn("Found child node with improper format : " + childName + " " + e,e);
            }
        }

        return orderedChildren;
    }

    /**
     * Return the head of the queue without modifying the queue.
     * @return the data at the head of the queue.
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    private byte[] element() throws NoSuchElementException, KeeperException, InterruptedException {
    	if(zk == null || zk.isClosed()  || terminated){
     		return null;
     	}
    	synchronized(mutex){
	        TreeMap<Long,String> orderedChildren;
	
	        // element, take, and remove follow the same pattern.
	        // We want to return the child node with the smallest sequence number.
	        // Since other clients are remove()ing and take()ing nodes concurrently, 
	        // the child with the smallest sequence number in orderedChildren might be gone by the time we check.
	        // We don't call getChildren again until we have tried the rest of the nodes in sequence order.
	        while(true){
	            try{
	                orderedChildren = orderedChildren(null);
	            }catch(KeeperException.NoNodeException e){
	                throw new NoSuchElementException();
	            }
	            if(orderedChildren.size() == 0 ) throw new NoSuchElementException();
	
	            for(String headNode : orderedChildren.values()){
	                if(headNode != null){
	                    try{
	                        return zk.getData(dir+"/"+headNode, false, null);
	                    }catch(KeeperException.NoNodeException e){
	                        //Another client removed the node first, try next
	                    }
	                }
	            }
	
	        }
    	}
    }


    /**
     * Attempts to remove the head of the queue and return it.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    private byte[] remove() throws NoSuchElementException, KeeperException, InterruptedException {
    	if(zk == null || zk.isClosed() || terminated){
     		return null;
     	}
    	synchronized(mutex){
        TreeMap<Long,String> orderedChildren;
        // Same as for element.  Should refactor this.
        while(true){
            try{
                orderedChildren = orderedChildren(null);
            }catch(KeeperException.NoNodeException e){
                throw new NoSuchElementException();
            }
            if(orderedChildren.size() == 0) throw new NoSuchElementException();

            for(String headNode : orderedChildren.values()){
                String path = dir +"/"+headNode;
                try{
                    byte[] data = zk.getData(path, false, null);
                    zk.delete(path, -1);
                    return data;
                }catch(KeeperException.NoNodeException e){
                    // Another client deleted the node first.
                }
            }

        }
    	}
    }

    private class LatchChildWatcher implements Watcher {

        CountDownLatch latch;

        public LatchChildWatcher(){
            latch = new CountDownLatch(1);
        }

        public void process(WatchedEvent event){
        	logger.debug("Watcher fired on path: " + event.getPath() + " state: " + 
                    event.getState() + " type " + event.getType());
            latch.countDown();
        }
        
        public void notice(){
        	latch.countDown();
        }
        public void await(long time, TimeUnit unit) throws InterruptedException {
        	if(terminated) return;
            latch.await(time,unit);
        }        
    }

    /**
     * Removes the head of the queue and returns it, blocks until it succeeds.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] take() throws KeeperException, InterruptedException {
        return take(timeout,true);
    }

    /**
     * Removes the head of the queue and returns it, blocks until it succeeds.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] take(long timeout) throws KeeperException, InterruptedException {
        return take(timeout,false);
    }
    
    private boolean terminated = false;
    
    public void terminate(){
    	terminated = true;
    	if(childWatcher!=null){
    		childWatcher.notice();
    	}
    }
    
    LatchChildWatcher childWatcher = null;
    
    /**
     * Removes the head of the queue and returns it, blocks until it succeeds.
     * @return The former head of the queue
     * @throws NoSuchElementException
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] take(long timeout,boolean returnWhenSessionExpire) throws KeeperException, InterruptedException {
    	if(zk == null || zk.isClosed() || terminated){
     		return null;
     	}
    	synchronized(mutex){
        TreeMap<Long,String> orderedChildren;
        // Same as for element.  Should refactor this.
        long start = System.currentTimeMillis();
        while(!terminated){
        	childWatcher = new LatchChildWatcher();
            try{
                orderedChildren = orderedChildren(childWatcher);
            }catch(KeeperException.NoNodeException e){
            	zk.create(dir, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                continue;
            }
            if(orderedChildren.size() == 0){
            	
            	if(returnWhenSessionExpire && (zk == null || zk.isClosed())){
            		return null;
            	}
            	
            	long interval = System.currentTimeMillis() - start;
            	long sleep = 10*1000;
            	if(timeout != -1){
            		if(interval > timeout) {
            			return null;
            		}else{
            			long tt = timeout - interval;
            			if(tt < sleep){
            				sleep = tt;
            			}
            		}
            	}
                childWatcher.await(sleep,TimeUnit.MILLISECONDS);
                if(terminated) return null;
                continue;
            }

            for(String headNode : orderedChildren.values()){
                String path = dir +"/"+headNode;
                try{
                    byte[] data = zk.getData(path, false, null);
                    zk.delete(path, -1);
                    return data;
                }catch(KeeperException.NoNodeException e){
                    // Another client deleted the node first.
                }
            }
        }
        return null;
    	}
    }
    
    /**
     * Inserts data into queue.
     * @param data
     * @return true if data was successfully added
     */
    public boolean offer(byte[] data) throws KeeperException, InterruptedException{
        if(zk == null || zk.isClosed()){
    		return false;
    	}
        for(;;){
            try{
                zk.create(dir+"/"+prefix, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return true;
            }catch(KeeperException.NoNodeException e){
            	zk.create(dir, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        }
    }

    /**
     * Returns the data at the first element of the queue, or null if the queue is empty.
     * @return data at the first element of the queue, or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] peek() throws KeeperException, InterruptedException{
        try{
            return element();
        }catch(NoSuchElementException e){
            return null;
        }
    }


    /**
     * Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
     * @return Head of the queue or null.
     * @throws KeeperException
     * @throws InterruptedException
     */
    public byte[] poll() throws KeeperException, InterruptedException {
        try{
            return remove();
        }catch(NoSuchElementException e){
            return null;
        }
    }



}
