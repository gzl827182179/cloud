package com.gleasy.library.cloud.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import com.gleasy.util.SystemExitListener;

/**
 * A Class which extends {@link ZooKeeper} and will automatically retry calls to
 * zookeeper if a {@link KeeperException.ConnectionLossException} occurs
 */
public class ZooKeeperRetry extends ZooKeeper {
	private static final Logger logger =  Logger.getLogger(ZooKeeperRetry.class);
	
    private boolean closed = false;
    private final Watcher watcher;
    private int limit = 20;
    private Object mutex = new Object();
    private List<ExitListener> listeners = new ArrayList();
    private final ExecutorService executor = Executors.newFixedThreadPool(100,new ThreadFactory() {
        public Thread newThread(Runnable r) {
            Thread s = Executors.defaultThreadFactory().newThread(r);
            s.setDaemon(true);
            return s;
        }
    });
    
    public void addExitListener(ExitListener listener){
    	synchronized(mutex){
    		if(listeners.contains(listener)) return;
    		listeners.add(listener);
    	}
    }
    
    private void notifyExit(){
    	if(SystemExitListener.isOver()) return;
    	try {
			close();
		} catch (InterruptedException e1) {
		}
		
    	synchronized(mutex){
	    	for(final ExitListener listener : listeners){
	    		executor.execute(new Runnable(){
					@Override
					public void run() {
						try{
			    			listener.execute();
			    		}catch(Exception e){
			    		}
					}
	    		});
	    	}
	    	listeners.clear();
    	}
    }
    
    /**
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @throws IOException
     */
    public ZooKeeperRetry(String connectString, int sessionTimeout,
            Watcher watcher) throws IOException {
        super(connectString, sessionTimeout, watcher);
        this.watcher = watcher;
    }

    /**
     * @param connectString
     * @param sessionTimeout
     * @param watcher
     * @param sessionId
     * @param sessionPasswd
     * @throws IOException
     */
    public ZooKeeperRetry(String connectString, int sessionTimeout,
            Watcher watcher, long sessionId, byte[] sessionPasswd)
            throws IOException {
        super(connectString, sessionTimeout, watcher, sessionId, sessionPasswd);
        this.watcher = watcher;
    }

    @Override
    public synchronized void close() throws InterruptedException {
    	if(closed) return;
        this.closed = true;
        super.close();
        notifyExit();
        executor.shutdownNow();
    }

    @Override
    public String create(String path, byte[] data, List<ACL> acl,
            CreateMode createMode) throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.create(path, data, acl, createMode);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
                if (exists(path, false) != null) {
                    return path;
                }
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            } catch (KeeperException.NodeExistsException e) {
                return path;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public void delete(String path, int version) throws InterruptedException,
            KeeperException {
        int count = 0;
        do {
            try {
                super.delete(path, version);
                return;
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
                if (exists(path, false) == null) {
                    return;
                }
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            } catch (KeeperException.NoNodeException e) {
                return;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
    }

    @Override
    public Stat exists(String path, boolean watch) throws KeeperException,
            InterruptedException {
        int count = 0;
        do {
            try {
                return super.exists(path, watch ? watcher : null);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public Stat exists(String path, Watcher watcher) throws KeeperException,
            InterruptedException {
        int count = 0;
        do {
            try {
                return super.exists(path, watcher);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public List<ACL> getACL(String path, Stat stat) throws KeeperException,
            InterruptedException {
        int count = 0;
        do {
            try {
                return super.getACL(path, stat);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public List<String> getChildren(String path, boolean watch)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.getChildren(path, watch ? watcher : null);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return new ArrayList<String>();
    }

    @Override
    public List<String> getChildren(String path, Watcher watcher)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.getChildren(path, watcher);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return new ArrayList<String>();
    }

    @Override
    public byte[] getData(String path, boolean watch, Stat stat)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.getData(path, watch ? watcher : null, stat);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public byte[] getData(String path, Watcher watcher, Stat stat)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.getData(path, watcher, stat);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public Stat setACL(String path, List<ACL> acl, int version)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.setACL(path, acl, version);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
                Stat s = exists(path, false);
                if (s != null) {
                    if (getACL(path, s).equals(acl)) {
                        return s;
                    }
                } else {
                    return null;
                }
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    @Override
    public Stat setData(String path, byte[] data, int version)
            throws KeeperException, InterruptedException {
        int count = 0;
        do {
            try {
                return super.setData(path, data, version);
            } catch (KeeperException.ConnectionLossException e) {
                logger.warn(
                        "ZooKeeper connection lost.  Trying to reconnect.");
                Stat s = exists(path, false);
                if (s != null) {
                    if (getData(path, false, s) == data) {
                        return s;
                    }
                } else {
                    return null;
                }
            }catch (KeeperException.SessionExpiredException e) {
            	notifyExit();
            	throw e;
            }
        } while (!closed && (limit == -1 || count++ < limit));
        close();
        return null;
    }

    /**
     * @param limit
     */
    public void setRetryLimit(int limit) {
        this.limit = limit;
    }

    public boolean isClosed(){
    	return closed;
    }
    /**
     * @return true if successfully connected to zookeeper
     */
    public boolean testConnection() {
        int count = 0;
        do {
            try {
                return super.exists("/", null) != null;
            } catch (Exception e) {
               
            }
        } while (count++ < 5);
        return false;
    }

    
    public boolean waitForConnect(long ms){
    	if(closed) return false;
    	logger.info("等待连接啊....");
    	long p1 = System.currentTimeMillis();
    	while(!testConnection()){
    		long t = System.currentTimeMillis() - p1;
    		if(t > ms ){
    			try {
					this.close();
				} catch (InterruptedException e) {
				}
    			return false;
    		}
    		logger.info("等待连接重试....");
    		try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
			}
    	}
    	return true;
    }
}
	