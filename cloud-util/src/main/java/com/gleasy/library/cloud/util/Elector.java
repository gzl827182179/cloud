package com.gleasy.library.cloud.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import com.gleasy.util.Util;

/**
 * 选举器
 *
 */
public class Elector extends ZkPrimative {
	private static final Logger logger =  Logger.getLogger(Elector.class);
	
	private static Map<String,Elector> electors = new ConcurrentHashMap();
	public synchronized static Elector getInstance(String nodes,String schema){
		Elector elector = electors.get(nodes);		
		if(elector == null || elector.closed){
			if(elector!=null){
				electors.remove(nodes);
			}
			elector = new Elector(nodes,schema);
		}
		
		return elector;
	}
	
	private String nodes;
	private LeaderOffer mySelf;
	private boolean isLeader = false;
	private List<ElectorListener> listeners = new ArrayList();
	private boolean closed = false;
	
	private Elector(String nodes,String schema){
		super(schema,true);
		//nodes = root+"/nodes";
		this.nodes = nodes;
		closed = false;
		Runtime.getRuntime().addShutdownHook(new Thread(){
			public void run() {
				exit();
			}
		});
		start();
	}
	
	public void initHook(){
		zk.addExitListener(new ExitListener(){
			@Override
			public void execute() {
				closed = true;
			}
		});
	}
	
	public void addListener(ElectorListener listener){
		if(listeners.contains(listener)) return;
		listeners.add(listener);
	}
	
	public void removeListener(ElectorListener listener){
		listeners.remove(listener);
	}
	
	private void notifyAllListener(){
		for(ElectorListener listener : listeners){
			try{
				listener.becomeLeader();
			}catch(Exception e){
				
			}
		}
	}
	
	public void exit(){
		try {
			if(closed) return;
			closed = true;
			logger.info("deleting.."+mySelf.getNodePath());
			if(zk == null || zk.isClosed()) return;
			zk.delete(mySelf.getNodePath(), -1);
		} catch (KeeperException e) {
			logger.warn("Caught: " + e, e);
		} catch (InterruptedException e) {
			logger.warn("Caught: " + e, e);
		}
	}
	
	public void start(){
		logger.info("选举器:"+nodeName+"起来了.");
		ready();
		determinLeader();
	}
	

	public boolean isLeader() {
		return !closed && isLeader;
	}

	public LeaderOffer getMySelf() {
		return mySelf;
	}

	public void process(WatchedEvent event) {
		logger.info(event.getPath()+":"+event.getType());
		if(Watcher.Event.EventType.NodeDeleted.equals(event.getType())) {
			//Oh 我前面的家伙真的挂了
			determinLeader();
		}	 
	}
	
	public List<LeaderOffer> getLeaderOffers(Watcher watcher) throws KeeperException, InterruptedException{
		List<LeaderOffer> leaderOffers = new ArrayList();
		if(zk == null || zk.isClosed()) return leaderOffers;
		
		List<String> nodeList = null;
		if(watcher == null){
			nodeList = zk.getChildren(nodes, false);
		}else{
			nodeList = zk.getChildren(nodes, watcher);
		}
		if(Util.isEmpty(nodeList)) return leaderOffers;
		
		for(String node : nodeList){
			logger.info("option:"+node);
			byte[] znodeName = zk.getData(nodes+"/"+node, false, null);
			Integer num = Util.parseInt(node.substring(("n_").length()));
			LeaderOffer offer = new LeaderOffer();
			offer.setId(num);
			offer.setNodePath(nodes+"/"+node);
			offer.setNodeName(new String(znodeName));
			leaderOffers.add(offer);
		}
		return leaderOffers;
	}
	
	private void determinLeader(){
		try {
			List<LeaderOffer> leaderOffers = getLeaderOffers(null);
			Collections.sort(leaderOffers, new LeaderOffer.IdComparator());
			
			int myPos = leaderOffers.indexOf(mySelf);
			if(myPos == 0){
				//哥是leader
				isLeader = true;
				notifyAllListener();
				logger.info("我是leader:"+nodeName);
				synchronized(this){
					this.notifyAll();
				}
			}else{
				isLeader = false;
				logger.info("我不是leader:"+nodeName);
				//哥只是一个跟风的,监视排名在我前面的那个家伙
				LeaderOffer target = leaderOffers.get(myPos-1);
				Stat stat = zk.exists(target.getNodePath(), this);
				if(stat == null){
					//如果前面这个家伙在我还没有监听的时候已经挂了,得重新选举了
					determinLeader();
				}
			}
		} catch (KeeperException e) {
			logger.warn("Caught: " + e, e);
		} catch (InterruptedException e) {
			logger.warn("Caught: " + e, e);
		}
	}
	
	private void ready(){
		ensurePathExists(nodes);
        try {
        	logger.info("ready for "+nodeName);
        	String myPath = zk.create(nodes+"/n_", nodeName.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        	mySelf = new LeaderOffer();
        	mySelf.setNodePath(myPath);
        	mySelf.setNodeName(nodeName);
        	Integer num = Util.parseInt(myPath.substring((nodes+"/n_").length()));
        	mySelf.setId(num);
        } catch (KeeperException e) {
        	logger.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
        	logger.warn("Caught: " + e, e);
        }	
	}
	
	public static void main(String[] args){
		String a = "/fconv/nodes/n_34434";
		long num = Util.parseLong(a.substring(("/fconv/nodes/n_").length()));
		System.out.println(num);
	}
}
