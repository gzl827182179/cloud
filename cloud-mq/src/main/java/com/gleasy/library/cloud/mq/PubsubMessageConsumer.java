package com.gleasy.library.cloud.mq;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;

import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.MqConfigUtil;
import com.gleasy.library.cloud.mq.util.MqLockUtil;
import com.gleasy.library.cloud.mq.util.RedisFactory;
import com.gleasy.library.cloud.mq.util.TopicConfig;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.library.cloud.util.ExitListener;
import com.gleasy.library.cloud.util.LeaderOffer;
import com.gleasy.library.cloud.util.ZkPrimative;
import com.gleasy.library.redis.client.lock.RedisLockUtil;
import com.gleasy.library.redis.connection.ConnectionFactory;
import com.gleasy.util.ExitHandler;
import com.gleasy.util.SystemExitListener;
import com.gleasy.util.Util;

public class PubsubMessageConsumer extends ZkPrimative {
	private static final Logger logger =  Logger.getLogger(PubsubMessageConsumer.class);
	
	private String topicPath;
	private String topicDalayPath;
	private String consumerRegPath;
	
	private boolean stoped = false;
	private int batchsize = 1;
	private String topic;
	private MessageHandler handler;
	private String subscriber;
	private String consumers;
	
	private MessageStore messageStore;
	private LeaderOffer mySelf;
	private AtomicBoolean needReblance = new AtomicBoolean(true);
	private AtomicBoolean rebalancing = new AtomicBoolean(false);
	private Object reblanceMutex = new Object();
	private List<Integer> ownerPartition = new ArrayList();
	private Map<String,MessageConsumerThread> threads = new HashMap();

	private CountDownLatch doneSignal;
	private ZkConfig zkConfig;
	

	public int getBatchsize() {
		return batchsize;
	}

	public void setBatchsize(int batchsize) {
		this.batchsize = batchsize;
	}

	
	public PubsubMessageConsumer(ZkConfig zkConfig, String subscriber, String topic, int batchsize, MessageHandler handler){
		super(zkConfig.getSchema());
		if(zk == null || zk.isClosed()){
			logger.error("无法启动消费者,连接不上zookeeper.永久退出.");
			return;
		}
		this.zkConfig = zkConfig;
		this.subscriber = subscriber;
		this.topic = topic;
		this.batchsize = batchsize;
		this.handler = handler;
		messageStore = new MessageStore(zkConfig.getSchema());
		
		
		String mode = MqConfigUtil.getInstance(zkConfig.getSchema()).getConfig(topic).getMode();
		if(this instanceof QueueMessageConsumer){
			if(!TopicConfig.MODE_QUEUE.equals(mode)){
				throw new MqException("QueueMessageConsumer只能消费queue类型的主题");
			}
		}else{
			if(!TopicConfig.MODE_TOPIC.equals(mode)){
				throw new MqException("PubsubMessageConsumer只能消费topic类型的主题");
			}
		}
		new DelayMessageConsumer(zkConfig,topic,100);
		new Thread(new Runnable(){
			@Override
			public void run() {
				start();
			}
		}).start();
		
		SystemExitListener.addListener(new ExitHandler(){
			public void run() {
				shutdown();
			}
		});
	}
	
	public void initHook(){
		zk.addExitListener(new ExitListener(){
			@Override
			public void execute() {
				shutdown();
				initZk(true);
				if(zk == null || zk.isClosed()){
					logger.error("无法重启消费者,连接不上zookeeper.永久退出.");
					return;
				}
				restart();
			}
		});
	}
	
	
	public void process(WatchedEvent event) {		
		String p = event.getPath();
		logger.debug("watching:"+p);
		if(p != null ){
			if(p.startsWith(consumerRegPath)){
				rebalance();
			}else if(p.startsWith(topicPath)){
				MessageConsumerThread t = threads.get(event.getPath());
				if(t != null){
					t.wakeup();
				}
				watchJobQueue(p);
			}
		}
	}

	
	private void watchJobQueue(String queuePath){
		try {
			logger.debug("watch:"+queuePath);
			zk.getData(queuePath, this, null);
		 } catch (KeeperException e) {
	    	logger.warn("Caught: " + e, e);
	    } catch (InterruptedException e) {
	    	logger.warn("Caught: " + e, e);
	    }
	}
	
	public List<LeaderOffer> getLeaderOffers(Watcher watcher) throws KeeperException, InterruptedException{
		List<LeaderOffer> leaderOffers = new ArrayList();
		List<String> nodeList = null;
		if(watcher == null){
			nodeList = zk.getChildren(consumerRegPath, false);
		}else{
			nodeList = zk.getChildren(consumerRegPath, watcher);
		}
		if(Util.isEmpty(nodeList)) return leaderOffers;
		
		logger.debug("consumerRegPath:"+consumerRegPath);
		for(String node : nodeList){
			Integer num = Util.parseInt(node.substring(("n_").length()));
			LeaderOffer offer = new LeaderOffer();
			offer.setId(num);
			offer.setNodePath(consumerRegPath+"/"+node);
			leaderOffers.add(offer);
		}
		return leaderOffers;
	}
	
	private void rebalance(){
		if(rebalancing.compareAndSet(false, true)){
			needReblance.set(true);
			while(needReblance.compareAndSet(true, false)){
				try {
					List<LeaderOffer> leaderOffers = getLeaderOffers(this);
					Collections.sort(leaderOffers, new LeaderOffer.IdComparator());
					
					int myPos = leaderOffers.indexOf(mySelf);
					if(myPos < 0) continue;
					
					int partitionNum = RedisFactory.getMessagePartitionNum(zkConfig.getSchema(),topic);
					
					int N = partitionNum / leaderOffers.size();
					
					
					int start = myPos * N;
					int end = (myPos + 1) * N;
					ownerPartition.clear();
					for(int i=start;i<end;i++){
						zk.setData(consumers + "/owners/"+topic+"/"+i, mySelf.getId().toString().getBytes(), -1);
						ownerPartition.add(i);
					}
					
					int total = N * leaderOffers.size();
					for(int i=0;i<(partitionNum - total);i++){
						if(i == myPos){
							int p = total + i;
							zk.setData(consumers + "/owners/"+topic+"/"+p, mySelf.getId().toString().getBytes(), -1);
							ownerPartition.add(p);
						}
					}
					synchronized(reblanceMutex){
						reblanceMutex.notifyAll();
					}
					
				} catch (KeeperException e) {
					logger.warn("Caught: " + e, e);
				} catch (InterruptedException e) {
					logger.warn("Caught: " + e, e);
				}
			}
			rebalancing.set(false);
		}else{
			needReblance.set(true);
		}
	}
	
	private void registerConsumer(){
        try {
        	String myPath = zk.create(consumerRegPath+"/n_", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        	mySelf = new LeaderOffer();
        	mySelf.setNodePath(myPath);
        	Integer num = Util.parseInt(myPath.substring((consumerRegPath+"/n_").length()));
        	mySelf.setId(num);
        } catch (KeeperException e) {
        	logger.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
        	logger.warn("Caught: " + e, e);
        }	
	}
	
	private void unregisterConsumer(){
		try {
			 if(zk.isClosed()) return;
			 if(mySelf == null) return;
			 zk.delete(mySelf.getNodePath(), -1);
        } catch (KeeperException e) {
        	logger.warn("Caught: " + e, e);
        } catch (InterruptedException e) {
        	logger.warn("Caught: " + e, e);
        }
	}
	
	private void start(){
		needReblance = new AtomicBoolean(true);
		rebalancing = new AtomicBoolean(false);
		stoped = false;
		ownerPartition = new ArrayList();
		threads = new HashMap();

		consumers = root + "/mqconsumers";
		ensurePathExists(consumers);
		
		consumers += "/" + subscriber;
		ensurePathExists(consumers);
		
		String owners = consumers + "/owners";
		ensurePathExists(owners);
		
		owners += "/" + topic;
		ensurePathExists(owners);
		
		consumerRegPath = consumers + "/" + topic;
		ensurePathExists(consumerRegPath);
		registerConsumer();		
		
		topicPath = root + "/topic";
		ensurePathExists(topicPath);
		
		topicPath += "/" + topic;
		ensurePathExists(topicPath);
		
		String lockPath = topicPath +"/" + subscriber;
		ensurePathExists(lockPath);
		
		topicDalayPath = root + "/topicDelay";
		ensurePathExists(topicDalayPath);
		
		topicDalayPath += "/" + topic;
		ensurePathExists(topicDalayPath);
		
		String delayLockPath = topicDalayPath +"/" + subscriber;
		ensurePathExists(delayLockPath);
		
		int partitionNum = RedisFactory.getMessagePartitionNum(zkConfig.getSchema(),topic);
		for(int i=0;i<partitionNum;i++){
			lockPath = topicPath +"/" + subscriber + "/" + i;
			ensurePathExists(lockPath);
			
			delayLockPath = topicDalayPath +"/" + subscriber + "/" +i;
			ensurePathExists(delayLockPath);
			
			String t = topicPath + "/" + i;
			ensurePathExists(t);
			
			t = topicPath + "/" + i + "/notify";
			ensurePathExists(t);
			
			String t2 = topicDalayPath + "/" + i;
			ensurePathExists(t2);
			
			t2 = topicDalayPath + "/" + i + "/notify";
			ensurePathExists(t2);
			
			String powners = owners + "/" + i;
			ensurePathExists(powners);
		}
		
		rebalance();		

		doneSignal = new CountDownLatch(partitionNum);
		
		for(int i=0;i<partitionNum;i++){					
			String p = topicPath + "/" + i + "/notify";
			MessageConsumerThread thread = new MessageConsumerThread(i);
			threads.put(p, thread);
			watchJobQueue(p);
			Thread td = new Thread(thread);
			td.setDaemon(false);
			td.setName("消费者:"+topic+":"+subscriber+":"+i);
			td.start();
		}
		
		logger.info("分区数量:"+partitionNum);
	}
	
	
	private void restart(){	
		logger.info("消费者重启........");
		new Thread(new Runnable(){
			@Override
			public void run() {
				start();
			}
		}).start();
		logger.info("消费者重启........完毕！");
	}
	
	public void shutdown(){
		stoped = true;
		synchronized(reblanceMutex){
			reblanceMutex.notifyAll();
		}
		Set<String> keys = threads.keySet();
		for(String k : keys){
			MessageConsumerThread t = threads.get(k);
			if(t != null) t.wakeup();
		}
		
		unregisterConsumer();
		
		if(doneSignal!=null){
			try {
				doneSignal.await();
			} catch (InterruptedException e) {
			}
		}
	}
	
	private void threadFinish(){
		if(doneSignal != null){
			doneSignal.countDown();
		}
	}

	private class MessageConsumerThread implements Runnable{
		private int partition;
		private Object clock = new Object();
		private Long maxoffset = -1l;
		private Object offsetLock = new Object();
		
		public MessageConsumerThread(int partition){
			this.partition = partition;
		}

		public void wakeup(){ 
			synchronized(clock){
				clock.notifyAll();
			}
		}
		
		private void waitForMessage(){
			messageStore.turnOnSignal(topic, partition);
			synchronized(clock){
				try {
					clock.wait(30000);
				} catch (InterruptedException e) {
				}
			}
		}
		
		private void consumeSingle(Message message){
			int error = 0;
			while(true){
				try {
					handler.handle(message);
					error = 0;
					break;
				} catch (RuntimeException e) {
					error++;
					if(error < 5){
						try{
							Thread.sleep(error * 1000);
						}catch(Exception f){
							
						}
						continue;
					}else{
						logger.error("处理消息失败5次以上:"+topic+":"+subscriber+":"+partition+"="+message.getId());
						break;
					}
				}
			}
			
			synchronized(offsetLock){
				if(message.getId() > maxoffset){
					maxoffset = message.getId();
				}
			}
		}
		
		private void consume(final List<Message> messages){
			maxoffset = -1l;
			if(handler.getExecutor() == null){
				for(Message message : messages){
					if(message == null) continue;
					consumeSingle(message);
				}
			}else{
				final CountDownLatch doneSignal = new CountDownLatch(messages.size());
				
				for(final Message message : messages){
					if(message == null) continue;
					handler.getExecutor().execute(new Runnable(){
						@Override
						public void run() {
							consumeSingle(message);
							doneSignal.countDown();
						}
					});
				}
				
				try {
					doneSignal.await();
				} catch (InterruptedException e) {
				}
			}
		}
		
		@Override
		public void run() {
		    ConnectionFactory.setSingleConnectionPerThread(1000);
		    RedisLockUtil.LockStat stat = null;
		    //String lockPath = topicPath +"/" + subscriber + "/" + partition + "/lock";
		    int error = 0;
			while(!stoped){
				try{
					if(!ownerPartition.contains(partition)){
						if(stat != null){
							try {
								MqLockUtil.getInstance(zkConfig.getSchema()).unlockSubscriberPartition(subscriber, topic, new Long(partition), stat);
								//DistributedLockUtil.getInstance(zkConfig.getSchema()).unlock(lockPath,stat,true);
							} catch (Exception e) {
								logger.error("err:"+e,e);
							} 
							stat = null;
						}
						synchronized(reblanceMutex){
							try {
								reblanceMutex.wait();
							} catch (InterruptedException e) {
							}
						}
						continue;
					}
					logger.debug("消费第"+partition+"号队列");
					if(stat == null){
						try {
							stat = MqLockUtil.getInstance(zkConfig.getSchema()).lockSubscriberPartition(subscriber, topic, new Long(partition));
							//stat = DistributedLockUtil.getInstance(zkConfig.getSchema()).lock(lockPath);
						} catch (Exception e1) {
							logger.error("error:"+e1,e1);
							try {
								Thread.sleep(200);
							} catch (InterruptedException e) {
							}
							continue;
						}
					}
					List<Message> messages = messageStore.getMessage(subscriber, topic, partition, batchsize);
					if(Util.isEmpty(messages)){
						waitForMessage();
						continue;
					}
					consume(messages);
					messageStore.saveOffset(subscriber, topic, partition, maxoffset, messages);
					error = 0;
				}catch(RuntimeException e){
					logger.error("error:"+e,e);
					error++;
					try{
						if(error > 100){
							error = 100;
						}
						Thread.sleep(error*100);
					}catch(Exception f){
					}
				}
			}
			if(stat != null){
				try {
					MqLockUtil.getInstance(zkConfig.getSchema()).unlockSubscriberPartition(subscriber, topic, new Long(partition), stat);
					//DistributedLockUtil.getInstance(zkConfig.getSchema()).unlock(lockPath,stat,true);
				} catch (Exception e) {
					logger.error("err:"+e,e);
				} 
				stat = null;
			}
			ConnectionFactory.releaseThreadConnection();
			logger.info(Thread.currentThread().getName()+"退出");
			threadFinish();
		}
	}
}
