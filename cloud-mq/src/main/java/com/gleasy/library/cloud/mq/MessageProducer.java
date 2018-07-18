package com.gleasy.library.cloud.mq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.Op;

import com.gleasy.library.cloud.mq.domain.DelayMessage;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.RedisFactory;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.library.cloud.util.ExitListener;
import com.gleasy.library.cloud.util.ZkPrimative;
import com.gleasy.library.redis.exception.RedisShardException;
import com.gleasy.util.ExitHandler;
import com.gleasy.util.StopWatch;
import com.gleasy.util.SystemExitListener;

public class MessageProducer extends ZkPrimative {
	private static final Logger logger = Logger.getLogger(MessageProducer.class);

	private MessageStore messageStore;
	private DelayMessageStore delayMessageStore;

	private String topicPath;
	private String topicDalayPath;
	private String topic;

	private ZkConfig zkConfig;
	private SendNotifyThread notifyThread;
	private CountDownLatch doneSignal;

	private Map<Long, SaveMessageThread> threadMap = new ConcurrentHashMap<Long, SaveMessageThread>();

	public MessageProducer() {
	}

	public MessageProducer(ZkConfig zkConfig, String topic) {
		super(zkConfig.getSchema(), true);
		this.topic = topic;
		this.zkConfig = zkConfig;

		delayMessageStore = new DelayMessageStore(zkConfig.getSchema());
		messageStore = new MessageStore(zkConfig.getSchema());
		topicPath = root + "/topic";
		ensurePathExists(topicPath);

		topicPath += "/" + topic;
		ensurePathExists(topicPath);

		topicDalayPath = root + "/topicDelay";
		ensurePathExists(topicDalayPath);

		topicDalayPath += "/" + topic;
		ensurePathExists(topicDalayPath);

		int partitionNum = RedisFactory.getMessagePartitionNum(zkConfig.getSchema(), topic);
		doneSignal = new CountDownLatch(partitionNum + 1);

		for (int i = 0; i < partitionNum; i++) {
			String t = topicPath + "/" + i;
			ensurePathExists(t);

			t = topicPath + "/" + i + "/notify";
			ensurePathExists(t);

			SaveMessageThread thread = new SaveMessageThread(new Long(i));
			threadMap.put(new Long(i), thread);
			Thread th = new Thread(thread);
			th.setDaemon(false);
			th.setName("消息生产线程:" + topic + ":" + i);
			th.start();
		}

		notifyThread = new SendNotifyThread();
		Thread th = new Thread(notifyThread);
		th.setDaemon(false);
		th.setName("通知线程:" + topic);
		th.start();

		SystemExitListener.addListener(new ExitHandler() {
			public void run() {
				shutdown();
			}
		});
	}

	private void shutdown() {
		Set<Long> parts = threadMap.keySet();
		for (Long part : parts) {
			SaveMessageThread thread = threadMap.get(part);
			thread.shutdown();
		}
		notifyThread.shutdown();
		try {
			doneSignal.await();
		} catch (InterruptedException e) {
		}
	}

	public void initHook() {
		zk.addExitListener(new ExitListener() {
			@Override
			public void execute() {
				initZk(true);
			}
		});
	}

	/**
	 * 阻塞发送（一直到发送成功才返回）
	 * 
	 * @param body
	 * @throws MqException
	 */
	public void setPartitionMapper(PartitionMapper partitionMapper) {
		messageStore.setPartitionMapper(partitionMapper);
		delayMessageStore.setPartitionMapper(partitionMapper);
	}

	/**
	 * 阻塞发送（一直到发送成功才返回）
	 * 
	 * @param body
	 * @throws MqException
	 */
	public void produce(byte[] body) throws MqException {
		produce(body, null, true);
	}

	/**
	 * 异步发送（不等发送成功就立刻返回）
	 * 
	 * @param body
	 * @throws MqException
	 */
	public void asyProduce(byte[] body) throws MqException {
		produce(body, null, false);
	}

	/**
	 * 阻塞发送（一直到发送成功才返回）
	 * 
	 * @param body
	 *            消息内容
	 * @param partition
	 *            分区
	 * @throws MqException
	 */
	public void produce(byte[] body, Long partition) throws MqException {
		produce(body, partition, false);
	}

	/**
	 * 发送消息
	 * 
	 * @param body
	 *            消息体
	 * @param partition
	 *            分区（可以为NULL）
	 * @param wait
	 *            是否阻塞等待保存成功后才返回(true:阻塞,false:非阻塞)
	 * @throws MqException
	 */
	public void produce(byte[] body, Long partition, boolean wait) throws MqException {
		// if(SystemExitListener.isOver()) {
		// throw new MqException("系统关闭中,不再提供服务");
		// }
		if (body == null) {
			throw new MqException("body不能为空.");
		}
		Message message = new Message(topic, body);
		if (partition == null) {
			partition = messageStore.getPartition(message);
		}
		SaveMessageThread t = threadMap.get(partition);
		if (t == null) {
			throw new MqException("异常,分区" + partition + "没有处理模块.");
		}
		t.save(message);
		message.waitResult(60);
	}

	/*
	 * public void produce(byte[] body) throws MqException{ produce(body,null);
	 * }
	 * 
	 * public void produce(byte[] body ,Long partition) throws MqException{
	 * if(body == null){ throw new MqException("body不能为空."); } Message message =
	 * new Message(topic,body); message.setPartition(partition); message =
	 * messageStore.addMessage(message); try {
	 * if(messageStore.turnOffSignal(topic, message.getPartition())){
	 * zk.setData(topicPath + "/" +message.getPartition() + "/notify",
	 * (message.getPartition()+"-"+message.getId()).getBytes(), -1); } } catch
	 * (Exception e) { } }
	 */

	public void produce(List<byte[]> bodys) throws MqException {
		produce(bodys, null);
	}

	public void produce(List<byte[]> bodys, Long partition) throws MqException {
		// if(SystemExitListener.isOver()) {
		// throw new MqException("系统关闭中,不再提供服务");
		// }

		if (bodys == null || bodys.isEmpty()) {
			throw new MqException("body不能为空.");
		}
		List<Message> messages = new ArrayList<Message>();
		for (byte[] body : bodys) {
			Message message = new Message(topic, body);
			message.setPartition(partition);
			messages.add(message);
		}
		messageStore.batchAddMessage(messages, topic);
		if (messageStore.turnOffSignal(topic, partition)) {
			notifyThread.addNotify(topicPath + "/" + partition + "/notify");
		}
	}

	public void produce(DelayMessage message) throws MqException {
		// if(SystemExitListener.isOver()) {
		// throw new MqException("系统关闭中,不再提供服务");
		// }

		if (message == null) {
			throw new MqException("message不能为空.");
		}
		StopWatch stop = new StopWatch("messageProducer.delay");
		stop.stop();
		message.setTopic(topic);
		stop.stop();
		delayMessageStore.addMessage(message);
		stop.stop();
		//stop.log();
	}

	private class SendNotifyThread implements Runnable {
		private List<String> commands = new ArrayList();
		private Integer mutex = new Integer(1);
		private boolean stoped = false;
		private Object clock = new Object();

		public void shutdown() {
			stoped = true;
			synchronized (clock) {
				clock.notifyAll();
			}

		}

		public void addNotify(String path) {
			synchronized (mutex) {
				if (commands.contains(path))
					return;
				commands.add(path);
			}
			synchronized (clock) {
				clock.notifyAll();
			}
		}

		private List<String> getAll() {
			List<String> all = new ArrayList<String>();
			synchronized (mutex) {
				all.addAll(commands);
				commands.clear();
			}
			return all;
		}

		@Override
		public void run() {
			while (!stoped) {
				execute();
			}
			doneSignal.countDown();
			logger.info("SendNotifyThread退出");
		}

		private void execute() {
			List<String> tmp = getAll();
			if (tmp.isEmpty()) {
				try {
					synchronized (clock) {
						clock.wait();
					}
				} catch (InterruptedException e) {
				}
				tmp = getAll();
			}
			if (tmp.isEmpty())
				return;

			Set<Op> ops = new HashSet<Op>();
			for (String p : tmp) {
				Op op = Op.setData(p, (Math.random() + "").getBytes(), -1);
				ops.add(op);
			}
			try {
				zk.multi(ops);
			} catch (Exception e) {
				logger.error("err:" + e, e);
			}
		}
	}

	private class SaveMessageThread implements Runnable {
		private Object mutex = new Object();
		private Object seedMutex = new Object();
		private long seed = 0;
		private int bufsize = 10000;
		private boolean stoped = false;
		private ArrayBlockingQueue<Message> buffer = new ArrayBlockingQueue<Message>(bufsize);
		private Long partition;
		private long maxId;
		private int batchsize = 500;
		private Exception error;
		private boolean terminated = false;

	/*	public void waitForFinish(Long id, long timeout) throws MqException {
			long p1 = System.currentTimeMillis();
			
			while(true){
				if((maxId >= id && (maxId-id)<bufsize) || (maxId < id && (id - maxId) > 2*bufsize)){
					break;
				}else{
					if(System.currentTimeMillis() - p1 < 10){
						continue;
					}
					try {
						synchronized (mutex) {
							mutex.wait(30000);
						}
					} catch (InterruptedException e) {
						throw new MqException(e);
					}
					if (System.currentTimeMillis() - p1 >= timeout) {
						throw new MqException("保存消息超时");
					}
				}
			}
			if (error != null) {
				throw new MqException(error);
			}
		}*/

		public SaveMessageThread(Long partition) {
			this.partition = partition;
		}

		public void shutdown() {
			stoped = true;
		}

/*		private long genSeed(Message message) {
			synchronized (seedMutex) {
				seed++;
				if (seed > (Long.MAX_VALUE-10)) {
					seed = 0;
				}
				try {
					message.setPartition(partition);
					message.setTid(seed);
					buffer.put(message);
				} catch (InterruptedException e) {
					throw new MqException(e);
				}
				return seed;
			}
		}*/

		public void save(Message message) throws MqException {
			if (terminated)
				throw new MqException("生产线程已经关闭,不再接受消息发送");
			if (message == null)
				throw new MqException("message不能为null");
			try {
				message.setPartition(partition);
				buffer.put(message);
			} catch (InterruptedException e) {
				throw new MqException(e);
			}
		}

		private void execute() {
			error = null;
			List<Message> tmp = new ArrayList<Message>();
			buffer.drainTo(tmp, batchsize);
			if (tmp.isEmpty()) {
				if (stoped) {
					terminated = true;
					return;
				}
				Message first = null;
				try {
					first = buffer.poll(2, TimeUnit.SECONDS);
				} catch (InterruptedException e1) {
				}
				if (first == null) {
					return;
				}
				buffer.drainTo(tmp, batchsize);
				tmp.add(0, first);
			}
			StopWatch stop = new StopWatch("MessageProducer.execute");
			stop.stop();
			try {
				messageStore.batchAddMessage(tmp, topic);
				stop.stop();
				if (messageStore.turnOffSignal(topic, partition)) {
					stop.stop();
					notifyThread.addNotify(topicPath + "/" + partition + "/notify");
					stop.stop();
				}
				//stop.log();
			} catch (Exception e) {
				error = e;
			}
			for(Message m : tmp){
				if(m != null) m.notice();
			}
		}

		@Override
		public void run() {
			while (!terminated) {
				execute();
			}
			doneSignal.countDown();
			logger.info("生产线程" + partition + "退出");
		}
	}

	public static void main(String[] args) {
		CountDownLatch doneSignal = new CountDownLatch(3);
		doneSignal.countDown();
		doneSignal.countDown();
		doneSignal.countDown();
		try {
			doneSignal.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("xx");
	}

}
