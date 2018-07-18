package com.gleasy.library.cloud.mq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.MqConfigUtil;
import com.gleasy.library.cloud.mq.util.MqLockUtil;
import com.gleasy.library.cloud.mq.util.RedisFactory;
import com.gleasy.library.cloud.mq.util.TopicConfig;
import com.gleasy.library.redis.client.LocalRedisClient;
import com.gleasy.library.redis.client.lock.RedisLockUtil.LockStat;
import com.gleasy.util.Util;

import redis.clients.util.SafeEncoder;

/**
 * 消息持久化类
 */
public class MessageStore {
	private Logger logger = Logger.getLogger(MessageStore.class);
	
	private String messagePrefix = "cloud:mq:msg:";
	private String messageIdGenerator = "cloud:mq:msg:id:gen:";
	private String messageIdMax = "cloud:mq:msg:id:max:";
	
	
	private String offsetPrefix = "cloud:mq:topic:offset:";
	private String signalPrefix = "cloud:mq:sn:";
	
	private PartitionMapper partitionMapper = new DefaultPubsubPartitionMapper();
	
	private String zkSchema;
	public MessageStore(String zkSchema){
		this.zkSchema = zkSchema;
	}
	
	public PartitionMapper getPartitionMapper() {
		return partitionMapper;
	}

	public void setPartitionMapper(PartitionMapper partitionMapper) {
		this.partitionMapper = partitionMapper;
	}

	public Long getPartition(Message message){
		if(message == null) return null;
		if(message.getPartition() != null) return message.getPartition();
		int partitionNum = RedisFactory.getMessagePartitionNum(zkSchema,message.getTopic());
		return new Long(partitionMapper.getPartition(message, partitionNum));
	}
	
	public Long getMaxId(String topic, int partition){
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		
		String max = client.get(new Long(partition),messageIdMax+topic+":"+partition);
		if(max == null){
			max = client.get(new Long(partition),messageIdGenerator+topic+":"+partition);
		}
		if(max == null) return -1l;
		Long m = Util.parseLong(max);
		if(m == null) return -1l;
		return m;
	}
	
	/**
	 * 保存消息
	 * @param job
	 * @param pattern
	 */
	public Message _addMessage(Message message) throws MqException{
		if(Util.isEmpty(message)) throw new MqException("message不能为空");
		if(Util.isEmpty(message.getTopic())) throw new MqException("message.topic不能为空");
		if(Util.isEmpty(message.getBody())) throw new MqException("message.body不能为空");
		//StopWatch stop = new StopWatch("MessageStore.addMessage");
		//stop.stop();
		String topic = message.getTopic();
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		//stop.stop();
		Long partition = getPartition(message);
		//stop.stop();
		long id = client.incr(partition,messageIdGenerator+topic+":"+partition);
		//stop.stop();
		message.setId(id);
		message.setPartition(partition);
		int messageLifetime = MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMessageLifetime();
		//stop.stop();
		if(messageLifetime > 0){
			//logger.info("增加topic消息");
			client.setex(partition, (messagePrefix+topic+":"+partition+":"+id).getBytes(),messageLifetime, message.getBody());
		}else{
			//logger.info("增加永久消息");
			client.set(partition, (messagePrefix+topic+":"+partition+":"+id).getBytes(),message.getBody());
		}
		//stop.stop();
		//stop.log();
		return message;
	}
	
	/**
	 * 批量生产消息
	 * @param messages
	 * @param partition
	 * @throws MqException
	 */
	public Set<Long> batchAddMessage(List<Message> messages,String topic) throws MqException{
		if(messages == null || messages.isEmpty()) return null;
		Map<Long,List<Message>> divids = new HashMap();
		for(Message message : messages){
			Long partition = getPartition(message);
			List<Message> tmp = divids.get(partition);
			if(tmp == null){
				tmp = new ArrayList<Message>();
				divids.put(partition, tmp);
			}
			tmp.add(message);
		}
		Set<Long> keyset = divids.keySet();
		for(Long partition:keyset){
			batchAddMessage(divids.get(partition),topic,partition);
		}
		return keyset;
	}
	
	/**
	 * 批量生产消息
	 * @param messages
	 * @param partition
	 * @throws MqException
	 */
	public void batchAddMessage(final List<Message> messages,final String topic,final Long partition) throws MqException{
		if(messages == null || messages.isEmpty()) return;
		final LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		final int messageLifetime = MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMessageLifetime();
		
		final int size = messages.size();
		
		MqLockUtil util = MqLockUtil.getInstance(zkSchema);
		
		LockStat stat = util.lockPartition(topic, partition);
		if(stat == null){
			throw new MqException("加锁失败");
		}
		try{
			final long maxId = client.incrBy(partition,messageIdGenerator+topic+":"+partition,size);
			final Map<String,String> params = new HashMap<String,String>();
			for(int i=0;i<size;i++){
				long id = maxId - size + i + 1;
				Message message = messages.get(i);
				message.setId(id);
				message.setPartition(partition);
				params.put(messagePrefix+topic+":"+partition+":"+id, SafeEncoder.encode(message.getBody()));
			}
/*			client.multi(partition, new MultiBlock(){
				@Override
				public void execute() {		
					if(messageLifetime > 0){
						transaction.msetex(params, messageLifetime);
					}else{
						transaction.mset( params);
					}
					transaction.set(messageIdMax+topic+":"+partition, maxId+"");
				}
			});*/
			if(messageLifetime > 0){
				client.msetex(partition,params, messageLifetime);
				client.set(partition,messageIdMax+topic+":"+partition, maxId+"");
			}else{
				params.put(messageIdMax+topic+":"+partition, maxId+"");
				//client.set(partition,messageIdMax+topic+":"+partition, maxId+"");
				client.mset(partition, params);
			}
			
		}finally{
			util.unlockPartition(topic, partition, stat);
		}
	}
	
	/**
	 * 某个主题的某个队列是否有人在等消息
	 * @param topic
	 * @param partition
	 * @return
	 */
	public boolean turnOffSignal(String topic, long partition){
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		String key =  signalPrefix+topic+":"+partition;
		String orignal = client.getSet(new Long(partition), key, "0");
		return "1".equals(orignal);
	}
	
	/**
	 * 设置某个主题的某个队列有人在等消息
	 * @param topic
	 * @param partition
	 * @return
	 */
	public void turnOnSignal(String topic, int partition){
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		String key =  signalPrefix+topic+":"+partition;
		client.set(new Long(partition), key , "1");
	}
	
	/**
	 * 读取消息
	 * @param topic
	 * @param id
	 * @return
	 * @throws MqException
	 */
	public List<Message> getMessage(String topic,int partition, long start, Param batchsize) throws MqException{
		if(Util.isEmpty(topic)) throw new MqException("topic不能为空");
		long maxid = getMaxId(topic,partition);
		//logger.info(topic+":partition="+partition+":maxid="+maxid);
		if(start >= maxid ) return null;
		long left = maxid - start;
		if(left < batchsize.value){
			batchsize.value = (int)left;
		}
		List<Message> messages = new ArrayList();
		byte[][] keys = new byte[batchsize.value][];
		for(int i=0;i<batchsize.value;i++){
			Long id = start + i + 1;
			byte[] key = (messagePrefix+topic+":"+partition+":"+id).getBytes();
			keys[i] = key;
		}
		List<byte[]> bodies = RedisFactory.getMessageLocalClient(zkSchema,topic).mget(new Long(partition), keys);
		for(int i=0;i<batchsize.value;i++){
			byte[] r = bodies.get(i);
			Long id = start + i + 1;
			if(r != null && id != null){
				Message message = new Message(topic,r);
				message.setId(id);
				message.setPartition(new Long(partition));	
				messages.add(message);
			}else{
				logger.warn("Message is expire."+messagePrefix+topic+":"+partition+":"+id);
			}
		}
		return messages;
	}
	
	/**
	 * 批量删除消息(用于queue模式)
	 * @param topic
	 * @param partition
	 * @param messages
	 * @throws MqException
	 */
	private void removeMessages(String topic, int partition,List<Message> messages)  throws MqException{
		if(Util.isEmpty(messages)) return;
		int size = messages.size();
		byte[][] keys = new byte[size][];
		for(int i=0;i<size;i++){
			Message message = messages.get(i);
			if(message == null) continue;
			Long id = message.getId();
			byte[] key = (messagePrefix+topic+":"+partition+":"+id).getBytes();
			keys[i] = key;
		}
		//logger.info("删除消息:"+size+"条");
		RedisFactory.getMessageLocalClient(zkSchema,topic).del(new Long(partition), keys);
	}

	/**
	 * 读取消息
	 * @param topic
	 * @param id
	 * @return
	 * @throws MqException
	 */
	public List<Message> getMessage(String subscriber, String topic, int partition ,int batchsize) throws MqException{
		List<Message> messages = null;
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		while(true){			
			String offset = client.get(new Long(partition), offsetPrefix+topic+":"+subscriber+":"+partition);
			Long start = Util.parseLong(offset);
			if(start == null || start<0) start = 0l;
			Param p = new Param();
			p.value = batchsize;
			messages = getMessage(topic, partition, start, p);
			if(messages == null) return null;
			if(messages.isEmpty()){
				saveOffset(subscriber,topic,partition,start + p.value,messages);
			}else{
				return messages;
			}
		}
	}
	public List<Message> _getMessage(String subscriber, String topic, int partition ,int batchsize) throws MqException{
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		String offset = client.get(new Long(partition), offsetPrefix+topic+":"+subscriber+":"+partition);
		Long start = Util.parseLong(offset);
		if(start == null || start<0) start = 0l;
		Param p = new Param();
		p.value = batchsize;
		List<Message> messages = getMessage(topic, partition, start, p);
		if(messages!=null && messages.isEmpty()){
			saveOffset(subscriber,topic,partition,start + p.value,messages);
			return getMessage(subscriber,topic,partition,batchsize);
		}else{
			return messages;
		}
	}
	
	private static class Param{
		public int value;
	}
	
	/**
	 * 保存offset
	 * @param subscriber
	 * @param topic
	 * @param partition
	 * @param offset
	 * @throws MqException
	 */
	public void saveOffset(String subscriber, String topic, int partition ,Long offset, List<Message> messages) throws MqException{
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		client.set(new Long(partition), offsetPrefix+topic+":"+subscriber+":"+partition,offset.toString());
		String mode = MqConfigUtil.getInstance(zkSchema).getConfig(topic).getMode();
		if(TopicConfig.MODE_QUEUE.equals(mode)){
			removeMessages(topic,partition,messages);
		}
	}
}
