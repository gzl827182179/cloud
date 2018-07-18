package com.gleasy.library.cloud.mq;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import redis.clients.jedis.Tuple;
import redis.clients.util.SafeEncoder;

import com.gleasy.library.cloud.mq.domain.DelayMessage;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.RedisFactory;
import com.gleasy.library.redis.client.LocalRedisClient;
import com.gleasy.util.Util;

/**
 * 消息持久化类
 */
public class DelayMessageStore {
	private Logger logger = Logger.getLogger(DelayMessageStore.class);
	
	private String messagePrefix = "cloud:mq:dmsg:";
	//private String messageIdPrefix = "cloud:mq:dmsg:id:";
	//private String messagePartitionPrefix = "cloud:mq:dmsg:pt:";
	//private String messageIdGenerator = "cloud:mq:dmsg:id:gen:";
	private String delayQueuePrefix = "cloud:mq:dmsg:q";
	//private String delayQueueSignalPrefix = "cloud:mq:dmsg:sn:";
	
	private PartitionMapper partitionMapper = new DefaultDelayMessagePartitionMapper();
	
	private String zkSchema;
	public DelayMessageStore(String zkSchema){
		this.zkSchema = zkSchema;
	}
	
	public PartitionMapper getPartitionMapper() {
		return partitionMapper;
	}

	public void setPartitionMapper(PartitionMapper partitionMapper) {
		this.partitionMapper = partitionMapper;
	}


	
	private Long genPartition(Message message){
		if(message == null) return null;
		if(message.getPartition() != null) return message.getPartition();
		int partitionNum = RedisFactory.getMessagePartitionNum(zkSchema,message.getTopic());
		return new Long(partitionMapper.getPartition(message, partitionNum));
	}
	
/*	public Long getPartition(String topic,Long id){
		if(id == null) return null;
		Long order = RedisFactory.getMessageGlobalClient(zkSchema,topic).getObject(messagePartitionPrefix+topic+":"+id);
		return order;
	}*/
	
/*	public List<Long> mgetPartition(String topic,List<Long> ids){
		if(ids == null) return null;
		List<String> keys = new ArrayList();
		for(Long id : ids){
			keys.add(messagePartitionPrefix+topic+":"+id);
		}
		List<Long> orders = RedisFactory.getMessageGlobalClient(zkSchema,topic).mgetObjectList(keys);
		return orders;
	}*/
	
	/**
	 * 保存消息
	 * @param job
	 * @param pattern
	 */
	public Long addMessage(DelayMessage message) throws MqException{
		if(Util.isEmpty(message)) throw new MqException("message不能为空");
		if(Util.isEmpty(message.getTopic())) throw new MqException("message.topic不能为空");
		if(Util.isEmpty(message.getBody())) throw new MqException("message.body不能为空");
		if(Util.isEmpty(message.getLatency())) throw new MqException("message.latency不能为空");
		
		String topic = message.getTopic();
		LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
		/**
		 * 1. 判断消息是否存在
		 * 2. 如果不存在，尝试加锁
		 * 3. 加锁成功，再次判断消息是否存在（以防在等锁过程中被其它结点创建了）
		 * 4. 生成新消息
		 */
		Long partition = genPartition(message);
		String k = messagePrefix+topic+":"+message.getUuid();
		byte[] original = client.getSet(partition,SafeEncoder.encode(k), message.getBody());
		if(original == null){
			long l = System.currentTimeMillis() + message.getLatency();
			return client.zadd(partition, delayQueuePrefix+topic+":"+partition, new Double(l), message.getUuid());
		}
		return null;
	}
	
	
	/**
	 * 获取应该立刻处理的消息
	 * @param queueOrder
	 * @return
	 */
	public List<DelayMessage> getMessages(String topic, int partition,int batchsize){
		try{
			List<DelayMessage> messages = new ArrayList<DelayMessage>();
			LocalRedisClient client = RedisFactory.getMessageLocalClient(zkSchema,topic);
			String key =  delayQueuePrefix+topic+":"+partition;
			Long now = System.currentTimeMillis();
			Set<Tuple> nodes  = client.zrangeByScoreWithScores(new Long(partition), key, 0, now ,0, batchsize);
			if(Util.isNotEmpty(nodes)){
				int size = nodes.size();			
				byte[][] keys = new byte[size][];
				int i=0;
				for(Tuple tp : nodes){
					keys[i] = (messagePrefix+topic+":"+tp.getElement()).getBytes();
					DelayMessage m = new DelayMessage(tp.getElement(),"".getBytes(),0);
					m.setPartition(new Long(partition));
					m.setTopic(topic);
					messages.add(m);
					i++;
				}
				List<byte[]> bodies = new ArrayList<byte[]>();
				bodies = client.mget(new Long(partition), keys);
				i=0;
				for(DelayMessage m : messages){
					m.setBody(bodies.get(i));
					i++;
				}
				removeMessages(topic,partition,messages);
				return messages;
			}
		}catch(Exception e){
			logger.error("error:"+e,e);
		}
		return null;
	}
	
	/**
	 * 批量删除消息
	 * @param topic
	 * @param partition
	 * @param messages
	 * @throws MqException
	 */
	public void removeMessages(String topic, int partition,List<DelayMessage> messages)  throws MqException{
		if(Util.isEmpty(messages)) return;
		int size = messages.size();
		byte[][] messageKeys = new byte[size][];
		String[] qkeys = new String[size];
		for(int i=0;i<size;i++){
			DelayMessage message = messages.get(i);
			if(message == null) continue;
			byte[] key = (messagePrefix+topic+":"+message.getUuid()).getBytes();
			messageKeys[i] = key;
			qkeys[i] = message.getUuid();
		}
		logger.info("删除delay消息:"+size+"条");
		RedisFactory.getMessageLocalClient(zkSchema,topic).del(new Long(partition), messageKeys);
		RedisFactory.getMessageLocalClient(zkSchema,topic).zrem(new Long(partition), delayQueuePrefix+topic+":"+partition, qkeys);
	}

}
