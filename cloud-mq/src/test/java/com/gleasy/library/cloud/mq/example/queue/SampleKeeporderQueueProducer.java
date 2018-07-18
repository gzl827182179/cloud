package com.gleasy.library.cloud.mq.example.queue;

import java.util.HashMap;
import java.util.Map;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.PartitionMapper;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.SerializationUtil;

public class SampleKeeporderQueueProducer {
	
    public static void main(String[] args) throws Exception {
    	ZkConfig zkConfig = new ZkConfig("mqtest");

       String topic = "index";//发布的主题
       final MessageProducer producer = new MessageProducer(zkConfig,topic);
       
       //通过使用PartitionMapper,来保证合适的消息进入合适的分片，从而保证合理的处理顺序
       producer.setPartitionMapper(new PartitionMapper(){
			@Override
			public int getPartition(Message message, int partitioNum) {
				byte[] body = message.getBody();
				try {
					Map obj = SerializationUtil.jsonByteDeserialize(body, Map.class);
					Integer uid = (Integer)obj.get("uid");
					return uid % partitioNum;
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}
       });
       
       for(int i=0;i<100;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", 10);
    	   obj.put("name", "name"+i);
    	   producer.produce(SerializationUtil.jsonByteSerialize(obj));
       }
       
	   System.exit(1);
    }

}
