package com.gleasy.library.cloud.mq.example.topic;

import java.util.HashMap;
import java.util.Map;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.PartitionMapper;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;

public class SampleKeeporderTopicProducer {
	
    public static void main(String[] args) throws Exception {
        Config.setConfig("E:\\j2ee\\gleasy.com\\config\\util\\config.properties");
        ZkConfig zkConfig = new ZkConfig("mq.test.schema");

        String topic = "topic1";//订阅主题
       
       //MessageProducer尽量复用,强烈建议使用单例
       final MessageProducer producer = new MessageProducer(zkConfig, topic);
       
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
       
       for(int i=0;i<1000000;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", 10);
    	   obj.put("name", "name"+i);
    	   producer.produce(SerializationUtil.jsonByteSerialize(obj));
    	   
    	   Thread.sleep(1000);
       }
       
	   System.exit(1);
    }

}
