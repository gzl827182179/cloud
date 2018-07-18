package com.gleasy.library.cloud.mq.example.user;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.PartitionMapper;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;
import com.gleasy.util.consts.MqConstants;

import java.util.HashMap;
import java.util.Map;

public class SampleKeeporderTopicProducer {
	
    public static void main(String[] args) throws Exception {
        Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_6.properties");

    	ZkConfig zkConfig = new ZkConfig(MqConstants.SCHEMA_UCENTER);

       String topic = MqConstants.TOPIC_DEPARTMENT_OP;//订阅主题
       final MessageProducer producer = new MessageProducer(zkConfig, topic);
       
       //通过使用PartitionMapper,来保证合适的消息进入合适的分片，从而保证合理的处理顺序
       producer.setPartitionMapper(new PartitionMapper(){
			@Override
			public int getPartition(Message message, int partitioNum) {
				byte[] body = message.getBody();
				try {
					Map obj = SerializationUtil.jsonDeserialize(new String(body), Map.class);
					Integer uid = (Integer)obj.get("uid");
					return uid % partitioNum;
				} catch (Exception e) {
					e.printStackTrace();
				}
				return 0;
			}
       });
       
       for(int i=8;i<10;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", i);
    	   obj.put("name", "name"+i);
           obj.put("type", 1);
           obj.put("opType", MqConstants.OP_ACCOUNT_CREATE);
    	   String j = SerializationUtil.jsonSerialize(obj);
    	   producer.produce(j.getBytes());
       }
       for(int i=8;i<10;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", i);
    	   obj.put("name", "name"+i);
           obj.put("type", 1);
           obj.put("opType", MqConstants.OP_ACCOUNT_REMOVE);
    	   String j = SerializationUtil.jsonSerialize(obj);
    	   producer.produce(j.getBytes());
       }

	   System.exit(1);
    }

}
