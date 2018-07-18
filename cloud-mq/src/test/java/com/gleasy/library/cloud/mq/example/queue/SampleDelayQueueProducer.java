package com.gleasy.library.cloud.mq.example.queue;

import java.util.HashMap;
import java.util.Map;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.domain.DelayMessage;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.SerializationUtil;

public class SampleDelayQueueProducer {
	
    public static void main(String[] args) throws Exception {
       ZkConfig zkConfig = new ZkConfig("mqtest");
       String topic = "index";//发布的主题
       MessageProducer producer = new MessageProducer(zkConfig,topic);
   
       for(int i=0;i<100;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", 10);
    	   obj.put("name", "name"+i);
    	   String j = SerializationUtil.jsonSerialize(obj);
    	   //producer.produce(j.getBytes());
    	   
    	   String uuid = "myidxxx"+i;
    	   DelayMessage message = new DelayMessage(uuid,j.getBytes(),15000);
    	   producer.produce(message);
       }
       
	   System.exit(1);
    }

}
