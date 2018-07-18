package com.gleasy.library.cloud.mq.example.topic;

import java.util.HashMap;
import java.util.Map;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;

public class SampleTopicProducer {
	
    public static void main(String[] args) throws Exception {
       Config.setConfig("e:\\j2ee\\gleasy.com\\config\\util\\config.properties");
       ZkConfig zkConfig = new ZkConfig("mq.test.schema");

       String topic = "topic1";//订阅主题
       
       //MessageProducer尽量复用,强烈建议使用单例
       MessageProducer producer = new MessageProducer(zkConfig,topic);
   
       for(int i=0;i<100;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", 10);
    	   obj.put("name", "name"+i);
    	   String j = SerializationUtil.jsonSerialize(obj);
    	   producer.produce(j.getBytes());
       }
       
	   System.exit(1);
    }

}
