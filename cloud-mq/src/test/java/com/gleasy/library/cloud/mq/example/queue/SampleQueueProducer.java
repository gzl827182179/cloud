package com.gleasy.library.cloud.mq.example.queue;

import java.util.HashMap;
import java.util.Map;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;

public class SampleQueueProducer {
	
    public static void main(String[] args) throws Exception {
    	Config.setConfig("E:\\j2ee\\gleasy.com\\config\\util\\config.properties");
    	
    	ZkConfig zkConfig = new ZkConfig("mqtest");

       String topic = "index";//发布的主题
       
       //MessageProducer尽量复用,强烈建议使用单例
       MessageProducer producer = new MessageProducer(zkConfig,topic);
   
       for(int i=0;i<100000;i++){
    	   Map obj = new HashMap();
    	   obj.put("uid", 10);
    	   obj.put("name", "name"+i);
    	   String j = SerializationUtil.jsonSerialize(obj);
    	   producer.produce(j.getBytes());
       }
       
	   System.exit(1);
    }

}
