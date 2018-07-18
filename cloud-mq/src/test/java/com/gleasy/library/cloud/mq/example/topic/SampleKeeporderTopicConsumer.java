package com.gleasy.library.cloud.mq.example.topic;

import java.util.concurrent.Executor;

import com.gleasy.library.cloud.mq.MessageHandler;
import com.gleasy.library.cloud.mq.PubsubMessageConsumer;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;

public class SampleKeeporderTopicConsumer {
	
    public static void main(String[] args) throws Exception {
    	Config.setConfig("E:\\j2ee\\gleasy.com\\config\\util\\config.properties");
        ZkConfig zkConfig = new ZkConfig("mq.test.schema");

        String topic = "topic1";//订阅主题
        
        String subscriber = "user1";//订阅者id

        new PubsubMessageConsumer(zkConfig,subscriber,topic,50, new MessageHandler() {
            public void handle(Message message) {
                System.out.println("Receive:" + new String(message.getBody()));
            }

			@Override
			public Executor getExecutor() {
				//严格保证顺序的情况下,是不能使用多线程处理的
				return null;
			}
        });
        
       
    }
}

