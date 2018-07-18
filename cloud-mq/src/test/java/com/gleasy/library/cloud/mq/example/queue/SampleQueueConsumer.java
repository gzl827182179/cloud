package com.gleasy.library.cloud.mq.example.queue;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.gleasy.library.cloud.mq.MessageHandler;
import com.gleasy.library.cloud.mq.QueueMessageConsumer;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;

public class SampleQueueConsumer {
	
    public static void main(String[] args) throws Exception {
    	Config.setConfig("e:\\j2ee\\gleasy.com\\config\\util\\config.properties");
    	
    	ZkConfig zkConfig = new ZkConfig("mqtest");
    	
        String topic = "index";//订阅主题
        final Executor executor = Executors.newFixedThreadPool(50);
        new QueueMessageConsumer(zkConfig,topic,50, new MessageHandler() {
            public void handle(Message message) {
                System.out.println("Receive:" + new String(message.getBody()));
            }

			@Override
			public Executor getExecutor() {
				return executor;
			}
        });
        
       
    }
}

