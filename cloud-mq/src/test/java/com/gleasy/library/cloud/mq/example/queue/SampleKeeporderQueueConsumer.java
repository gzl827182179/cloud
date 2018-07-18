package com.gleasy.library.cloud.mq.example.queue;

import java.util.concurrent.Executor;

import com.gleasy.library.cloud.mq.MessageHandler;
import com.gleasy.library.cloud.mq.QueueMessageConsumer;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;

public class SampleKeeporderQueueConsumer {
	
    public static void main(String[] args) throws Exception {
    	ZkConfig zkConfig = new ZkConfig("mqtest");
    	
        String topic = "index";//订阅主题

        new QueueMessageConsumer(zkConfig,topic,50, new MessageHandler() {
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

