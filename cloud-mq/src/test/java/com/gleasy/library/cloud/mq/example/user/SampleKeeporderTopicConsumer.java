package com.gleasy.library.cloud.mq.example.user;

import com.gleasy.library.cloud.mq.MessageHandler;
import com.gleasy.library.cloud.mq.PubsubMessageConsumer;
import com.gleasy.library.cloud.mq.domain.Message;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;
import com.gleasy.util.consts.MqConstants;

import java.util.Map;
import java.util.concurrent.Executor;

public class SampleKeeporderTopicConsumer {

    public SampleKeeporderTopicConsumer() {
    }

    public static void main(String[] args) throws Exception {
//        Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_6.properties");
        Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_175.properties");

    	ZkConfig zkConfig = new ZkConfig(MqConstants.SCHEMA_UCENTER);
    	
    	String topic = MqConstants.TOPIC_DEPARTMENT_OP;//订阅主题
        String subscriber = "asdfasdfas";

        new PubsubMessageConsumer(zkConfig, subscriber, topic,50, new MessageHandler() {
            public void handle(Message message) {
                System.out.println("Receive:" + new String(message.getBody()));
                try {
                    Map param = SerializationUtil.jsonDeserialize(new String(message.getBody()), Map.class);
                    String opType = (String) param.get(MqConstants.OP_TYPE);
                    System.out.println("optype:" + opType);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            @Override
            public Executor getExecutor() {
                //严格保证顺序的情况下,是不能使用多线程处理的
                return null;
            }
        });
        /*new QueueMessageConsumer(zkConfig, topic,50, new MessageHandler() {
            public void handle(Message message) {
                System.out.println("Receive:" + new String(message.getBody()));
            }

			@Override
			public Executor getExecutor() {
				//严格保证顺序的情况下,是不能使用多线程处理的
				return null;
			}
        });*/
        
       
    }
}

