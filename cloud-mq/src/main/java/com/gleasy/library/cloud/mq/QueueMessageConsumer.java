package com.gleasy.library.cloud.mq;

import com.gleasy.library.cloud.mq.util.ZkConfig;


public class QueueMessageConsumer extends PubsubMessageConsumer {
	
	public QueueMessageConsumer(ZkConfig zkConfig, String topic, int batchsize,
			MessageHandler handler) {
		super(zkConfig,topic+"-cloudmq-consumer", topic, batchsize, handler);
	}

}
