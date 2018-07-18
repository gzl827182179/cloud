package com.gleasy.library.cloud.mq;

import java.util.concurrent.Executor;

import com.gleasy.library.cloud.mq.domain.Message;

public interface MessageHandler {
	public void handle(Message message);
	
	public Executor getExecutor();
	
}
