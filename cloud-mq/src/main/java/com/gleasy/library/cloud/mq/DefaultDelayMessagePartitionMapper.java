package com.gleasy.library.cloud.mq;

import com.gleasy.library.cloud.mq.domain.DelayMessage;
import com.gleasy.library.cloud.mq.domain.Message;

public class DefaultDelayMessagePartitionMapper implements PartitionMapper{
	@Override
	public int getPartition(Message message, int partitioNum) {
		DelayMessage m = (DelayMessage) message;
		int hash = m.getUuid().hashCode();
		if(hash < 0){
			hash = -hash;
		}
		return  hash % partitioNum;
	}

}
