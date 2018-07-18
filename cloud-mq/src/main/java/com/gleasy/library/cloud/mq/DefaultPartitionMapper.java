package com.gleasy.library.cloud.mq;

import com.gleasy.library.cloud.mq.domain.Message;

public class DefaultPartitionMapper implements PartitionMapper{

	@Override
	public int getPartition(Message message, int partitioNum) {
		Long order = message.getId() % partitioNum;
		return order.intValue();
	}

}
