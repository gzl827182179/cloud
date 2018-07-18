package com.gleasy.library.cloud.mq;

import com.gleasy.library.cloud.mq.domain.Message;

public interface PartitionMapper {
	public int getPartition(Message message, int partitioNum);
}
