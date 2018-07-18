package com.gleasy.library.cloud.mq;

import com.gleasy.library.cloud.mq.domain.Message;

public class DefaultPubsubPartitionMapper implements PartitionMapper{
	private Integer index = 0;
	private Integer indexLock = 0;
	
	@Override
	public int getPartition(Message message, int partitioNum) {
		synchronized(indexLock){
			index ++;
			if(index > 999999) index  = 0;
			Integer order = index % partitioNum;
			return order;
		}
	}

}
