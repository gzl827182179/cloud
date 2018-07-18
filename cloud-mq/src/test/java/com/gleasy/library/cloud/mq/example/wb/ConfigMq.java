package com.gleasy.library.cloud.mq.example.wb;

import com.gleasy.library.cloud.mq.util.MqConfigUtil;
import com.gleasy.library.cloud.mq.util.TopicConfig;
import com.gleasy.library.cloud.util.CloudException;
import com.gleasy.library.cloud.util.ZkFactory;
import com.gleasy.library.redis.shard.ConfigLoader;
import com.gleasy.library.redis.shard.ShardCluster;
import com.gleasy.util.Config;
import com.gleasy.util.consts.MqConstants;

import java.util.ArrayList;
import java.util.List;

public class ConfigMq {
	private static String redisSchema = "mq.redis.wb";
	private static String zkSchema = MqConstants.SCHEMA_WB;

	private static void configRedis() throws Exception{
		List<ShardCluster> clusters = new ArrayList();
		ShardCluster cluster = new ShardCluster();
		cluster.setIp("192.168.0.11");
		cluster.setPort(6678);
		cluster.setMin(0);
		cluster.setMax(20);
		cluster.setRw("W");
        cluster.setDb("2");
		cluster.setType(ShardCluster.TYPE_LOCAL);
		clusters.add(cluster);
		
		cluster = new ShardCluster();
        cluster.setIp("192.168.0.11");
        cluster.setPort(6678);
		cluster.setRw("R");
		cluster.setMin(0);
		cluster.setMax(20);
        cluster.setDb("2");
		cluster.setType(ShardCluster.TYPE_LOCAL);
		clusters.add(cluster);
		
		ConfigLoader.getInstance().setConfig(redisSchema, clusters);
		clusters = ConfigLoader.getInstance().getClusters(redisSchema);
		for(ShardCluster c:clusters){
			System.out.println(c);
		}
		
	}
	
	private static void configZk() throws CloudException{
		ZkFactory.setAddress(zkSchema,"192.168.0.11:2181");
		ZkFactory.setRoot(zkSchema, "/wb");
		ZkFactory.setTimeout(zkSchema,90000);
		
		System.out.println(ZkFactory.getAddress(zkSchema));
		System.out.println(ZkFactory.getRoot(zkSchema));
		
	}
	
	public static void main(String[] args) throws Exception{
		Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_11.properties");
		
		configZk();
		configRedis();
		
		TopicConfig config = new TopicConfig();
		config.setMessageStoreSchema(redisSchema);
		config.setMode(TopicConfig.MODE_QUEUE);
		//config.setMessageLifetime(30);//单位s,为测试起见,消息的失效时间设了30s,正常使用情况下,至少要设为1天以上
		
		MqConfigUtil.getInstance(zkSchema).configTopic(MqConstants.QUEUE_OA_ACTIVITY_SEND, config);
		MqConfigUtil.getInstance(zkSchema).configTopic(MqConstants.QUEUE_OA_APPROVAL_SEND, config);
		MqConfigUtil.getInstance(zkSchema).configTopic(MqConstants.QUEUE_OA_TASK_SEND, config);
		MqConfigUtil.getInstance(zkSchema).configTopic(MqConstants.QUEUE_OA_VOTE_SEND, config);
		System.exit(0);
	}
}
