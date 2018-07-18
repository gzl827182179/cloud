package com.gleasy.library.cloud.mq.example.queue;

import java.util.ArrayList;
import java.util.List;

import com.gleasy.library.cloud.mq.util.MqConfigUtil;
import com.gleasy.library.cloud.mq.util.TopicConfig;
import com.gleasy.library.cloud.util.CloudException;
import com.gleasy.library.cloud.util.ZkFactory;
import com.gleasy.library.redis.shard.ConfigLoader;
import com.gleasy.library.redis.shard.ShardCluster;
import com.gleasy.util.Config;

public class ConfigMq {
	private static String redisSchema = "mqtest";
	private static String zkSchema = "mqtest";
	private static String zkRoot = "/mqtest";
	private static String topic = "index";
	private static int queueNum = 2;//分片数量.分片的设置对性能影响非常大,如果不懂其原理请联系xueke
	
	private static void configRedis() throws Exception{
		List<ShardCluster> clusters = new ArrayList();
		ShardCluster cluster = new ShardCluster();
		cluster.setIp("192.168.0.12");
		cluster.setPort(6680);
		cluster.setMin(0);
		cluster.setDb("15");
		cluster.setMax(queueNum);
		cluster.setRw("W");
		cluster.setType(ShardCluster.TYPE_LOCAL);
		clusters.add(cluster);
		
		cluster = new ShardCluster();
		cluster.setIp("192.168.0.12");
		cluster.setPort(6680);
		cluster.setRw("R");
		cluster.setMin(0);
		cluster.setDb("15");
		cluster.setMax(queueNum);
		cluster.setType(ShardCluster.TYPE_LOCAL);
		clusters.add(cluster);
		
		
		ConfigLoader.getInstance().setConfig(redisSchema, clusters);
		clusters = ConfigLoader.getInstance().getClusters(redisSchema);
		for(ShardCluster c:clusters){
			System.out.println(c);
		}
		
	}
	
	private static void configZk() throws CloudException{
		ZkFactory.setAddress(zkSchema,"192.168.0.12:2181");
		ZkFactory.setRoot(zkSchema, zkRoot);
		ZkFactory.setTimeout(zkSchema,10000);
		
		System.out.println(ZkFactory.getAddress(zkSchema));
		System.out.println(ZkFactory.getRoot(zkSchema));
	}
	

	
	public static void main(String[] args) throws Exception{
		Config.setConfig("E:\\j2ee\\gleasy.com\\config\\util\\config.properties");
		
		configZk();
		configRedis();
		
		TopicConfig config = new TopicConfig();
		config.setMessageStoreSchema(redisSchema);
		config.setMode(TopicConfig.MODE_QUEUE);
		
		MqConfigUtil.getInstance(zkSchema).configTopic(topic, config);
		System.exit(1);
	}
}
