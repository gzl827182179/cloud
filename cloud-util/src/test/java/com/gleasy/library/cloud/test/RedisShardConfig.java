package com.gleasy.library.cloud.test;

import java.util.ArrayList;
import java.util.List;

import com.gleasy.library.redis.shard.ConfigLoader;
import com.gleasy.library.redis.shard.ShardCluster;
import com.gleasy.util.Config;

public class RedisShardConfig {
	public static void main(String[] args) throws Exception{
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config.properties");
		List<ShardCluster> clusters = new ArrayList();
		ShardCluster cluster = new ShardCluster();
		cluster = new ShardCluster();
		cluster.setIp("192.168.0.6");
		cluster.setPort(6379);
		cluster.setName("jobtest-global");
		cluster.setRw("W");
		cluster.setType(ShardCluster.TYPE_GLOBAL);
		clusters.add(cluster);
		
		cluster = new ShardCluster();
		cluster.setIp("192.168.0.6");
		cluster.setPort(6379);
		cluster.setName("jobtest-global");
		cluster.setRw("R");
		cluster.setType(ShardCluster.TYPE_GLOBAL);
		clusters.add(cluster);
		
		ConfigLoader.getInstance().setConfig("jobtest", clusters);
		clusters = ConfigLoader.getInstance().getClusters("jobtest");
		for(ShardCluster c:clusters){
			System.out.println(c);
		}
		/*
		Config.setConfig("D:\\j2ee\\gleasy.com\\config\\util\\config_0_175.properties");
		List<ShardCluster> clusters = new ArrayList();
		ShardCluster cluster = new ShardCluster();
		cluster = new ShardCluster();
		cluster.setIp("222.76.218.175");
		cluster.setPort(6379);
		cluster.setName("fconv-global");
		cluster.setRw("W");
		cluster.setType(ShardCluster.TYPE_GLOBAL);
		clusters.add(cluster);
		
		cluster = new ShardCluster();
		cluster.setIp("222.76.218.175");
		cluster.setPort(6379);
		cluster.setName("fconv-global");
		cluster.setRw("R");
		cluster.setType(ShardCluster.TYPE_GLOBAL);
		clusters.add(cluster);
		
		ConfigLoader.getInstance().setConfig("fconv", clusters);
		clusters = ConfigLoader.getInstance().getClusters("fconv");
		for(ShardCluster c:clusters){
			System.out.println(c);
		}
		System.exit(1);*/
	}
}
