package com.gleasy.library.cloud.mq.example.user;

import com.gleasy.library.cloud.mq.util.MqConfigUtil;
import com.gleasy.library.cloud.mq.util.TopicConfig;
import com.gleasy.util.Config;
import com.gleasy.util.consts.MqConstants;

public class ConfigPromoterMq {
    private static String redisSchema = "mq.redis.ucenter";
    private static String zkSchema = MqConstants.SCHEMA_UCENTER;



	public static void main(String[] args) throws Exception{
		Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_11.properties");
		

		TopicConfig config = new TopicConfig();
		config.setMessageStoreSchema(redisSchema);
		config.setMode(TopicConfig.MODE_TOPIC);
		//config.setMessageLifetime(30);//单位s,为测试起见,消息的失效时间设了30s,正常使用情况下,至少要设为1天以上
		
		MqConfigUtil.getInstance(zkSchema).configTopic(MqConstants.TOPIC_DOMAIN_BINDED, config);
		System.exit(0);
	}
}
