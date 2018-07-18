package com.gleasy.library.cloud.mq.example.user;

import com.gleasy.library.cloud.mq.MessageProducer;
import com.gleasy.library.cloud.mq.util.ZkConfig;
import com.gleasy.util.Config;
import com.gleasy.util.SerializationUtil;
import com.gleasy.util.consts.MqConstants;

import java.util.HashMap;
import java.util.Map;

public class InviteRegTopicProducer {

    public static void main(String[] args) throws Exception {
        Config.setConfig("D:\\studio\\gleasy\\config\\ucenter\\config_0_6.properties");

        ZkConfig zkConfig = new ZkConfig(MqConstants.SCHEMA_UCENTER);

        String topic = MqConstants.TOPIC_REG_INVITED;//订阅主题

        //MessageProducer尽量复用,强烈建议使用单例
        MessageProducer producer = new MessageProducer(zkConfig, topic);


        Map obj = new HashMap();
        obj.put("uid", 111);
        String j = SerializationUtil.jsonSerialize(obj);
        producer.produce(j.getBytes());

        System.exit(1);
    }

}
