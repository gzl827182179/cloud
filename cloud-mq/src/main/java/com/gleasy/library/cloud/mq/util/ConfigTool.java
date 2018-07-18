package com.gleasy.library.cloud.mq.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import com.gleasy.library.cloud.util.CloudException;
import com.gleasy.library.cloud.util.ZkFactory;
import com.gleasy.util.Config;
import com.gleasy.util.Util;

public class ConfigTool {
	public static void main(String[] args){
		System.out.println(args.length);
		if(args.length < 2){
			System.out.println("Usage: ConfigTool config.property import|export a.dat {zkSchema} {topicName}");
			System.exit(1);
		}
		String config = args[0];
		Config.setConfig(config);
		String act = args[1];
		
		String filepath = args[2];
		
		if("import".equalsIgnoreCase(act)){
			try {
				doImport(filepath);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.exit(1);
		}else if("export".equalsIgnoreCase(act)){
			try {
				String zkSchema = args[3];
				String topicName = args[4];
				doExport(filepath,zkSchema,topicName);
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.exit(1);
		}else{
			System.out.println("Usage: ConfigTool config.property import|export a.dat {zkSchema} {topicName}");
			System.exit(1);
		}
	}
	
	private static void doImport(String filePath) throws IOException{
		File f = new File(filePath);
		if(!f.exists()) {
			System.out.println("文件"+filePath+"不存在");
			System.exit(1);
		}
		String storeSchema = null;
		String mode = null;
		String lifetime = null;
		String topicName = null;
		String zkSchema = null;
		
		List<String> lines = FileUtils.readLines(f, "UTF-8");
		for(String line : lines){
			if(Util.isEmpty(line)) continue;
			
			int idx = line.indexOf("=");
			String key = line.substring(0, idx);
			String value = line.substring(idx+1);
			if(Util.isEmpty(key) || Util.isEmpty(value)) continue;
			key = key.trim();
			value = value.trim();
			if(line.startsWith("gleasy:mq:mode:")){
				mode = value;
			}
			if(line.startsWith("gleasy:mq:lifetime:")){
				lifetime = value;
			}
			if(line.startsWith("gleasy:mq:topicName:")){
				topicName = value;
			}
			if(line.startsWith("gleasy:mq:redis")){
				storeSchema = value;
			}
			if(line.startsWith("gleasy:mq:zk")){
				zkSchema = value;
			}
			if(Util.isEmpty(key) || Util.isEmpty(value)) continue;
			Config.set(key, value);
			System.out.println("写入 "+key+" = "+value);
		}
		
		System.out.println("redis schema : "+storeSchema);
		
		System.out.println("zkSchema : "+zkSchema);
		System.out.println("topicName : "+topicName);
		
		TopicConfig config = new TopicConfig();
		config.setMessageStoreSchema(storeSchema);
		if(mode != null){
			System.out.println("mode : "+mode);
			config.setMode(mode);
		}
		if(lifetime != null){
			System.out.println("lifetime : "+lifetime);
			config.setMessageLifetime(Integer.parseInt(lifetime));
		}
		MqConfigUtil.getInstance(zkSchema).configTopic(topicName, config);
	}
	
	private static void doExport(String filePath,String zkSchema,String topicName) throws IOException{
		File f = new File(filePath);
		
		TopicConfig topicConfig = MqConfigUtil.getInstance(zkSchema).getConfig(topicName);
		String storeSchema = topicConfig.getMessageStoreSchema();
		
		List<String> lines = new ArrayList();
		
		String configKey = "gleasy.redis.shard."+storeSchema;
		String value = configKey+"="+Config.get(configKey);
		lines.add(value);
		
		lines.add("gleasy:mq:mode:"+topicName+"="+topicConfig.getMode());
		lines.add("gleasy:mq:lifetime:"+topicName+"="+topicConfig.getMessageLifetime());
		lines.add("gleasy:mq:topicName:"+topicName+"="+topicName);
		lines.add("gleasy:mq:redis:"+topicName+"="+storeSchema);
		lines.add("gleasy:mq:zk:"+topicName+"="+zkSchema);
		
		try {
			lines.add("zk:addr:"+zkSchema+"="+ZkFactory.getAddress(zkSchema));
			lines.add("zk:root:"+zkSchema+"="+ZkFactory.getRoot(zkSchema));
			lines.add("zk:timeout:"+zkSchema+"="+ZkFactory.getTimeout(zkSchema));
		} catch (CloudException e) {
			e.printStackTrace();
			System.exit(1);
		}
		FileUtils.writeLines(f, "UTF-8", lines);
	}
}
