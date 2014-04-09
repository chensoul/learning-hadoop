package com.embracesource.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;

/**
 * zk配置文件下载类
 * @author june
 *
 */
public class ZkConfigSaver {
	public static final String CONF_ENCODING = "UTF-8";
	public static String ZK_CONFIG_ROOTNODE = "/zkSample/conf";
	public static String ZK_CONF_ENCODING = "UTF-8";
	public static int ZK_TIMEOUT = 30000;
	public static String ZK_ADDRESS = "";

	private static final void loadProperties() {
		InputStream is = ZkConfigPublisher.class
				.getResourceAsStream("/zkpublisher.properties");
		if (is == null) {
			throw new RuntimeException("找不到config.properties资源文件.");
		}
		Properties props = new Properties();
		try {
			props.load(new BufferedReader(new InputStreamReader(is, "UTF-8")));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
		ZK_CONFIG_ROOTNODE = props.getProperty("ZK_CONFIG_ROOTNODE");
		ZK_CONF_ENCODING = props.getProperty("ZK_CONF_ENCODING");
		ZK_TIMEOUT = Integer.parseInt(props.getProperty("ZK_TIMEOUT"));
		ZK_ADDRESS = props.getProperty("ZK_ADDRESS");
	}

	public static void main(String[] args) {
		if ((args == null) || (args.length < 1)) {
			throw new RuntimeException("需要指定输出目录名");
		}
		loadProperties();

		ZkClient client = new ZkClient(ZK_ADDRESS, ZK_TIMEOUT);
		client.setZkSerializer(new ZkUtils.StringSerializer(ZK_CONF_ENCODING));

		File confDir = new File(args[0]);
		confDir.mkdirs();

		saveConfigs(client, ZK_CONFIG_ROOTNODE, confDir);
	}

	private static void saveConfigs(ZkClient client, String rootNode,
			File confDir) {
		List<String> configs = client.getChildren(rootNode);
		for (String config : configs) {
			String content = (String) client.readData(rootNode + "/" + config);
			File confFile = new File(confDir, config);
			try {
				FileUtils.writeStringToFile(confFile, content, "UTF-8");
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("配置成功保存到本地: " + confFile.getAbsolutePath());
		}
	}
}