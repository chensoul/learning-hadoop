package com.embracesource.config;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.io.FileUtils;

/**
 * zk配置文件发布者类
 * @author june
 *
 */
public class ZkConfigPublisher {
	public static String CONF_DIR = "conf";
	public static final String CONF_ENCODING = "UTF-8";
	public static String ZK_CONFIG_ROOTNODE = "/zkSample/conf";
	public static String ZK_CONF_ENCODING = "UTF-8";
	public static int ZK_TIMEOUT = 30000;
	public static String ZK_ADDRESS = "";

	private static final void loadProperties() {
		InputStream is = ZkConfigPublisher.class
				.getResourceAsStream("/zkpublisher.properties");
		if (is == null) {
			throw new RuntimeException("找不到zkpublisher.properties资源文件.");
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
		loadProperties();

		ZkClient client = new ZkClient(ZK_ADDRESS, ZK_TIMEOUT);
		client.setZkSerializer(new ZkUtils.StringSerializer(ZK_CONF_ENCODING));

		File confDir = new File(CONF_DIR);
		if ((!confDir.exists()) || (!confDir.isDirectory())) {
			System.err.println("错误： 配置目录" + confDir + "不存在或非法! ");
			System.exit(1);
		}

		publishConfigs(client, ZK_CONFIG_ROOTNODE, confDir);
	}

	private static void publishConfigs(ZkClient client, String rootNode,
			File confDir) {
		File[] confs = confDir.listFiles();
		int success = 0;
		int failed = 0;
		for (File conf : confs) {
			if (!conf.isFile()) {
				continue;
			}
			String name = conf.getName();
			String path = ZkUtils.getZkPath(rootNode, name);
			ZkUtils.mkPaths(client, path);
			String content;
			try {
				content = FileUtils.readFileToString(conf, "UTF-8");
			} catch (IOException e) {
				System.err.println("错误: 读取文件内容时遇到异常:" + e.getMessage());
				failed++;
				continue;
			}
			if (!client.exists(path)) {
				try {
					client.createPersistent(path);
					client.writeData(path, content);
				} catch (Throwable e) {
					System.err.println("错误: 尝试发布配置失败: " + e.getMessage());
					failed++;
					continue;
				}
				System.out.println("提示: 已经成功将配置文件" + conf + "内容发布为新的ZK配置"
						+ path);
			} else {
				try {
					client.writeData(path, content);
				} catch (Throwable e) {
					System.err.println("错误: 尝试发布配置失败: " + e.getMessage());
					failed++;
					continue;
				}
				System.out.println("提示: 已经成功将配置文件" + conf + "内容更新到ZK配置" + path);
			}
			success++;
		}
		System.out.println("提示: 完成配置发布，成功" + success + "，失败" + failed + "。");
	}
}