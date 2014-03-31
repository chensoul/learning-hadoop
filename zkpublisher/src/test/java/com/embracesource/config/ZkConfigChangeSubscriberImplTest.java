//package com.embracesource.config;
//
//import java.io.IOException;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//
//import junit.framework.TestCase;
//
//import org.I0Itec.zkclient.ZkClient;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//
//public class ZkConfigChangeSubscriberImplTest extends TestCase {
//	private ZkClient zkClient;
//	ConfigChangeSubscriber zkConfig;
//
//	public void setUp() {
//		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
//				"test-spring-config.xml");
//		this.zkClient = ((ZkClient) ctx.getBean("zkClient"));
//		this.zkConfig = ((ConfigChangeSubscriber) ctx
//				.getBean("configChangeSubscriber"));
//		ZkUtils.mkPaths(this.zkClient, "/zkSample/conf");
//		if (!this.zkClient.exists("/zkSample/conf/test1.properties"))
//			this.zkClient.createPersistent("/zkSample/conf/test1.properties");
//
//		if (!this.zkClient.exists("/zkSample/conf/test2.properties"))
//			this.zkClient.createPersistent("/zkSample/conf/test2.properties");
//	}
//
//	public void testSubscribe() throws IOException, InterruptedException {
//		final CountDownLatch latch = new CountDownLatch(1);
//		this.zkConfig.subscribe("test1.properties", new ConfigChangeListener() {
//			public void configChanged(String key, String value) {
//				System.out.println("test1接收到数据变更通知: key=" + key + ", value="
//						+ value);
//				latch.countDown();
//			}
//		});
//		this.zkClient.writeData("/zkSample/conf/test1.properties", "aa=1");
//		boolean notified = latch.await(30L, TimeUnit.SECONDS);
//		if (!notified)
//			fail("客户端没有收到变更通知");
//	}
//
//	public void testA() throws InterruptedException {
//		List<String> list = this.zkClient.getChildren("/zkSample/conf");
//		for (String s : list) {
//			System.out.println("children:" + s);
//		}
//
//	}
//
//	public void testB() throws InterruptedException {
//		this.zkClient.writeData("/zkSample/conf/test2.properties", "test=123");
//		System.out.println(this.zkClient
//				.readData("/zkSample/conf/test2.properties"));
//
//	}
//
//	public void tearDown() {
//		this.zkClient.delete("/zkSample/conf/test1.properties");
//		this.zkClient.delete("/zkSample/conf/test2.properties");
//	}
//}