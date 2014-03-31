//package com.embracesource.config;
//
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//
//import junit.framework.TestCase;
//
//import org.I0Itec.zkclient.ZkClient;
//import org.springframework.context.support.ClassPathXmlApplicationContext;
//
//public class DynamicPropertiesHelperTest extends TestCase {
//	private DynamicPropertiesHelperFactory helperFactory;
//	private ZkClient zkClient;
//
//	protected void setUp() throws Exception {
//		super.setUp();
//		ClassPathXmlApplicationContext ctx = new ClassPathXmlApplicationContext(
//				"test-spring-config.xml");
//		this.helperFactory = ((DynamicPropertiesHelperFactory) ctx
//				.getBean(DynamicPropertiesHelperFactory.class));
//		this.zkClient = ((ZkClient) ctx.getBean(ZkClient.class));
//
//		if (!this.zkClient.exists("/zkSample/conf/test.properties"))
//			this.zkClient.createPersistent("/zkSample/conf/test.properties");
//	}
//
//	public void testRegisterListener() throws InterruptedException {
//		DynamicPropertiesHelper helper = this.helperFactory
//				.getHelper("test.properties");
//		final CountDownLatch latch = new CountDownLatch(1);
//		helper.registerListener("listener",
//				new DynamicPropertiesHelper.PropertyChangeListener() {
//					public void propertyChanged(String oldValue, String newValue) {
//						System.out.println("property chaged: oldValue="
//								+ oldValue + ", newValue=" + newValue);
//						latch.countDown();
//					}
//				});
//		this.zkClient.writeData("/zkSample/conf/test.properties", "test=123");
//		try {
//			if (!latch.await(5L, TimeUnit.SECONDS))
//				System.out.println("no property changed event fired in 5 seconds.");
//		} catch (InterruptedException e) {
//			e.printStackTrace();
//		}
//		assertEquals(helper.getProperty("test"), "123");
//	}
//
//	public void tearDown() {
//		this.zkClient.delete("/zkSample/conf/test.properties");
//	}
//}