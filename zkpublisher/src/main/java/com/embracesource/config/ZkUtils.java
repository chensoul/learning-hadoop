package com.embracesource.config;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class ZkUtils {
	private static final Logger logger = Logger.getLogger(ZkUtils.class);

	public static String getZkPath(String rootNode, String key) {
		if (!StringUtils.isEmpty(rootNode)) {
			if (key.startsWith("/")) {
				key = key.substring(1);
			}
			if (rootNode.endsWith("/")) {
				return rootNode + key;
			}

			return rootNode + "/" + key;
		}

		return key;
	}

	public static void mkPaths(ZkClient client, String path) {
		String[] subs = path.split("\\/");
		if (subs.length < 2) {
			return;
		}
		String curPath = "";
		for (int i = 1; i < subs.length; i++) {
			curPath = curPath + "/" + subs[i];
			if (!client.exists(curPath)) {
				if (logger.isDebugEnabled()) {
					logger.debug("Trying to create zk node: " + curPath);
				}
				client.createPersistent(curPath);
				if (logger.isDebugEnabled())
					logger.debug("Zk node created successfully: " + curPath);
			}
		}
	}

	public static String formatAsMonthDate(Date requestTime) {
		return new SimpleDateFormat("MMdd").format(requestTime);
	}

	public static class StringSerializer implements ZkSerializer {
		private String encoding;

		public StringSerializer(String encoding) {
			this.encoding = encoding;
		}

		public Object deserialize(byte[] abyte0) throws ZkMarshallingError {
			try {
				return new String(abyte0, this.encoding);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}

		}

		public byte[] serialize(Object obj) throws ZkMarshallingError {
			if (obj == null) {
				return null;
			}

			if (!(obj instanceof String)) {
				throw new ZkMarshallingError(
						"The input obj must be an instance of String.");
			}
			try {
				return ((String) obj).getBytes(this.encoding);
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}
	}
}