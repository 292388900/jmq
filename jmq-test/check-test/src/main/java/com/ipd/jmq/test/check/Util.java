package com.ipd.jmq.test.check;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public class Util {

	private static Map<String, String> map = new HashMap<String, String>();

	public static AtomicBoolean IS_STOP = new AtomicBoolean();

	public static void main(String[] args) {
		System.out.println(new File("").getAbsolutePath());
	}

	public static String get(String key, String defaultValue) {
		if (map.isEmpty()) {
			read();
		}
		return map.get(key) == null ? defaultValue : map.get(key);
	}

	static void read() {
		Properties prop = new Properties();
		InputStream in = Util.class.getResourceAsStream("/check.properties");
		if (in == null) {
			in = Util.class.getResourceAsStream("check.properties");
		}
		try {
			prop.load(in);
			Enumeration en = prop.propertyNames();
			while (en.hasMoreElements()) {
				String key = (String) en.nextElement();
				String value = prop.getProperty(key);
				map.put(key, value);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
				}
			}
		}
	}



}
