package com.ipd.jmq.client.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * ExceptionsReader
 *
 * @author luoruiheng
 * @since 12/7/16
 */
public class ExceptionsReader {

    // TODO: 12/21/16 link url to jmq-web for dynamic configuration

    // slf4j logger
    private static final Logger logger = LoggerFactory.getLogger(ExceptionsReader.class);
    // map
    private static Map<String, String> map = new HashMap<String, String>();



    public static String get(String key, String defaultValue) {
        if (map.isEmpty()) {
            // read from file
            readFromProperties();
        }

        return map.get(key) == null ? defaultValue : map.get(key);
    }

    private static void readFromProperties() {
        Properties prop = new Properties();
        InputStream in = ExceptionsReader.class.getResourceAsStream("/conf/exception-handle.properties");
        if (in == null) {
            in = ExceptionsReader.class.getResourceAsStream("exception-handle.properties");
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
            logger.error(e.getMessage(), e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }
    }

}
