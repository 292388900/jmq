package com.ipd.jmq.demo.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * PropertiesUtil
 *
 * @author luoruiheng
 * @since 8/2/16
 */
public class TxStatusUtil {

    private static Logger logger = LoggerFactory.getLogger(TxStatusUtil.class);


    protected static String get(String key, String defaultValue) {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(System.getProperty("user.dir") + "/src/test/resources/querier/tx_status.properties");
            prop.load(in);
            return prop.getProperty(key, defaultValue);
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
        return defaultValue;
    }

    protected static void set(String key, String value) throws Exception {
        Properties properties = new Properties();
        OutputStream output = null;
        try {
            output = new FileOutputStream(
                    System.getProperty("user.dir") + "/src/test/resources/querier/tx_status.properties",
                    true);
            properties.setProperty(key, value);
            properties.store(output, null);
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
            throw e;
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
            throw e;
        } finally {
            if (null != output) {
                try {
                    output.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

    }

    protected static void set(Map<String, String> keyAndValue) {
        Properties properties = new Properties();
        OutputStream output = null;
        try {
            output = new FileOutputStream(
                    System.getProperty("user.dir") + "/src/test/resources/querier/tx_status.properties",
                    true);
            for (String key : keyAndValue.keySet()) {
                properties.setProperty(key, keyAndValue.get(key));
            }
            properties.store(output, "store time: " + new Date());
        } catch (FileNotFoundException e) {
            logger.error(e.getMessage(), e);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        } finally {
            if (null != output) {
                try {
                    output.close();
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                }
            }
        }

    }


}
