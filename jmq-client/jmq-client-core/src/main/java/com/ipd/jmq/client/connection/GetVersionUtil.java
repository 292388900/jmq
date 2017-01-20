package com.ipd.jmq.client.connection;

import java.io.IOException;
import java.util.Properties;

/**
 * Get Version From pom.xml
 *
 * @author luoruiheng
 * @since 12/28/16
 */
public class GetVersionUtil {

    public static String getClientVersion() {
        String version = null;
        Properties properties = new Properties();

        try {
            properties.load(GetVersionUtil.class.getResourceAsStream("/conf/version.properties"));
            if (!properties.isEmpty()) {
                version = properties.getProperty("client.version", "1.0.0");
            }
        } catch (IOException ignored) {

        }

        return version == null || version.isEmpty() ? "1.0.0" : version;
    }

}
