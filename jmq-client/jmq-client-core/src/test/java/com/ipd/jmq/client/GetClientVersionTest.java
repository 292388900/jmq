package com.ipd.jmq.client;

import org.junit.Test;

import java.io.IOException;
import java.util.Properties;

/**
 * GetClientVersionTest
 *
 * @author luoruiheng
 * @since 12/28/16
 */
public class GetClientVersionTest {

    @Test
    public void getVersionFromPom() {
        String version = null;
        Properties properties = new Properties();

        try {
            properties.load(GetClientVersionTest.class.getResourceAsStream("/version.properties"));
            if (!properties.isEmpty()) {
                version = properties.getProperty("client.version", "1.0.0");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println(version);

    }



}
