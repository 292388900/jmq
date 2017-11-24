package com.ipd.jmq.server.broker.registry;


import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.RegistryFactory;
import com.ipd.jmq.toolkit.URL;

/**
 * Created by llw on 15-7-16.
 */
public class WebRegistryFactory extends RegistryFactory {

    private static final String type = "web";

    /**
     * example:
     * web://localhost:8080/jmq-web/webRegistry
     */
    private String url;

    @Override
    public Registry create() throws RegistryException {

        return new WebRegistry(url.toString().replaceFirst(type,"http"));
    }


    @Override
    public void setUrl(String url) {
        this.url = url;
    }

    public static void main(String[] args) {
        URL url = URL.valueOf("web://localhost:8080/jmq-web/webRegistry");

        System.out.println(url.getProtocol());
        System.out.println(url.getHost());
        System.out.println(url.getPort());
        System.out.println(url.getAbsolutePath());
        System.out.println(url.getAddress());
        System.out.println(url.toString().replaceFirst(type,"http"));
    }
}
