package com.ipd.jmq.server.broker.registry;

import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.*;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.io.Compressors;
import com.ipd.jmq.toolkit.io.Zip;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.Args;
import org.apache.http.util.ByteArrayBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by llw on 15-7-15.
 */
public class WebRegistry implements Registry, Runnable {

    protected static Logger logger = LoggerFactory.getLogger(WebRegistry.class);

    private static final String type = "web";

    //后缀
    private final static String urlSurfix = ".do";

    private int connectTimeout = 5000;
    private int requestTimeout = 15000;


    private String urlBase;
    private boolean isCompress = false;

    private Map<String, PathListener> paths = new HashMap<String, PathListener>();
    private Set<ConnectionListener> connects = new HashSet<ConnectionListener>();
    private Map<String, List<LeaderListener>> leaders = new HashMap<String, List<LeaderListener>>();
    private Map<String, LeaderListener> currentLeader = new HashMap<String, LeaderListener>();

    private Lock lock = new ReentrantLock();

    public WebRegistry() {

    }

    public WebRegistry(String url) {
        this.urlBase = url;
    }


    public void run() {

        if (paths.size() > 0) {
            for (Map.Entry<String, PathListener> entry : paths.entrySet()) {
                pullData(entry.getKey(), entry.getValue());
            }
        } else {
            logger.error("--registry paths is empty--");
        }

    }

    private void pullData(String path, PathListener listener) {

        try {
            byte[] data = requestByteData(path);
            PathEvent pathEvent = new PathEvent(PathEvent.PathEventType.UPDATED, path, data);
            listener.onEvent(pathEvent);
            if (logger.isDebugEnabled()) {
                logger.debug("--pull data success--" + path);
            }

        } catch (Exception e) {
            logger.error("--pull data error--" + path, e);
        }

    }


    @Override
    public URL getUrl() {
        return null;
    }


    private boolean opened = false;

    @Override
    public void start() throws Exception {
        request("/open");
        opened = true;
        for (ConnectionListener listener : connects) {
            listener.onEvent(new ConnectionEvent(ConnectionEvent.ConnectionEventType.CONNECTED, new URL(null, null, 0)));
        }
    }

    @Override
    public boolean isConnected() {
        return opened;
    }

    @Override
    public void stop() {

    }

    @Override
    public void create(String s, byte[] bytes) throws RegistryException {

    }

    @Override
    public void create(List<String> list) throws RegistryException {

    }

    public boolean isCompress() {
        return isCompress;
    }

    public void setCompress(boolean isCompress) {
        this.isCompress = isCompress;
    }

    @Override
    public void createLive(String s, byte[] bytes) {

    }

    @Override
    public void deleteLive(String s) {

    }

    @Override
    public Lock createLock(String s) {
        return null;
    }

    @Override
    public void update(String s, byte[] bytes) throws RegistryException {

    }

    public void update(PathData data) throws RegistryException {

    }

    @Override
    public void update(String s, byte[] bytes, byte[] bytes1) throws RegistryException {

    }

    @Override
    public void delete(String s) throws RegistryException {

    }

    @Override
    public void delete(List<String> list) throws RegistryException {

    }

    @Override
    public boolean exists(String s) throws RegistryException {
        return false;
    }

    @Override
    public boolean isLeader(String s) throws RegistryException {
        return false;
    }

    @Override
    public PathData getData(String s) throws RegistryException {

        try {
            byte[] bytes = requestByteData(s);

            return new PathData(s, bytes);

        } catch (Exception e) {
            throw new RegistryException(e);
        }
    }

    @Override
    public List<PathData> getChildData(String s) throws RegistryException {
        return null;
    }

    @Override
    public List<String> getChildren(String s) throws RegistryException {
        return null;
    }

    @Override
    public void addListener(String s, ChildrenListener childrenListener) {

    }

    @Override
    public void addListener(String s, ChildrenDataListener childrenDataListener) {

    }


    @Override
    public void addListener(String s, PathListener pathListener) {
        paths.put(s, pathListener);
    }

    @Override
    public void addListener(String s, LeaderListener leaderListener) {

        if (s == null || s.trim().length() == 0 || leaderListener == null) {
            return;
        }

        lock.lock();

        try {

            List<LeaderListener> leaderListeners = leaders.get(s);
            if (leaderListeners == null) {
                leaderListeners = new ArrayList<LeaderListener>();
                leaders.put(s, leaderListeners);
            }


            leaderListeners.add(leaderListener);
            if (leaderListeners.size() == 1 && leaderListeners.get(0) == leaderListener) {
                //选举第一个作为leader
                selectLeader(s, leaderListener);
            }

        } finally {
            lock.unlock();
        }
    }

    @Override
    public void addListener(String s, ClusterListener clusterListener) {

    }

    @Override
    public void addListener(ConnectionListener connectionListener) {
        connects.add(connectionListener);
    }

    @Override
    public void removeListener(String s, PathListener pathListener) {
        paths.remove(s);
    }

    @Override
    public void removeListener(String s, ChildrenListener childrenListener) {

    }

    @Override
    public void removeListener(String s, ChildrenDataListener childrenDataListener) {

    }

    @Override
    public void removeListener(String s, LeaderListener leaderListener) {

        lock.lock();

        leaderListener.onEvent(new LeaderEvent(LeaderEvent.LeaderEventType.LOST, s));

        try {
            List<LeaderListener> listeners = leaders.get(s);
            if (listeners != null && listeners.size() > 0) {
                listeners.remove(leaderListener);
            }

            if (currentLeader.get(s) == leaderListener && listeners.size() > 0) {
                //当前leader被移除,重新选举leader
                selectLeader(s, listeners.get(0));
            }
        } finally {
            lock.unlock();
        }

    }

    private boolean selectLeader(String path, LeaderListener leaderListener) {

        if (path == null || leaderListener == null) {
            logger.error("--invalid leader data--" + path + "--" + leaderListener);
            return false;
        }

        leaderListener.onEvent(new LeaderEvent(LeaderEvent.LeaderEventType.TAKE, path));
        currentLeader.put(path, leaderListener);

        return true;
    }


    @Override
    public void removeListener(String s, ClusterListener clusterListener) {

    }

    @Override
    public void removeListener(ConnectionListener connectionListener) {
        connects.remove(connectionListener);
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setUrl(URL url) {

    }


    public String request(String path) throws Exception {

        byte[] bytes = requestByteData(path);

        if (bytes != null && bytes.length > 0)
            return new String(bytes, Charset.forName("UTF-8"));


        return null;


    }


    private byte[] requestByteData(String path) throws Exception {

        HttpClientBuilder builder = HttpClientBuilder.create();

        RequestConfig.Builder confBuild = RequestConfig.custom();
        confBuild.setConnectTimeout(connectTimeout);
        confBuild.setConnectionRequestTimeout(requestTimeout);

        builder.setDefaultRequestConfig(confBuild.build());
        CloseableHttpClient httpClient = builder.build();

        try {

            String url = urlBase + path + urlSurfix;
            HttpPost httppost = new HttpPost(url);

            CloseableHttpResponse response = httpClient.execute(httppost);
            if (response == null) {
                throw new RuntimeException("-Null response--" + url);
            }

            if (response.getStatusLine().getStatusCode() != 200) {
                throw new RuntimeException("--Invalid response code--" + response.getStatusLine().getStatusCode() + "--url--" + url);
            }

            HttpEntity entity = response.getEntity();
            if (entity == null) {
                throw new RuntimeException("--Null response entity --url--" + url);
            }

            Args.check(entity.getContentLength() <= 2147483647L, "--HTTP entity too large to be buffered in memory--");
            int length = (int) entity.getContentLength();
            if (length < 0) {
                length = 4096;
            }


            ByteArrayBuffer byteArrayBuffer = new ByteArrayBuffer(length);

            byte[] tmp = new byte[1024];

            InputStream inputStream = response.getEntity().getContent();
            int readBytes = 0;
            while ((readBytes = inputStream.read(tmp)) != -1) {
                byteArrayBuffer.append(tmp, 0, readBytes);
            }

            byte[] data = byteArrayBuffer.toByteArray();
            if (isCompress && path.endsWith("_zip")) {
                data = Compressors.compress(data, Zip.INSTANCE);
            }
            return data;
        } finally {
            httpClient.close();
        }


    }


    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public String getUrlBase() {
        return urlBase;
    }

    public void setUrlBase(String urlBase) {
        this.urlBase = urlBase;
    }

    @Override
    public boolean isStarted() {
        return opened;
    }
}
