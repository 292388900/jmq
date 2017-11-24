package com.ipd.jmq.server.broker.registry;

import com.ipd.jmq.registry.PathData;
import com.ipd.jmq.registry.Registry;
import com.ipd.jmq.registry.RegistryException;
import com.ipd.jmq.registry.listener.*;
import com.ipd.jmq.registry.zookeeper.ZKRegistry;
import com.ipd.jmq.toolkit.URL;
import com.ipd.jmq.toolkit.concurrent.EventListener;
import com.ipd.jmq.toolkit.lang.Close;
import com.ipd.jmq.toolkit.network.http.Get;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * Created by llw on 15-10-10.
 */
public class DynamicZkRegistry implements Registry, Runnable {

    private static Logger logger = LoggerFactory.getLogger(DynamicZkRegistry.class);

    private ZKRegistry zkRegistry;
    private URL url;
    private String registryInfoURL;

    private int connectTimeout = 5000;
    private int requestTimeout = 3000;


    protected ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final String type = "dynamicZookeeper";


    public DynamicZkRegistry(URL url) {

        this.url = url;
        this.zkRegistry = new ZKRegistry(this.url);
    }

    public DynamicZkRegistry() {

    }

    public DynamicZkRegistry(ZKRegistry zkRegistry) {
        this.zkRegistry = zkRegistry;
        this.url = zkRegistry.getUrl();
    }


    public String getType() {
        return type;
    }


    @Override
    public void run() {

        if (registryInfoURL != null && !registryInfoURL.isEmpty()) {
            Get httpGet = new Get();
            try {
                String registryUrl = httpGet.get(URL.valueOf(registryInfoURL));
                if (registryUrl == null || registryUrl.trim().length() == 0) {
                    logger.error("--invalid registryURL--" + registryUrl);
                    return;
                }
                this.reload(URL.valueOf(registryUrl));
            } catch (Exception e) {
                logger.error("--registryInfo error--" + registryInfoURL, e);
            }
        }
    }


    public void reload(URL url) {

        if (url == null || url.toString().equals(this.url.toString())) {
            if(logger.isDebugEnabled()) {
                logger.debug("--current zkRegistry--" + url.toString());
            }
            return;
        }

        if (url == null) {
            url = URL.valueOf("zookeeper://arch-yz.zookeeper.360buy.com:2181");
        } else if (url.getHost() == null || url.getHost().isEmpty()) {
            url = url.setHost("arch-yz.zookeeper.360buy.com").setPort(2181);
        }


        lock.writeLock().lock();

        try {
            this.url = url;
            Close.close(zkRegistry);
            this.started = false;

            zkRegistry = new ZKRegistry(this.url);

            this.stop();

            reloadListeners();

            logger.error("--zk reload success--" + url);

        } catch (Exception e) {
            logger.error("--reload error--" + zkRegistry.getUrl() + "--newZkURL--" + this.getUrl(), e);
        } finally {
            lock.writeLock().unlock();
        }
    }


    private void reloadListeners() {

        if (pathListenerMap.size() > 0) {
            Set<String> paths = pathListenerMap.keySet();
            for (String path : paths) {
                Set<EventListener> listeners = pathListenerMap.get(path);
                if (listeners.size() > 0) {
                    for (EventListener listener : listeners) {
                        this.addListener(path, (PathListener) listener);
                    }
                }
            }
        }

        if (leaderListenerMap.size() > 0) {
            Set<String> paths = leaderListenerMap.keySet();
            for (String path : paths) {
                Set<EventListener> listeners = leaderListenerMap.get(path);
                if (listeners.size() > 0) {
                    for (EventListener listener : listeners) {
                        this.addListener(path, (LeaderListener) listener);
                    }
                }
            }
        }

        if (childrenListenerMap.size() > 0) {
            Set<String> paths = childrenListenerMap.keySet();
            for (String path : paths) {
                Set<EventListener> listeners = childrenListenerMap.get(path);
                if (listeners.size() > 0) {
                    for (EventListener listener : listeners) {
                        this.addListener(path, (ChildrenListener) listener);
                    }
                }
            }
        }

        if (childrenDataListenerMap.size() > 0) {
            Set<String> paths = childrenDataListenerMap.keySet();
            for (String path : paths) {
                Set<EventListener> listeners = childrenDataListenerMap.get(path);
                if (listeners.size() > 0) {
                    for (EventListener listener : listeners) {
                        this.addListener(path, (ChildrenDataListener) listener);
                    }
                }
            }
        }


        if (connectionListeners.size() > 0) {
            for (EventListener listener : connectionListeners) {
                this.addListener((ConnectionListener) listener);
            }
        }

        if (lives.size() > 0) {
            Set<String> paths = lives.keySet();
            for (String path : paths) {
                this.createLive(path, lives.get(path));
            }
        }

    }

    @Override
    public URL getUrl() {
        return this.url;
    }


    private boolean started = false;

    @Override
    public void start() throws Exception {

        if (started) {
            return;
        }

        lock.writeLock().lock();
        try {
            if (started) {
                return;
            }

            zkRegistry.start();
            started = true;
        } finally {
            lock.writeLock().unlock();
        }

    }

    @Override
    public boolean isConnected() {
        return zkRegistry.isConnected();
    }

    @Override
    public void stop() {
        if (zkRegistry != null) {
            zkRegistry.stop();
        }
    }

    @Override
    public void create(String s, byte[] bytes) throws RegistryException {
        zkRegistry.create(s, bytes);
    }

    @Override
    public void create(List<String> list) throws RegistryException {
        zkRegistry.create(list);
    }

    private Map<String, byte[]> lives = new HashMap<String, byte[]>();

    @Override
    public void createLive(String s, byte[] bytes) {
        zkRegistry.createLive(s, bytes);
        lives.put(s, bytes);
    }

    @Override
    public void deleteLive(String s) {
        zkRegistry.deleteLive(s);
        lives.remove(s);
    }

    @Override
    public Lock createLock(String s) {
        return null;
    }

    @Override
    public void update(String s, byte[] bytes) throws RegistryException {
        zkRegistry.update(s, bytes);
    }

    public void update(PathData data) throws RegistryException {
        zkRegistry.update(data);
    }

    @Override
    public void update(String s, byte[] bytes, byte[] bytes1) throws RegistryException {
        zkRegistry.update(s, bytes, bytes1);
    }

    @Override
    public void delete(String s) throws RegistryException {
        zkRegistry.delete(s);
    }

    @Override
    public void delete(List<String> list) throws RegistryException {
        zkRegistry.delete(list);
    }

    @Override
    public boolean exists(String s) throws RegistryException {
        return zkRegistry.exists(s);
    }

    @Override
    public boolean isLeader(String s) throws RegistryException {
        return zkRegistry.isLeader(s);
    }

    @Override
    public PathData getData(String s) throws RegistryException {
        return zkRegistry.getData(s);
    }

    @Override
    public List<PathData> getChildData(String s) throws RegistryException {
        return zkRegistry.getChildData(s);
    }

    @Override
    public List<String> getChildren(String s) throws RegistryException {
        return getChildren(s);
    }

    private Map<String, HashSet<EventListener>> childrenListenerMap = new HashMap<String, HashSet<EventListener>>();

    @Override
    public void addListener(String s, ChildrenListener childrenListener) {
        zkRegistry.addListener(s, childrenListener);
        collectListener(s, childrenListener, childrenListenerMap);
    }

    private Map<String, HashSet<EventListener>> childrenDataListenerMap = new HashMap<String, HashSet<EventListener>>();

    @Override
    public void addListener(String s, ChildrenDataListener childrenDataListener) {

        zkRegistry.addListener(s, childrenDataListener);

        collectListener(s, childrenDataListener, childrenDataListenerMap);
    }

    private void collectListener(String path, EventListener listener, Map<String, HashSet<EventListener>> collector) {

        HashSet<EventListener> listeners = collector.get(path);
        if (listeners == null) {
            listeners = new HashSet<EventListener>();
            listeners.add(listener);
            collector.put(path, listeners);
        } else {
            listeners.add(listener);
        }
    }

    private void clearCollectedListener(String path, EventListener listener, Map<String, HashSet<EventListener>> collector) {

        HashSet<EventListener> listeners = collector.get(path);
        if (listeners != null) {
            listeners.remove(listener);
        }
    }


    private Map<String, HashSet<EventListener>> pathListenerMap = new HashMap<String, HashSet<EventListener>>();

    @Override
    public void addListener(String s, PathListener pathListener) {
        zkRegistry.addListener(s, pathListener);
        collectListener(s, pathListener, pathListenerMap);
    }

    private Map<String, HashSet<EventListener>> leaderListenerMap = new HashMap<String, HashSet<EventListener>>();

    @Override
    public void addListener(String s, LeaderListener leaderListener) {
        zkRegistry.addListener(s, leaderListener);

        collectListener(s, leaderListener, leaderListenerMap);
    }

    private Map<String, HashSet<EventListener>> clusterListenerMap = new HashMap<String, HashSet<EventListener>>();

    @Override
    public void addListener(String s, ClusterListener clusterListener) {
        zkRegistry.addListener(s, clusterListener);

        collectListener(s, clusterListener, clusterListenerMap);
    }


    private Set<ConnectionListener> connectionListeners = new HashSet<ConnectionListener>();

    @Override
    public void addListener(ConnectionListener connectionListener) {

        zkRegistry.addListener(connectionListener);

        connectionListeners.add(connectionListener);
    }

    @Override
    public void removeListener(String s, PathListener pathListener) {
        zkRegistry.removeListener(s, pathListener);

        clearCollectedListener(s, pathListener, pathListenerMap);
    }

    @Override
    public void removeListener(String s, ChildrenListener childrenListener) {
        zkRegistry.removeListener(s, childrenListener);

        clearCollectedListener(s, childrenListener, childrenListenerMap);
    }

    @Override
    public void removeListener(String s, ChildrenDataListener childrenDataListener) {
        zkRegistry.removeListener(s, childrenDataListener);

        clearCollectedListener(s, childrenDataListener, childrenDataListenerMap);
    }

    @Override
    public void removeListener(String s, LeaderListener leaderListener) {
        zkRegistry.removeListener(s, leaderListener);

        clearCollectedListener(s, leaderListener, leaderListenerMap);
    }

    @Override
    public void removeListener(String s, ClusterListener clusterListener) {
        zkRegistry.removeListener(s, clusterListener);

        clearCollectedListener(s, clusterListener, clusterListenerMap);
    }

    @Override
    public void removeListener(ConnectionListener connectionListener) {

        zkRegistry.removeListener(connectionListener);
        connectionListeners.remove(connectionListener);
    }

    @Override
    public void setUrl(URL url) {
        this.url = url;
    }

    public Map<String, HashSet<EventListener>> getChildrenListenerMap() {
        return childrenListenerMap;
    }

    public Map<String, HashSet<EventListener>> getChildrenDataListenerMap() {
        return childrenDataListenerMap;
    }

    public Map<String, HashSet<EventListener>> getPathListenerMap() {
        return pathListenerMap;
    }

    public Map<String, HashSet<EventListener>> getLeaderListenerMap() {
        return leaderListenerMap;
    }

    public Map<String, HashSet<EventListener>> getClusterListenerMap() {
        return clusterListenerMap;
    }

    public Set<ConnectionListener> getConnectionListeners() {
        return connectionListeners;
    }

    public ZKRegistry getZkRegistry() {
        return zkRegistry;
    }

    public String getRegistryInfoURL() {
        return registryInfoURL;
    }

    public void setRegistryInfoURL(String registryInfoURL) {
        this.registryInfoURL = registryInfoURL;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getRequestTimeout() {
        return requestTimeout;
    }

    public void setRequestTimeout(int requestTimeout) {
        this.requestTimeout = requestTimeout;
    }

    @Override
    public boolean isStarted() {
        return started;
    }
}
