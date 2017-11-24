package com.ipd.jmq.registry.zookeeper.manager;

import com.ipd.jmq.registry.listener.LeaderEvent;
import com.ipd.jmq.registry.util.Path;
import com.ipd.jmq.registry.zookeeper.ZKClient;
import com.ipd.jmq.registry.listener.LeaderListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Leader管理
 *
 * @author 何小锋
 */
public class LeaderManager extends ElectionManager<LeaderListener, LeaderEvent> {

    private static final Logger logger = LoggerFactory.getLogger(LeaderManager.class);

    public LeaderManager(ZKClient zkClient, String path) {
        super(zkClient, path);
    }

    @Override
    protected void onLostEvent() {
        writeLock.lock();
        try {
            if (leader.compareAndSet(true, false)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("lost leader." + Path.concat(path, node.get()));
                }
                // 对外发布失去领导事件
                events.add(new LeaderEvent(LeaderEvent.LeaderEventType.LOST, path));
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    protected void onAddListener(LeaderListener listener) {
        if (node.get() == null) {
            //初始化创建Leader选举的临时节点
            updateEvents.add(UpdateType.UPDATE);
        } else if (leader.get()) {
            //当前已经是Leader，广播Take事件给该监听器进行初始化
            events.add(new LeaderEvent(LeaderEvent.LeaderEventType.TAKE, path), listener);
        }
    }

    @Override
    protected void elect() throws Exception {
        //得到当前节点数据，并注册Watcher
        List<String> children = zkClient.getSortedChildren(path, updateWatcher);
        if (children.isEmpty()) {
            events.add(new LeaderEvent(LeaderEvent.LeaderEventType.LOST, path));
        } else {
            String leaderName = children.get(0);
            if (leader.get() && !leaderName.equals(node.get())) {
                // 以前是leader，现在不是leader
                events.add(new LeaderEvent(LeaderEvent.LeaderEventType.LOST, path));
                leader.set(false);
                if (logger.isDebugEnabled()) {
                    logger.debug("lost leader." + Path.concat(path, leaderName));
                }
            } else if (!leader.get() && leaderName.equals(node.get())) {
                // 以前不是leader，现在是leader
                events.add(new LeaderEvent(LeaderEvent.LeaderEventType.TAKE, path));
                leader.set(true);
                if (logger.isDebugEnabled()) {
                    logger.debug("take leader." + Path.concat(path, leaderName));
                }
            }
        }
    }

}