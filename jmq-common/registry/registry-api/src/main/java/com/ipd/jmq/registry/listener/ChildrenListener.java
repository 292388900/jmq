package com.ipd.jmq.registry.listener;


import com.ipd.jmq.toolkit.concurrent.EventListener;

/**
 * 孩子监听器，不感知数据变化
 */
public interface ChildrenListener extends EventListener<ChildrenEvent> {
	
}
