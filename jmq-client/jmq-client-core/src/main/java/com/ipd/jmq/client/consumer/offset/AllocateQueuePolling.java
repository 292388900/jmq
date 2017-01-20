/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ipd.jmq.client.consumer.offset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;


/**
 * Polling queue algorithm
 *
 */
public class AllocateQueuePolling {
    // 日志
    private static final Logger logger = LoggerFactory.getLogger(AllocateQueuePolling.class);
    private ConcurrentLinkedQueue<Short> queue = new ConcurrentLinkedQueue<Short>();
    private short queues;

    public ConcurrentLinkedQueue<Short> allocate(short queues) {

        if (queues <= 0) {
            throw new IllegalArgumentException("queueAll is illegal");
        }
        this.queues = queues;
        for (short i = 1; i <= queues; i++) {
            queue.offer(i);
        }
        return queue;
    }

    public void resetQueues(short queues){
        if (queues == this.queues) {
            return;
        }
        this.queues = queues;
        if (queues < this.queues) {
            //出队后不会再入队
        }

        if (queues > this.queues) {
            for (short i = (short) (this.queues + 1); i <= queues; i++) {
                queue.offer(i);
            }
        }

    }

    public Short poll(){
        return queue.poll();
    }

    public boolean add(short i) {
        if (i > this.queues) {
            return false;
        } else {
            queue.add(i);
            return true;
        }
    }
}
