/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ipd.jmq.server.broker.sequence;


import com.ipd.jmq.server.store.ConsumeQueue;

/**
 * Represents a range of numbers.
 *
 * @author chirino
 */
public class Sequence extends LinkedNode<Sequence> {

    public static final String DEVIDE = "-";
    long first;
    long last;
    int blockSize = ConsumeQueue.CQ_RECORD_SIZE;

    public Sequence(long value) {
        first = last = value;
    }

    public Sequence(long first, long last) {
        if (first > last) {
            throw new RuntimeException("Construct Sequence error,first:" + first + " ,last:" + last);
        }

        this.first = first;
        this.last = last;
    }

    public Sequence() {

    }

    public boolean isAdjacentToLast(long value) {
        return last + blockSize == value;
    }

    public boolean isAdjacentToFirst(long value) {
        return first - blockSize == value;
    }

    public boolean contains(long value) {
        return first <= value && value <= last;
    }

    public long range() {
        return first == last ? 1 : (last - first) / blockSize + 1;
    }

    @Override
    public String toString() {
        return first == last ? "" + first : first + DEVIDE + last;
    }

    public long getFirst() {
        return first;
    }

    public void setFirst(long first) {
        this.first = first;
    }

    public long getLast() {
        return last;
    }

    public void setLast(long last) {
        this.last = last;
    }

    public interface Closure<T extends Throwable> {
        public void execute(long value) throws T;
    }

    public <T extends Throwable> void each(Closure<T> closure) throws T {
        for (long i = first; i <= last; i++) {
            closure.execute(i);
        }
    }


}