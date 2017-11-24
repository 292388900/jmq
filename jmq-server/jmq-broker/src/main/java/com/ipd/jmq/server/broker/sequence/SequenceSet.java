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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Keeps track of a added long values. Collapses ranges of numbers using a
 * Sequence representation. Use to keep track of received message ids to find
 * out if a message is duplicate or if there are any missing messages.
 *
 * @author chirino
 */
//public class SequenceSet extends LinkedNodeList<Sequence> implements Iterable<Long> {
public class SequenceSet extends LinkedNodeList<Sequence> {
    int blockSize = ConsumeQueue.CQ_RECORD_SIZE;

    public SequenceSet() {

    }

    public SequenceSet(SequenceSet sequenceSet) {
        if (sequenceSet == null || sequenceSet.isEmpty()) return;
        // 此为双向循环链表
        Sequence fromSequence = sequenceSet.getHead();
        do {
            Sequence toSequence = new Sequence(fromSequence.first, fromSequence.last);
            toSequence.linkToTail(this);
        } while ((fromSequence = fromSequence.next) != sequenceSet.getHead());
    }

    public void add(Sequence value) {
        // TODO we can probably optimize this a bit
        for (long i = value.first; i < value.last + blockSize; i = i + blockSize) {
            add(i);
        }
    }

    /**
     * @param value the value to add to the list
     * @return false if the value was a duplicate.
     */
    public synchronized boolean add(long value) {

        if (isEmpty()) {
            addFirst(new Sequence(value));
            return true;
        }

        Sequence sequence = getHead();
        while (sequence != null) {
            if (sequence.isAdjacentToLast(value)) {
                // grow the sequence...
                sequence.last = value;
                // it might connect us to the next sequence..
                if (sequence.getNext() != null) {
                    Sequence next = sequence.getNext();
                    if (next.isAdjacentToFirst(value)) {
                        // Yep the sequence connected.. so join them.
                        sequence.last = next.last;
                        next.unlink();
                    }
                }
                return true;
            }

            if (sequence.isAdjacentToFirst(value)) {
                // grow the sequence...
                sequence.first = value;

                // it might connect us to the previous
                if (sequence.getPrevious() != null) {
                    Sequence prev = sequence.getPrevious();
                    if (prev.isAdjacentToLast(value)) {
                        // Yep the sequence connected.. so join them.
                        sequence.first = prev.first;
                        prev.unlink();
                    }
                }
                return true;
            }

            // Did that value land before this sequence?
            if (value < sequence.first) {
                // Then insert a new entry before this sequence item.
                sequence.linkBefore(new Sequence(value));
                return true;
            }

            // Did that value land within the sequence? The it's a duplicate.
            if (sequence.contains(value)) {
                return false;
            }

            sequence = sequence.getNext();
        }

        // Then the value is getting appended to the tail of the sequence.
        addLast(new Sequence(value));
        return true;
    }

    public boolean remove(Sequence sequence) {
        if (sequence == null) {
            return false;
        }

        for (long first = sequence.first; first <= sequence.last; first = first + sequence.blockSize) {
            remove(first);
        }

        return true;
    }

    /**
     * Removes the given value from the Sequence set, splitting a
     * contained sequence if necessary.
     *
     * @param value The value that should be removed from the SequenceSet.
     * @return true if the value was removed from the set, false if there
     * was no sequence in the set that contained the given value.
     */

    public boolean remove(long value) {
        Sequence sequence = getHead();
        while (sequence != null) {
            if (sequence.contains(value)) {
                if (sequence.range() == 1) {
                    sequence.unlink();
                    return true;
                } else if (sequence.getFirst() == value) {
                    sequence.setFirst(value + blockSize);
                    return true;
                } else if (sequence.getLast() == value) {
                    sequence.setLast(value - blockSize);
                    return true;
                } else {
                    sequence.linkBefore(new Sequence(sequence.first, value - blockSize));
                    sequence.linkAfter(new Sequence(value + blockSize, sequence.last));
                    sequence.unlink();
                    return true;
                }

            }

            sequence = sequence.getNext();
        }

        return false;
    }

    /**
     * Removes and returns the first element from this list.
     *
     * @return the first element from this list.
     * @throws NoSuchElementException if this list is empty.
     */
    public long removeFirst() {
        if (isEmpty()) {
            throw new NoSuchElementException();
        }

        Sequence rc = removeFirstSequence(1);
        return rc.first;
    }

    /**
     * Removes and returns the last sequence from this list.
     *
     * @return the last sequence from this list or null if the list is empty.
     */
    public Sequence removeLastSequence() {
        if (isEmpty()) {
            return null;
        }

        Sequence rc = tail();
        rc.unlink();
        return rc;
    }

    /**
     * Removes and returns the first sequence that is count range large.
     *
     * @return a sequence that is count range large, or null if no sequence is that large in the list.
     */
    public Sequence removeFirstSequence(long count) {
        if (isEmpty()) {
            return null;
        }

        Sequence sequence = getHead();
        while (sequence != null) {
            if (sequence.range() == count) {
                sequence.unlink();
                return sequence;
            }
            if (sequence.range() > count) {
                Sequence rc = new Sequence(sequence.first, sequence.first + (count - 1) * blockSize);
                sequence.first += count * blockSize;
                return rc;
            }
            sequence = sequence.getNext();
        }
        return null;
    }

    /**
     * @return all the id Sequences that are missing from this set that are not
     * in between the range provided.
     */
    public List<Sequence> missing(long first, long last) {
        ArrayList<Sequence> rc = new ArrayList<Sequence>();
        if (first > last) {
            throw new IllegalArgumentException("First cannot be more than last");
        }
        if (isEmpty()) {
            // We are missing all the messages.
            rc.add(new Sequence(first, last));
            return rc;
        }

        Sequence sequence = getHead();
        while (sequence != null && first <= last) {
            if (sequence.contains(first)) {
                first = sequence.last + blockSize;
            } else {
                if (first < sequence.first) {
                    if (last < sequence.first) {
                        rc.add(new Sequence(first, last));
                        return rc;
                    } else {
                        rc.add(new Sequence(first, sequence.first - blockSize));
                        first = sequence.last + blockSize;
                    }
                }
            }
            sequence = sequence.getNext();
        }

        if (first <= last) {
            rc.add(new Sequence(first, last));
        }
        return rc;
    }

    /**
     * @return all the Sequence that are in this list
     */
    public List<Sequence> allSequence() {
        ArrayList<Sequence> rc = new ArrayList<Sequence>(size());
        Sequence sequence = getHead();
        while (sequence != null) {
            rc.add(new Sequence(sequence.first, sequence.last));
            sequence = sequence.getNext();
        }
        return rc;
    }

    /**
     * @return all the Sequence that are in this list
     */
    public List<String> getSequences() {
        List<Sequence> sequences = allSequence();
        if (sequences == null || sequences.size() == 0) {
            return null;
        }

        List<String> result = new ArrayList<String>(sequences.size());
        for (Sequence sequence : sequences) {
            result.add(sequence.toString());
        }

        return result;
    }

    public void setSequences(List<String> sequences) {
        if (sequences == null || sequences.size() == 0) {
            return;
        }

        for (String seqStr : sequences) {
            Sequence sequence = toSequence(seqStr);
            if (sequence != null) {
                if (isEmpty()) {
                    addFirst(sequence);
                    continue;
                }
                Sequence curSeq = getHead();
                for (; curSeq != null; curSeq = curSeq.getNext()) {
                    if (sequence.getLast() < curSeq.getFirst()) {
                        curSeq.linkBefore(sequence);
                        break;
                    }
                }
                if (curSeq == null) {
                    tail().linkAfter(sequence);
                }
            }
        }
    }

    private Sequence toSequence(String str) {

        String[] data = str.split(Sequence.DEVIDE);
        if (data.length > 3 || data.length == 0) {
            throw new RuntimeException("Invalid sequence:" + str);
        }

        if (data.length == 1) {
            return new Sequence(Long.parseLong(data[0]));
        } else {
            return new Sequence(Long.parseLong(data[0]), Long.parseLong(data[1]));
        }


    }

    private void checkValue(long value) {
        if (value % blockSize != 0) {
            throw new RuntimeException("Invalid value, mod " + blockSize + "!=0");
        }
    }

    /**
     * Returns true if the value given is contained within one of the
     * sequences held in this set.
     *
     * @param value The value to search for in the set.
     * @return true if the value is contained in the set.
     */
    public boolean contains(long value) {
        if (isEmpty()) {
            return false;
        }

        checkValue(value);

        Sequence sequence = getHead();
        while (sequence != null) {
            if (sequence.contains(value)) {
                return true;
            }
            sequence = sequence.getNext();
        }

        return false;
    }

    public boolean contains(long first, long last) {
        if (isEmpty()) {
            return false;
        }
        Sequence sequence = getHead();
        while (sequence != null) {
            if (sequence.first <= first && first <= sequence.last) {
                return last <= sequence.last;
            }
            sequence = sequence.getNext();
        }
        return false;
    }

    public boolean contains(Sequence sequence) {
        return sequence != null && contains(sequence.getFirst(), sequence.getLast());
    }


    /**
     * Computes the size of this Sequence by summing the values of all
     * the contained sequences.
     *
     * @return the total number of values contained in this set if it
     * were to be iterated over like an array.
     */
    public long rangeSize() {
        long result = 0;
        Sequence sequence = getHead();
        while (sequence != null) {
            result += sequence.range();
            sequence = sequence.getNext();
        }
        return result;
    }

    public Iterator<Long> iterator() {
        return new SequenceIterator();
    }

    private class SequenceIterator implements Iterator<Long> {

        private Sequence currentEntry;
        private long lastReturned = -1;

        public SequenceIterator() {
            currentEntry = getHead();
            if (currentEntry != null) {
                lastReturned = currentEntry.first - blockSize;
            }
        }

        public boolean hasNext() {
            return currentEntry != null;
        }

        public Long next() {
            if (currentEntry == null) {
                throw new NoSuchElementException();
            }

            if (lastReturned < currentEntry.first) {
                lastReturned = currentEntry.first;
                if (currentEntry.range() == blockSize) {
                    currentEntry = currentEntry.getNext();
                }
            } else {
                lastReturned++;
                if (lastReturned == currentEntry.last) {
                    currentEntry = currentEntry.getNext();
                }
            }

            return lastReturned;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    public int blockSize() {
        return blockSize;
    }

    public static void main(String[] args) throws Exception {


        SequenceSet sequenceSet = new SequenceSet();

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000 * 22; i = i + 22) {
            sequenceSet.add(i);
        }

        long end = System.currentTimeMillis();

        long used = end - start;

        System.out.println("used: " + used);


    }

}