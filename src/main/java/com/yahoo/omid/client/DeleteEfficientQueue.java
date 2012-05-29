/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved.
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
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.client;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

/**
 * This class enables queues with efficient random deletes
 * The deletes are delayed until they reach the head of the queue
 * The top is always valid (not deleted)
 *
 * @param <E> Type stored
 */
class DeleteEfficientQueue<E> {
    private Queue<E> queue;
    private Set<E> deletedSet;

    public DeleteEfficientQueue() {
        queue = new LinkedList<E>();
        deletedSet = new HashSet<E>();
    }

    public boolean offer(E o) {
        return queue.offer(o);
    }

    public E peek() {
        return queue.peek();
    }

    /*
     * Also remove the next tops that are already deleted
     */
    public E poll() {
        E top =  queue.poll();
        //Now remove the next tops until you find one that is not deleted
        garbageCollectTops();
        return top;
    }

    //remove the tops till you get one that is not deleted
    private void garbageCollectTops() {
        E nexttop = queue.peek();
        while (deletedSet.contains(nexttop)) {
            deletedSet.remove(nexttop);
            queue.poll();
            nexttop = queue.peek();
        }
    }

    //If the object is not simply the top, delay its deletion
    public void delete(E o) {
        E top = queue.peek();
        assert(top != null);
        if (top.equals(o)) {
            queue.poll();
            garbageCollectTops();
        } else {
            deletedSet.add(o);
        }
    }

    public void clear() {
        queue.clear();
        deletedSet.clear();
    }
}

