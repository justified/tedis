/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
/**
 * Copyright 2007 The Apache Software Foundation
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
package com.taobao.common.tedis.support.dictionary.support.merging;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.ListIterator;

/**
 * 
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 * 
 * @since 1.0
 * 
 */
public class Merger {

    public static void merge(LinkedList<String> a, LinkedList<String> b) {
        ListIterator<String> aIter = (ListIterator<String>) a.iterator();
        ListIterator<String> bIter = (ListIterator<String>) b.iterator();
        while (aIter.hasNext() && bIter.hasNext()) {
            String aWord = aIter.next();
            boolean bGoOn = true;
            while (bGoOn && bIter.hasNext()) {
                String bWord = bIter.next();
                int r = bWord.compareTo(aWord);
                if (r == 0) {
                    continue;
                }
                if (r < 0) {
                    aIter.previous();
                    aIter.add(bWord);
                    aIter.next();
                } else {
                    bIter.previous();
                    bGoOn = false;
                }
            }
        }
        while (bIter.hasNext()) {
            a.add(bIter.next());
        }
    }

    public static void remove(LinkedList<String> a, LinkedList<String> b) {
        ListIterator<String> aIter = (ListIterator<String>) a.iterator();
        ListIterator<String> bIter = (ListIterator<String>) b.iterator();
        while (aIter.hasNext() && bIter.hasNext()) {
            String aWord = aIter.next();
            boolean bGoOn = true;
            while (bGoOn && bIter.hasNext()) {
                String bWord = bIter.next();
                int r = bWord.compareTo(aWord);
                if (r == 0) {
                    aIter.remove();
                    if (aIter.hasNext()) {
                        aWord = aIter.next();
                    }
                } else if (r < 0) {
                    continue;
                } else {
                    bIter.previous();
                    bGoOn = false;
                }
            }
        }
    }

    public static void main(String[] args) {
        LinkedList<String> a = new LinkedList<String>();
        LinkedList<String> b = new LinkedList<String>();
        a.add("1");
        a.add("4");
        a.add("a");
        a.add("c");

        b.add("2");
        b.add("3");
        b.add("b");
        b.add("d");
        b.add("Ì«Ñô");

        Merger.merge(a, b);

        System.out.println(Arrays.toString(a.toArray(new String[] {})));
    }
}
