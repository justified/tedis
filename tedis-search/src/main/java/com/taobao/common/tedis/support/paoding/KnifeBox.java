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
package com.taobao.common.tedis.support.paoding;

import java.util.ArrayList;

/**
 * KnifeBox负责决策当遇到字符串指定位置时应使用的Knife对象.
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see Paoding
 *
 * @since 1.0
 *
 */
public class KnifeBox implements Knife {

    private ArrayList<Knife> knives = new ArrayList<Knife>();

    public void addKnife(Knife k) {
        knives.add(k);
    }

    public ArrayList<Knife> getKnives() {
        return knives;
    }

    public void setKnives(ArrayList<Knife> knives) {
        this.knives = knives;
    }

    public boolean assignable(CharSequence beaf, int index) {
        return true;
    }

    public int dissect(Collector collector, CharSequence beaf, int offset) {
        int size = knives.size();
        Knife knife;
        for (int i = 0; i < size; i++) {
            knife = knives.get(i);
            if (knife.assignable(beaf, offset)) {
                return knife.dissect(collector, beaf, offset);
            }
        }
        return ++offset;
    }
}
