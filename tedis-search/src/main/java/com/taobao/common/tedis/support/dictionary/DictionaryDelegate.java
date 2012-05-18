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
package com.taobao.common.tedis.support.dictionary;


/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.1
 */
public class DictionaryDelegate implements Dictionary {
    private Dictionary target;

    public DictionaryDelegate() {
    }

    public DictionaryDelegate(Dictionary target) {
        this.target = target;
    }

    public Dictionary getTarget() {
        return target;
    }

    public void setTarget(Dictionary target) {
        this.target = target;
    }

    public String get(int index) {
        return target.get(index);
    }

    public Hit search(CharSequence input, int offset, int count) {
        return target.search(input, offset, count);
    }

    public int size() {
        return target.size();
    }

}
