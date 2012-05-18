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

import java.util.Arrays;
import java.util.HashSet;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public abstract class CharKnife implements Knife {

    private HashSet<String> noiseTable;

    public CharKnife() {
    }

    public CharKnife(String[] noiseWords) {
        setNoiseWords(noiseWords);
    }

    public void setNoiseWords(String[] noiseWords) {
        Arrays.sort(noiseWords);
        noiseTable = new HashSet<String>((int) (noiseWords.length * 1.5));
        for (int i = 0; i < noiseWords.length; i++) {
            noiseTable.add(noiseWords[i]);
        }

    }

    public int dissect(Collector collector, CharSequence beaf, int offset) {
        int end = offset + 1;
        for (; end < beaf.length() && isTokenChar(beaf, offset, end); end++) {
        }
        if (end == beaf.length() && offset > 0) {
            return -offset;
        }
        String word = beaf.subSequence(offset, end).toString();
        if (noiseTable != null && noiseTable.contains(word)) {
            word = null;
        }
        if (word != null) {
            collect(collector, beaf, offset, end, word);
        }
        return end;
    }

    protected void collect(Collector collector, CharSequence beaf, int offset, int end, String word) {
        collector.collect(word, offset, end);
    }

    protected abstract boolean isTokenChar(CharSequence beaf, int history, int index);

}
