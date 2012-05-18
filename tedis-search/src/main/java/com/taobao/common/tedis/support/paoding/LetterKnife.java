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


/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 */
public class LetterKnife extends CharKnife {

    public static final String[] DEFAULT_NOISE = { "and", "are", "as", "at", "be", "but", "by", "for", "if", "in", "into", "is", "it", "no", "not", "of", "on", "or", "such", "that", "the", "their",
            "then", "there", "these", "they", "this", "to", "was", "will", "with", "www" };

    public LetterKnife() {
        super(DEFAULT_NOISE);
    }

    public LetterKnife(String[] noiseWords) {
        super(noiseWords);
    }

    public boolean assignable(CharSequence beaf, int index) {
        return CharSet.isLetter(beaf.charAt(index));
    }

    @Override
    protected boolean isTokenChar(CharSequence beaf, int history, int index) {
        char ch = beaf.charAt(index);
        return CharSet.isLetter(ch) || (ch >= '0' && ch <= '9') || ch == '-';
    }

    @Override
    protected void collect(Collector collector, CharSequence beaf, int offset, int end, String word) {
        if (word.length() > 1) {
            super.collect(collector, beaf, offset, end, word);
        }
    }

}
