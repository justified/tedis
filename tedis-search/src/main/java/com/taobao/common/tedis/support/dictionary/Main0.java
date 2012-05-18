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

import java.util.Arrays;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public class Main0 {

    static String[] words = { "开始", "建立", "数据仓库", "经销", "经销商", "商品", "品味", "看出", "问题" };

    static String segment = "当经销商品味茶叶时，看出问题了。";

    /**
     * @param args
     */
    public static void main(String[] args) {
        Arrays.sort(words);
        System.out.println(Arrays.toString(words));
        Dictionary dic = new BinaryDictionary(words);
        //
        System.out.println(segment);
        //
        String input = segment;

        //
        int index = 1, count;
        int segmentLength = segment.length();
        for (int begin = 0; begin < segmentLength; begin++) {
            for (index = begin + 1, count = 1; index <= segmentLength; index++, count++) {
                Hit word = dic.search(input, begin, count);
                if (word.isUndefined()) {
                    break;
                } else if (word.isUnclosed()) {
                    continue;
                } else {
                    System.out.println("--" + begin + "," + count + ":" + word.getWord());
                }
            }

        }
    }

}
