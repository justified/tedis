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
package com.taobao.common.tedis.support.dictionary.support.filewords;

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public class SimpleReadListener implements ReadListener {
    private Map<String, LinkedList<String>> dics = new Hashtable<String, LinkedList<String>>();
    private LinkedList<String> sortedWords;

    public boolean onFileBegin(String file) {
        if (!file.endsWith(".dic")) {
            return false;
        }
        sortedWords = new LinkedList<String>();
        return true;
    }

    public void onFileEnd(String file) {
        String name = file.substring(0, file.length() - 4);
        dics.put(name, sortedWords);
    }

    public void onWord(String word) {
        word = word.trim().toLowerCase();
        if (word.length() == 0 || word.charAt(0) == '#' || word.charAt(0) == '-') {
            return;
        }
        sortedWords.add(word);
    }

    public Map<String, LinkedList<String>> getResult() {
        return dics;
    }

}