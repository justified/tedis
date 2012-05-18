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

import java.util.HashMap;
import java.util.Map;

/**
 * Dictionary的散列+二叉查找实现。
 * <p>
 * 用于对大数量的，且头字符相同的字符串较多的情况，e.g汉字词语字典。在这种情况下，检索速度将比二叉字典更快。
 * <p>
 *
 * HashBinaryDictionary以一组已经排序的词语为输入，所有<b>头字符</b>相同的词语划为一个集合作为分字典(
 * 使用BinaryDictionary实现)。 查找词语时，先根据第一个字符找得分词典(BinaryDictionary实现)，再从该分词典中定位该词语。
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see BinaryDictionary
 *
 * @since 1.0
 *
 */
public class HashBinaryDictionary implements Dictionary {

    // -------------------------------------------------

    /**
     * 字典中所有词语，用于方便{@link #get(int)}方法
     */
    private String[] ascWords;

    /**
     * 首字符到分词典的映射
     */
    private Map<Integer, SubDictionaryWrap> subs;

    /**
	 *
	 */
    private final int hashIndex;

    private final int start;
    private final int end;
    private final int count;

    // -------------------------------------------------

    /**
     *
     * @param ascWords
     *            升序排列词语
     * @param initialCapacity
     * @param loadFactor
     */
    public HashBinaryDictionary(String[] ascWords, int initialCapacity, float loadFactor) {
        this(ascWords, 0, 0, ascWords.length, initialCapacity, loadFactor);
    }

    public HashBinaryDictionary(String[] ascWords, int hashIndex, int start, int end, int initialCapacity, float loadFactor) {
        this.ascWords = ascWords;
        this.start = start;
        this.end = end;
        this.count = end - start;
        this.hashIndex = hashIndex;
        subs = new HashMap<Integer, SubDictionaryWrap>(initialCapacity, loadFactor);
        createSubDictionaries();
    }

    // -------------------------------------------------

    /**
     * 创建分词典映射，为构造函数调用
     */
    protected void createSubDictionaries() {
        // 定位相同头字符词语的开头和结束位置以确认分字典
        int beginIndex = this.start;
        int endIndex = this.start + 1;

        char beginHashChar = getChar(ascWords[start], hashIndex);
        char endHashChar;
        for (; endIndex < this.end; endIndex++) {
            endHashChar = getChar(ascWords[endIndex], hashIndex);
            if (endHashChar != beginHashChar) {
                addSubDictionary(beginHashChar, beginIndex, endIndex);
                beginIndex = endIndex;
                beginHashChar = endHashChar;
            }
        }
        addSubDictionary(beginHashChar, beginIndex, this.end);
    }

    protected char getChar(String s, int index) {
        if (index >= s.length()) {
            return (char) 0;
        }
        return s.charAt(index);
    }

    /**
     * 将位置在beginIndex和endIndex之间(不包括endIndex)的词语作为一个分词典
     *
     * @param hashChar
     * @param beginIndex
     * @param endIndex
     */
    protected void addSubDictionary(char hashChar, int beginIndex, int endIndex) {
        SubDictionaryWrap subDic = new SubDictionaryWrap(hashChar, createSubDictionary(ascWords, beginIndex, endIndex), beginIndex);
        Integer key = keyOf(hashChar);
        if (subs.containsKey(key)) {
            System.out.println("出现这个文字，表示输入的词语排序错误，请确保词典排序正确>>>>>>>>>" + hashChar);
        }
        subs.put(key, subDic);
    }

    protected Dictionary createSubDictionary(String[] ascWords, int beginIndex, int endIndex) {
        int count = endIndex - beginIndex;
        if (count < 16) {
            return new BinaryDictionary(ascWords, beginIndex, endIndex);
        } else {
            return new HashBinaryDictionary(ascWords, hashIndex + 1, beginIndex, endIndex, getCapacity(count), 0.75f);
        }
    }

    protected static final int[] capacityCandiate = { 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 10192 };

    protected int getCapacity(int count) {
        int capacity = -1;
        count <<= 2;
        count /= 3;
        count += 1;
        for (int i = 0; i < capacityCandiate.length; i++) {
            if (count <= capacityCandiate[i]) {
                capacity = capacityCandiate[i];
                break;
            }
        }
        if (capacity < 0) {
            capacity = capacityCandiate[capacityCandiate.length - 1];
        }
        return capacity;
    }

    // -------------------------------------------------

    public String get(int index) {
        return ascWords[start + index];
    }

    public Hit search(CharSequence input, int begin, int count) {
        SubDictionaryWrap subDic = subs.get(keyOf(input.charAt(hashIndex + begin)));
        if (subDic == null) {
            return Hit.UNDEFINED;
        }
        Dictionary dic = subDic.dic;
        // 对count==hashIndex + 1的处理
        if (count == hashIndex + 1) {
            String header = dic.get(0);
            if (header.length() == hashIndex + 1) {
                if (subDic.wordIndexOffset + 1 < this.ascWords.length) {
                    return new Hit(subDic.wordIndexOffset, header, this.ascWords[subDic.wordIndexOffset + 1]);
                } else {
                    return new Hit(subDic.wordIndexOffset, header, null);
                }
            } else {
                return new Hit(Hit.UNCLOSED_INDEX, null, header);
            }
        }
        // count > hashIndex + 1
        Hit word = dic.search(input, begin, count);
        if (word.isHit()) {
            int index = subDic.wordIndexOffset + word.getIndex();
            word.setIndex(index);
            if (word.getNext() == null && index < size()) {
                word.setNext(get(index + 1));
            }
        }
        return word;
    }

    public int size() {
        return count;
    }

    // -------------------------------------------------

    /**
     * 字符的在{@link #subs}的key值。
     *
     * @param theChar
     * @return
     *
     * @see #subs
     */
    protected int keyOf(char theChar) {
        // return theChar - 0x4E00;// '一'==0x4E00
        return theChar;
    }

    /**
     * 分词典封箱
     */
    static class SubDictionaryWrap {
        /**
         * 分词典词组的头字符
         */
        char hashChar;

        /**
         * 分词典
         */
        Dictionary dic;

        /**
         * 分词典第一个词语在所有词语中的偏移位置
         */
        int wordIndexOffset;

        public SubDictionaryWrap(char hashChar, Dictionary dic, int wordIndexOffset) {
            this.hashChar = hashChar;
            this.dic = dic;
            this.wordIndexOffset = wordIndexOffset;
        }
    }

}
