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
package com.taobao.common.tedis.support.paoding.cjk;

import com.taobao.common.tedis.support.dictionary.BinaryDictionary;
import com.taobao.common.tedis.support.dictionary.Dictionary;
import com.taobao.common.tedis.support.dictionary.HashBinaryDictionary;

/**
 * 中文字典缓存根据地,为{@link CJKKnife}所用。<br>
 * 从本对象可以获取中文需要的相关字典。包括词汇表、姓氏表、计量单位表、忽略的词或单字等。
 * <p>
 * 使用{@link CJKDictionaryFactory}需要设置一个非空的{@link #wordsLoader}。
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see CJKKnife
 *
 * @since 1.0
 */
public class CJKDictionaryFactory {

    // -------------------------------------------------

    /**
     * 用于从目录或数据库中获取词语
     */
    private WordsLoader wordsLoader;

    // -------------------------------------------------

    /**
     * 词汇表字典
     */
    private Dictionary vocabulary;

    /**
     * 姓氏字典
     *
     */
    private Dictionary confucianFamilyNames;

    /**
     * 忽略的单字
     */
    private Dictionary xchars;

    /**
     * 忽略的词语
     *
     */
    private Dictionary xwords;

    /**
     * 计量单位
     */
    private Dictionary units;

    // -------------------------------------------------

    public CJKDictionaryFactory() {
    }

    public CJKDictionaryFactory(WordsLoader wordsLoader) {
        this.wordsLoader = wordsLoader;
    }

    // -------------------------------------------------

    public WordsLoader getWordsLoader() {
        return wordsLoader;
    }

    public void setWordsLoader(WordsLoader wordsLoader) {
        this.wordsLoader = wordsLoader;
    }

    // -------------------------------------------------
    /**
     * 词汇表字典
     *
     * @return
     */
    public Dictionary getVocabulary() {
        if (vocabulary == null) {
            synchronized (this) {
                if (vocabulary == null) {
                    // 大概有5639个字有词语，故取0x2fff=x^13>8000>8000*0.75=6000>5639
                    vocabulary = new HashBinaryDictionary(wordsLoader.loadCJKVocabulary().toArray(new String[0]), 0x2fff, 0.75f);
                }
            }
        }
        return vocabulary;
    }

    /**
     * 姓氏字典
     *
     * @return
     */
    public Dictionary getConfucianFamilyNames() {
        if (confucianFamilyNames == null) {
            synchronized (this) {
                if (confucianFamilyNames == null) {
                    confucianFamilyNames = new BinaryDictionary(wordsLoader.loadCJKConfucianFamilyNames().toArray(new String[0]));
                }
            }
        }
        return confucianFamilyNames;
    }

    /**
     * 忽略的词语
     *
     * @return
     */
    public Dictionary getXchars() {
        if (xchars == null) {
            synchronized (this) {
                if (xchars == null) {
                    xchars = new HashBinaryDictionary(wordsLoader.loadCJKXchars().toArray(new String[0]), 256, 0.75f);
                }
            }
        }
        return xchars;
    }

    /**
     * 忽略的单字
     *
     * @return
     */
    public Dictionary getXwords() {
        if (xwords == null) {
            synchronized (this) {
                if (xwords == null) {
                    xwords = new BinaryDictionary(wordsLoader.loadCJKXwords().toArray(new String[0]));
                }
            }
        }
        return xwords;
    }

    /**
     * 计量单位
     *
     * @return
     */
    public Dictionary getUnits() {
        if (units == null) {
            synchronized (this) {
                if (units == null) {
                    units = new HashBinaryDictionary(wordsLoader.loadCJKUnit().toArray(new String[0]), 1024, 0.75f);
                }
            }
        }
        return units;
    }

}
