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
package com.taobao.common.tedis.support.lucene.analysis.xanalyzer;

import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;

import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.collector.QueryTokenCollector;
import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.collector.WriterTokenCollector;
import com.taobao.common.tedis.support.paoding.Knife;
import com.taobao.common.tedis.support.paoding.Paoding;
import com.taobao.common.tedis.support.paoding.cjk.CJKKnife;

/**
 * XAnalyzer是基于“庖丁解牛”框架的Lucene词语分析器，是“庖丁解牛”框架对Lucene的适配器。
 * <p>
 *
 * XAnalyzer是线程安全的：并发情况下使用同一个XAnalyzer实例是可行的。<br>
 * XAnalyzer是可复用的：推荐多次同一个XAnalyzer实例。
 * <p>
 *
 * 如有需要特别调整，应通过构造函数或knife设置器(setter)配置自订制的Knife实例。
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see XWriterAnalyzer
 * @see XQueryAnalyzer
 *
 * @see XTokenizer
 * @see Knife
 * @see Paoding
 * @see CJKKnife
 * @see TokenCollector
 *
 * @since 1.0
 *
 */
public class XAnalyzer extends Analyzer {

    // -------------------------------------------------

    /**
     * 该模式在建立索引时使用，能够使分析器对每个可能的词语建立索引
     */
    public static final int WRITER_MODE = 1;

    /**
     * 该模式在用户搜索时使用，使用户检索的结果匹配度最大化
     */
    public static final int QUERY_MODE = 2;

    // -------------------------------------------------
    /**
     * 用于向XTokenizer提供，分解文本字符
     *
     * @see XTokenizer#next()
     *
     */
    private Knife knife;

    /**
     * @see #WRITER_MODE
     * @see #QUERY_MODE
     */
    private int mode = WRITER_MODE;

    // -------------------------------------------------

    public XAnalyzer() {
    }

    public XAnalyzer(Knife knife) {
        this.knife = knife;
    }

    // -------------------------------------------------

    public Knife getKnife() {
        return knife;
    }

    public void setKnife(Knife knife) {
        this.knife = knife;
    }

    public int getMode() {
        return mode;
    }

    /**
     * 设置分析器模式。写模式(WRITER_MODE)或检索模式(QUERY_MODE)其中一种。默认为写模式。
     * <p>
     * WRITER_MODE在建立索引时使用，能够使分析器对每个可能的词语建立索引<br>
     * QUERY_MODE在用户搜索时使用，使用户检索的结果匹配度最大化
     *
     * @param mode
     */
    public void setMode(int mode) {
        this.mode = mode;
    }

    // -------------------------------------------------

    @Override
    public TokenStream tokenStream(String fieldName, Reader reader) {
        if (knife == null) {
            throw new NullPointerException("knife should be set before token");
        }
        // XTokenizer是TokenStream实现，使用knife解析reader流入的文本
        return new XTokenizer(reader, knife, createTokenCollector());
    }

    protected TokenCollector createTokenCollector() {
        switch (mode) {
        case WRITER_MODE:
            return new WriterTokenCollector();
        case QUERY_MODE:
            return new QueryTokenCollector();
        default:
            throw new IllegalArgumentException("wrong mode");
        }
    }

}
