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
package com.taobao.common.tedis.support.lucene.analysis.xanalyzer.collector;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.lucene.analysis.Token;

import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.TokenCollector;

/**
 * 
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 * 
 * @since 1.1
 */
public class QueryTokenCollector implements TokenCollector {

    /**
     * 存储当前被knife分解而成的Token对象
     * 
     */
    private LinkedList<Token> tokens = new LinkedList<Token>();

    private Token candidate;

    private Token last;

    public Iterator<Token> iterator() {
        if (candidate != null) {
            this.tokens.add(candidate);
            candidate = null;
        }
        Iterator<Token> iter = this.tokens.iterator();
        this.tokens = new LinkedList<Token>();
        return iter;
    }

    public void collect(String word, int offset, int end) {
        Token c = candidate != null ? candidate : last;
        if (c == null) {
            candidate = new Token(word, offset, end);
        } else if (offset == c.startOffset()) {
            if (end > c.endOffset()) {
                candidate = new Token(word, offset, end);
            }
        } else if (offset > c.startOffset()) {
            if (candidate != null) {
                select(candidate);
            }
            if (end > c.endOffset()) {
                candidate = new Token(word, offset, end);
            } else {
                candidate = null;
            }
        } else if (end >= c.endOffset()) {
            if (last != null && last.startOffset() >= offset && last.endOffset() <= end) {
                for (Iterator iter = tokens.iterator(); iter.hasNext();) {
                    last = (Token) iter.next();
                    if (last.startOffset() >= offset && last.endOffset() <= end) {
                        iter.remove();
                    }
                }
            }
            last = null;
            candidate = new Token(word, offset, end);
        }
    }

    protected void select(Token t) {
        this.tokens.add(t);
        this.last = t;
    }

}
