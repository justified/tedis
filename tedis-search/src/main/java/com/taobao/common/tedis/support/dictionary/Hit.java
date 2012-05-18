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
 * Hit是检索字典时返回的结果。检索字典时，总是返回一个非空的Hit对象表示可能的各种情况。
 * <p>
 *
 * Hit对象包含2类判断信息：
 * <li>要检索的词语是否存在于词典中: {@link #isHit()}</li>
 * <li>词典是否含有以给定字符串开头的其他词语: {@link #isUnclosed()}</li>
 * <br>
 * 如果上面2个信息都是否定的，则 {@link #isUndefined()}返回true，否则返回false. <br>
 * <br>
 *
 * 如果{@link #isHit()}返回true，则{@link #getWord()}返回查找结果，{@link #getNext()}返回下一个词语。
 * <br>
 * 如果{@link #isHit()}返回false，但{@link #isUnclosed()}返回true，{@link #getNext()}
 * 返回以所查询词语开头的位置最靠前的词语。
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see Dictionary
 * @see BinaryDictionary
 * @see HashBinaryDictionary
 *
 * @since 1.0
 *
 */
public class Hit {

    // -------------------------------------------------

    public final static int UNCLOSED_INDEX = -1;

    public final static int UNDEFINED_INDEX = -2;

    public final static Hit UNDEFINED = new Hit(UNDEFINED_INDEX, null, null);

    // -------------------------------------------------

    /**
     * 目标词语在词典中的位置，或者在字典没有该词语是表示其他意思(参见以上静态变量定义的情况)
     */
    private int index;

    /**
     * 查找命中时，词典中相应的词
     */
    private String word;

    /**
     * 词典中命中词的下一个单词，或{@link #isUnclosed()}为true时最接近的下一个词(参见本类的注释)
     */
    private String next;

    // -------------------------------------------------

    /**
     *
     * @param index
     *            目标词语在词典中的位置，或者在字典没有该词语是表示其他意思(参见以上静态变量定义的情况)
     * @param word
     *            查找命中时，词典中相应的词
     * @param next
     *            词典中命中词的下一个单词，或{@link #isUnclosed()}为true时最接近的下一个词(参见本类的注释)
     */
    public Hit(int index, String word, String next) {
        this.index = index;
        this.word = word;
        this.next = next;
    }

    // -------------------------------------------------

    /**
     * 查找命中时，词典中相应的词
     */
    public String getWord() {
        return word;
    }

    /**
     * 目标词语在词典中的位置，或者在字典没有该词语是表示其他意思(参见以上静态变量定义的情况)
     *
     * @return
     */
    public int getIndex() {
        return index;
    }

    /**
     * 词典中命中词的下一个单词，或{@link #isUnclosed()}为true时最接近的下一个词(参见本类的注释)
     *
     * @return
     */
    public String getNext() {
        return next;
    }

    /**
     * 是否在字典中检索到要检索的词语
     *
     * @return
     */
    public boolean isHit() {
        return this.index >= 0;
    }

    /**
     * 是否有以当前检索词语开头的词语
     *
     * @return
     */
    public boolean isUnclosed() {
        return UNCLOSED_INDEX == this.index || (this.next != null && this.next.length() >= this.word.length() && this.next.startsWith(word));
    }

    /**
     * 字典中没有当前检索的词语，或以其开头的词语
     *
     * @return
     */
    public boolean isUndefined() {
        return UNDEFINED.index == this.index;
    }

    // -------------------------------------------------

    void setIndex(int index) {
        this.index = index;
    }

    void setWord(String key) {
        this.word = key;
    }

    void setNext(String next) {
        this.next = next;
    }

    // -------------------------------------------------

    @Override
    public int hashCode() {
        final int PRIME = 31;
        int result = 1;
        result = PRIME * result + ((word == null) ? 0 : word.hashCode());
        result = PRIME * result + index;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Hit other = (Hit) obj;
        if (word == null) {
            if (other.word != null)
                return false;
        } else if (!word.equals(other.word))
            return false;
        if (index != other.index)
            return false;
        return true;
    }

    public String toString() {
        if (isUnclosed()) {
            return "[UNCLOSED]";
        } else if (isUndefined()) {
            return "[UNDEFINED]";
        }
        return "[" + index + ']' + word;
    }

}
