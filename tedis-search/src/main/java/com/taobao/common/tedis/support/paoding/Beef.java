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
 * {@link Beef}是要被庖丁“解”的“牛骨肉”，是对文本字符流的高效封装，可以从中读取指定位置的字符。
 * <p>
 * {@link Beef}和{@link String}对象的不同之处在于：<br>
 * {@link Beef}共享输入的char数组，{@link String}的策略是对共享数组进行克隆，克隆损耗了性能。<br>
 * 同时，{@link Beef}在 {@link #charAt(int)}方法还进行对字符的预处理，使返回时符合规则:1)toLowerCase
 * 2)全角转半角等
 * <p>
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public class Beef implements CharSequence {

    // -------------------------------------------------

    /**
     * 文本字符数组
     */
    private final char[] value;

    /**
     * 字符开始位置，即charAt(i)返回value[offset+i]字符
     */
    private int offset;

    /**
     * 从offset位置开始的字符数
     */
    private int count;

    /** Cache the hash code for the beef */
    private int hash; // Default to 0

    // -------------------------------------------------

    /**
     * 构造函数
     *
     * @param body
     *            被本对象中直接拥有的文本字符数组
     * @param offset
     *            字符开始位置，即get(i)返回body[offset+i]字符
     * @param count
     *            从offset位置开始的字符数
     */
    public Beef(char[] value, int offset, int count) {
        this.value = value;
        set(offset, count);
    }

    // -------------------------------------------------

    public void set(int offset, int count) {
        if (offset < 0) {
            throw new StringIndexOutOfBoundsException(offset);
        }
        if (count < 0) {
            throw new StringIndexOutOfBoundsException(count);
        }
        if (offset > value.length - count) {
            throw new StringIndexOutOfBoundsException(offset + count);
        }
        this.offset = offset;
        this.count = count;
    }

    public char[] getValue() {
        return value;
    }

    public int getCount() {
        return count;
    }

    public int getOffset() {
        return offset;
    }

    // -------------------------------------------------

    /**
     * 获取指定位置的字符。返回之前将被预处理：1)toLowerCase，2)全角转半角等
     */
    public char charAt(int index) {
        if (index >= 0 && index < count) {
            char src = value[offset + index];
            if (src > 65280 && src < 65375) {
                src = (char) (src - 65248);
                value[offset + index] = src;
            }
            if (src >= 'A' && src <= 'Z') {
                src += 32;
                value[offset + index] = src;
            } else if (src == 12288) {
                src = 32;
                value[offset + index] = 32;
            }
            return src;
        }
        return (char) -1;
    }

    public int length() {
        return count;
    }

    public CharSequence subSequence(int start, int end) {
        return new Beef(value, offset + start, end - start);
    }

    // -------------------------------------------------

    @Override
    public String toString() {
        return new String(value, offset, count);
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            int off = offset;
            char val[] = value;
            int len = count;

            for (int i = 0; i < len; i++) {
                h = 31 * h + val[off++];
            }
            hash = h;
        }
        return h;
    }

}
