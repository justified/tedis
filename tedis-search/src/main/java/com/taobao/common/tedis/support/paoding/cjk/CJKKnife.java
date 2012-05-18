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

import com.taobao.common.tedis.support.dictionary.Dictionary;
import com.taobao.common.tedis.support.dictionary.Hit;
import com.taobao.common.tedis.support.paoding.CharSet;
import com.taobao.common.tedis.support.paoding.Collector;
import com.taobao.common.tedis.support.paoding.Knife;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public class CJKKnife implements Knife {

    // -------------------------------------------------

    private CJKDictionaryFactory factory;

    // -------------------------------------------------

    public CJKKnife() {
    }

    public CJKKnife(CJKDictionaryFactory factory) {
        this.factory = factory;
    }

    // -------------------------------------------------

    public CJKDictionaryFactory getFactory() {
        return factory;
    }

    public void setFactory(CJKDictionaryFactory factory) {
        this.factory = factory;
    }

    // -------------------------------------------------

    public boolean assignable(CharSequence beaf, int index) {
        return CharSet.isCjkUnifiedIdeographs(beaf.charAt(index));
    }

    public int dissect(Collector collector, CharSequence beaf, int offset) {
        if (CharSet.isCjkUnifiedIdeographs(beaf.charAt(beaf.length() - 1)) && offset > 0 && beaf.length() - offset < 50) {
            return -offset;
        }
        Dictionary vocabulary = factory.getVocabulary();
        /* 例句:王崇浩住在北京积水潭桥附近 */
        // setup和end用于规定其之间的文字是否为词典词语
        int setup, end;
        // 为unidentifiedIndex服务，为已找出的词语结束位置的最大者，e.g '在','京','桥','近'
        int identifiedEnd = offset;
        // 用于定位未能分词的块的开始位置，e.g '王'
        int unidentifiedIndex = -1;
        // 用于辅助判断是否调用shouldAWord()方法
        int maxWordLength = 0;
        Hit word = null;
        for (setup = offset, end = offset; setup < beaf.length() && CharSet.isCjkUnifiedIdeographs(beaf.charAt(setup)); end = ++setup) {
            for (int count = 1; end < beaf.length() && CharSet.isCjkUnifiedIdeographs(beaf.charAt(end++)); count++) {
                // 第一次for循环时，end=setup+1
                word = vocabulary.search(beaf, setup, count);
                if (word.isUndefined()) {
                    if (unidentifiedIndex < 0 && setup >= identifiedEnd) {
                        unidentifiedIndex = setup;
                    }
                    break;
                } else if (word.isHit()) {
                    if (identifiedEnd < end) {
                        identifiedEnd = end;
                    }
                    if (unidentifiedIndex >= 0) {
                        dissectUnidentified(collector, beaf, unidentifiedIndex, setup - unidentifiedIndex);
                        unidentifiedIndex = -1;
                    }
                    collector.collect(word.getWord(), setup, end);
                    if (setup == offset && maxWordLength < count) {
                        maxWordLength = count;
                    }
                    if (!(word.isUnclosed() && end < beaf.length()// 这个判断是为下面判断服务
                    && beaf.charAt(end) >= word.getNext().charAt(count))) {
                        break;
                    }
                }
            }
        }
        if (identifiedEnd != end) {
            dissectUnidentified(collector, beaf, identifiedEnd, end - identifiedEnd);
        }
        int len = end - offset;
        if (len > 2 && len != maxWordLength && shouldAWord(beaf, offset, end)) {
            collect(collector, beaf, offset, end);
        }
        return setup;// 此时end=start
    }

    // -------------------------------------------------

    /**
     * 对非词汇表中的字词分词
     *
     * @param cellector
     * @param beaf
     * @param offset
     * @param count
     */
    protected void dissectUnidentified(Collector collector, CharSequence beaf, int offset, int count) {
        int end = offset + count;
        Hit word = null;
        int nearEnd = end - 1;
        for (int i = offset, j = i; i < end;) {
            j = skipXword(beaf, i, end);
            if (j >= 0 && i != j) {
                i = j;
                continue;
            }
            j = collectNumber(collector, beaf, i, end);
            if (j >= 0 && i != j) {
                i = j;
                continue;
            }
            word = factory.getXchars().search(beaf, i, 1);
            if (word.isHit()) {
                i++;
                continue;
            }
            // 头字
            if (i == offset) {
                // 百度门事件=百度+门+...!=百度+门事+...
                collect(collector, beaf, offset, offset + 1);
            }
            // 尾字
            if (i == nearEnd) {
                if (nearEnd != offset) {
                    collect(collector, beaf, nearEnd, end);
                }
            }
            // 穷尽二元分词
            else {
                collect(collector, beaf, i, i + 2);
            }
            i++;
        }
    }

    protected boolean shouldAWord(CharSequence beaf, int offset, int end) {
        if (offset > 0 && end < beaf.length()) {// 确保前有字符，后也有字符
            int prev = offset - 1;
            if (beaf.charAt(prev) == '“' && beaf.charAt(end) == '”') {
                return true;
            } else if (beaf.charAt(prev) == '‘' && beaf.charAt(end) == '’') {
                return true;
            } else if (beaf.charAt(prev) == '\'' && beaf.charAt(end) == '\'') {
                return true;
            } else if (beaf.charAt(prev) == '\"' && beaf.charAt(end) == '\"') {
                return true;
            }
        }
        return false;
    }

    private final void collect(Collector collector, CharSequence beaf, int offset, int end) {
        collector.collect(beaf.subSequence(offset, end).toString(), offset, end);
    }

    private final int skipXword(CharSequence beaf, int offset, int end) {
        Hit word;
        for (int k = offset + 2; k <= end; k++) {
            word = factory.getXwords().search(beaf, offset, k - offset);
            if (word.isHit()) {
                offset = k;
            }
            if (word.isUndefined() || !word.isUnclosed()) {
                break;
            }
        }
        return offset;
    }

    private final int collectNumber(Collector collector, CharSequence beaf, int offset, int end) {
        int number1 = -1;
        int number2 = -1;
        int cur = offset;
        int bitValue = 0;
        int maxUnit = 0;
        boolean hasDigit = false;// 作用：去除没有数字只有单位的汉字，如“万”，“千”
        for (; cur <= end && (bitValue = toNumber(beaf.charAt(cur))) >= 0; cur++) {
            if (bitValue == 2 && (beaf.charAt(cur) == '两' || beaf.charAt(cur) == '俩' || beaf.charAt(cur) == '倆')) {
                if (cur != offset)
                    break;
            }
            if (bitValue >= 0 && bitValue < 10) {
                hasDigit = true;
                if (number2 < 0)
                    number2 = bitValue;
                else {
                    number2 *= 10;
                    number2 += bitValue;
                }
            } else {
                if (number2 < 0) {
                    if (number1 < 0) {
                        number1 = 1;
                    }
                    number1 *= bitValue;
                } else {
                    if (number1 < 0) {
                        number1 = 0;
                    }
                    if (bitValue >= maxUnit) {
                        number1 += number2;
                        number1 *= bitValue;
                        maxUnit = bitValue;
                    } else {
                        number1 += number2 * bitValue;
                    }
                }
                number2 = -1;
            }
        }
        if (!hasDigit && cur < beaf.length() && !factory.getUnits().search(beaf, cur, 1).isHit()) {
            return offset;
        }
        if (number2 > 0) {
            if (number1 < 0) {
                number1 = number2;
            } else {
                number1 += number2;
            }
        }
        if (number1 >= 0) {
            collector.collect(String.valueOf(number1), offset, cur);

            // 后面可能跟了计量单位
            Hit wd;
            int i = cur + 1;
            while (i <= beaf.length() && (wd = factory.getUnits().search(beaf, cur, i - cur)).isHit()) {
                collector.collect(String.valueOf(number1) + beaf.subSequence(cur, i), offset, i);
                cur++;
                if (!wd.isUnclosed()) {
                    break;
                }
                i++;
            }
        }
        return cur;
    }

    private final int toNumber(char c) {
        switch (c) {
        case '零':
        case '〇':
            return 0;
        case '一':
        case '壹':
            return 1;
        case '二':
        case '两':
        case '俩':
        case '貳':
            return 2;
        case '三':
        case '叁':
            return 3;
        case '四':
        case '肆':
            return 4;
        case '五':
        case '伍':
            return 5;
        case '六':
        case '陸':
            return 6;
        case '柒':
        case '七':
            return 7;
        case '捌':
        case '八':
            return 8;
        case '九':
        case '玖':
            return 9;
        case '十':
        case '什':
            return 10;
        case '百':
        case '佰':
            return 100;
        case '千':
        case '仟':
            return 1000;
        case '万':
        case '萬':
            return 10000;
        case '亿':
        case '億':
            return 100000000;
        default:
            return -1;
        }
    }

}
