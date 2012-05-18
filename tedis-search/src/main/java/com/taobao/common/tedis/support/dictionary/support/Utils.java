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
package com.taobao.common.tedis.support.dictionary.support;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.taobao.common.tedis.support.dictionary.support.filewords.FileWordsReader;

/**
 * 
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 * 
 * @since 1.0
 * 
 */
public class Utils {

    public static int compare(CharSequence one, int begin, int count, CharSequence theOther) {
        for (int i = begin, j = 0; i < one.length() && j < Math.min(theOther.length(), count); i++, j++) {
            if (one.charAt(i) > theOther.charAt(j)) {
                return 1;
            } else if (one.charAt(i) < theOther.charAt(j)) {
                return -1;
            }
        }
        return count - theOther.length();
    }

    public static void main(String[] args) throws IOException {
        String dir = "CJK/locale/";
        // String name = "base";
        // String name = "语言";
        String name = "福州";
        // String name = "名人-外国";
        // String name = "x计量单位";
        // String name = "名人-中国";
        // String name = "xcharacter";
        // String name = "节日节气";

        LinkedList<String> words = FileWordsReader.readWords("dic/" + dir + name + ".dic").get(name);
        Set<String> set = new HashSet<String>(words);
        String[] array = set.toArray(new String[] {});
        Arrays.sort(array);
        // String last = "";
        for (int i = 0; i < array.length; i++) {
            // if (array[i].compareTo(last) <= 0) {
            // System.out.println(array[i] + "----" + last);
            // }
            // last = array[i];
            System.out.println(array[i]);
        }
        System.out.println("-" + array.length);
    }

    public static void main0(String[] args) throws IOException {
        String dir = "CJK";
        String name = "base";
        // String name = "语言";
        // String name = "名人-外国";
        // String name = "x计量单位";
        // String name = "名人-中国";
        // String name = "xcharacter";
        // String name = "节日节气";

        LinkedList<String> words = FileWordsReader.readWords("dic/" + dir + name + ".dic").get(name);
        Set<String> set = new HashSet<String>(words);
        String[] array = set.toArray(new String[] {});
        Arrays.sort(array);
        for (int i = 0; i < array.length; i++) {
            System.out.println(array[i]);
        }
        System.out.println("-" + array.length);
    }

    public static void main5(String[] args) throws IOException {
        String dir = "CJK/";
        String name = "base";

        HashSet<Integer> 字符集 = new HashSet<Integer>();
        字符集.add((int) '零');
        字符集.add((int) '一');
        字符集.add((int) '二');
        字符集.add((int) '两');
        字符集.add((int) '俩');
        字符集.add((int) '三');
        字符集.add((int) '四');
        字符集.add((int) '五');
        字符集.add((int) '六');
        字符集.add((int) '七');
        字符集.add((int) '八');
        字符集.add((int) '九');
        字符集.add((int) '十');
        字符集.add((int) '百');
        字符集.add((int) '千');
        字符集.add((int) '万');
        字符集.add((int) '亿');

        LinkedList<String> words = FileWordsReader.readWords("dic/" + dir + name + ".dic").get(name);
        System.out.println(words.size());
        Iterator<String> iter = words.iterator();
        while (iter.hasNext()) {
            String 元素 = (String) iter.next();

            if (元素.equals("二十五")) {
                System.out.println("--" + 元素);
            }
            int i = 0;
            for (; i < 元素.length(); i++) {
                if (!字符集.contains((int) 元素.charAt(i))) {
                    break;
                }
            }
            if (元素.equals("二十五")) {
                System.out.println(i);
            }
            if (i == 元素.length()) {
                System.out.println(元素);
                iter.remove();
            }
        }
        System.out.println(words.size());
    }

    public static boolean outb(char c) {
        return true;
    }

    /**
     * 字窜全角转半角的函数(DBC case) 全角空格为12288，半角空格为32
     * 其他字符半角(33-126)与全角(65281-65374)的对应关系是：均相差65248
     * 
     * @param input
     * @return
     */
    public static char toDbcCase(char src) {
        if (src == 12288) {
            src = (char) 32;
        } else if (src > 65280 && src < 65375) {
            src = (char) (src - 65248);
        }
        return src;
    }

}
