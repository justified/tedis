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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.util.LinkedList;
import java.util.Map;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.0
 *
 */
public class FileWordsReader {

    public static Map<String, LinkedList<String>> readWords(String fileOrDirectory) throws IOException {
        SimpleReadListener l = new SimpleReadListener();
        readWords(fileOrDirectory, l);
        return l.getResult();
    }

    public static void readWords(String fileOrDirectory, ReadListener l) throws IOException {
        String path = FileWordsReader.class.getResource(fileOrDirectory).getFile();
        File file = new File(URLDecoder.decode(path, "utf-8"));
        File[] files = new File[] { file };
        if (file.isDirectory()) {
            files = file.listFiles();
        }
        for (int i = 0; i < files.length; i++) {
            if (!l.onFileBegin(files[i].getName())) {
                continue;
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(files[i])));
            String word;
            while ((word = in.readLine()) != null) {
                l.onWord(word);
            }
            l.onFileEnd(files[i].getName());
            in.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String path = "/dic/CJK/x¸ÉÈÅ´Ê.dic";
        String solvePath = FileWordsReader.class.getResource(path).getFile();
        System.out.println(solvePath);
        System.out.println(URLDecoder.decode(solvePath, "utf-8"));
        File in = new File(FileWordsReader.class.getResource(path).getFile());
        System.out.println(in.getName());

//        Map<String, LinkedList<String>> map = readWords("dic/chinese/x/character.dic");
//        String[] baseWords = map.get("character").toArray(new String[0]);
//        System.out.println(Arrays.toString(baseWords));
//
//        Map<String, LinkedList<String>> map2 = readWords("dic/chinese");
//        String[] baseWords2 = map2.get("title").toArray(new String[0]);
//        System.out.println(Arrays.toString(baseWords2));
//        String[] baseWords3 = map2.get("xword").toArray(new String[0]);
//        System.out.println(Arrays.toString(baseWords3));
    }

}
