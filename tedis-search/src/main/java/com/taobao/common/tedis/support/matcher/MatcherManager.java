/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.support.matcher;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.analysis.Token;

import com.taobao.common.tedis.core.TedisManager;
import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.XAnalyzer;
import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.XFactory;
import com.taobao.common.tedis.support.lucene.analysis.xanalyzer.XTokenizer;

public class MatcherManager {

    private final static Log logger = LogFactory.getLog(MatcherManager.class);

    private int namespace = 0;

    private int minComplete = 1;

    private final static String BASE = "matcher-index";

    private final static String CACHEBASE = "matcher-cache";

    private TedisManager tedisManager;

    /**
     * 加载一个Match对象
     * @param item
     */
    public void load(Matchable item) {
        Set<String> tags = null;
        try {
            tags = paoding(item.matchString());
        } catch (IOException e) {
            logger.error("Paoding error:" + e);
        }
        if (tags != null) {
            for (String tag : tags) {
                for (String s : prefixes(tag)) {
                    tedisManager.getSetCommands().add(namespace, BASE, s);
                    tedisManager.getZSetCommands().add(namespace, s, item.matchKey(), item.matchScore());
                }
            }
        }
        logger.debug("Item loaded:" + item);
    }

    /**
     * 根据term匹配指定数量的结果
     * @param term
     * @param num 返回的数量，例如9为返回0-9共10个，-1为返回全部
     * @return
     */
    @SuppressWarnings("unchecked")
    public Set match(String term, int num) {
        Set<String> terms;
        try {
            terms = paoding(term);
        } catch (IOException e) {
            return null;
        }
        List<String> words = new ArrayList<String>();
        for (String t : terms) {
            for (String word : normalize(t)) {
                words.add(word);
            }
        }
        if (words.size() < minComplete) {
            return null;
        }
        String tmpkey = CACHEBASE + ":" + words;
        if (!tedisManager.hasKey(namespace, tmpkey)) {
            tedisManager.getZSetCommands().intersectAndStore(namespace, words.get(0), words, tmpkey);
            tedisManager.expire(namespace, tmpkey, 30, TimeUnit.SECONDS);
        }
        Set ids = tedisManager.getZSetCommands().range(namespace, tmpkey, 0, num);
        return ids;
    }

    private List<String> prefixes(String phrase) {
        List<String> result = new ArrayList<String>();
        for (String s : normalize(phrase)) {
            result.addAll(maps(s));
        }
        return result;
    }

    private static Set<String> normalize(String str) {
        Pattern pattern = Pattern.compile("([\u4e00-\u9fa5a-z0-9]+)");
        Matcher matcher = pattern.matcher(str.toLowerCase());
        Set<String> result = new HashSet<String>();
        while (matcher.find()) {
            result.add(matcher.group());
        }
        return result;
    }

    private Set<String> maps(String str) {
        Set<String> result = new HashSet<String>();
        for (int i = minComplete - 1; i < str.length(); i++) {
            result.add(str.substring(0, i + 1).trim());
        }
        return result;
    }

    public static Set<String> paoding(String str) throws IOException {
        Set<String> result = new HashSet<String>();
        XAnalyzer analyzer = XFactory.getQueryAnalyzer();
        XTokenizer ts = (XTokenizer) analyzer.tokenStream("", new StringReader(str));
        Token t;
        while ((t = ts.next()) != null) {
            result.add(t.termText());
        }
        return result;
    }

    public int getNamespace() {
        return namespace;
    }

    public void setNamespace(int namespace) {
        this.namespace = namespace;
    }

    public int getMinComplete() {
        return minComplete;
    }

    public void setMinComplete(int minComplete) {
        this.minComplete = minComplete;
    }

    public TedisManager getTedisManager() {
        return tedisManager;
    }

    public void setTedisManager(TedisManager tedisManager) {
        this.tedisManager = tedisManager;
    }

}
