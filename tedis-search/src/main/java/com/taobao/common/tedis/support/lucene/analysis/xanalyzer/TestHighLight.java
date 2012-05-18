/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.support.lucene.analysis.xanalyzer;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.TermPositionVector;
import org.apache.lucene.queryParser.QueryParser;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Searcher;
import org.apache.lucene.search.highlight.Formatter;
import org.apache.lucene.search.highlight.Highlighter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleFragmenter;
import org.apache.lucene.search.highlight.TokenGroup;
import org.apache.lucene.search.highlight.TokenSources;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

public class TestHighLight {
    private static String FIELD_NAME = "content";
    private static String CONTENT = "Since development first began on " + "Spring in 2003, there's been a constant buzz about it in " + "Java development publications and corporate IT departments."
            + " The reason is clear: Spring is a lightweight Java framework " + "in a world of complex heavyweight architectures that take "
            + "forever to implement. Spring is like a breath of fresh air " + "to overworked developers. In Spring, you can make an object "
            + "secure, remote, or transactional, with a couple of lines of " + "configuration instead of embedded code. The resulting application"
            + " is simple and clean. In Spring, you can work less and go home " + "early, because you can strip away a whole lot of the redundant "
            + "code that you tend to see in most J2EE applications. You won't" + " be nearly as burdened with meaningless detail. In Spring, you"
            + " can change your mind without the consequences bleeding through " + "your entire application. You'll adapt much more quickly than you "
            + "ever could before. Spring: A Developer's Notebook offers a quick " + "dive into the new Spring framework, designed to let you get "
            + "hands-on as quickly as you like. If you don't want to bother with" + " a lot of theory, this book is definitely for you. You'll work "
            + "through one example after another. Along the way, you'll discover " + "the energy and promise of the Spring framework. This practical guide "
            + "features ten code-intensive labs that'll rapidly get you up to speed." + " You'll learn how to do the following, and more: install the Spring"
            + " Framework set up the development environment use Spring with other " + "open source Java tools such as Tomcat, Struts, and Hibernate master "
            + "AOP and transactions utilize ORM solutions As with all titles in the" + " Developer's Notebook series, this no-nonsense book skips all the "
            + "boring prose and cuts right to the chase. It's an approach that " + "forces you to get your hands dirty by working through one instructional"
            + " example after another-examples that speak to you instead of at you. ";
    private static String QUERY = "tomcat";

    /**
     * @param args
     */
    public static void main(String[] args) {

        Directory ramDir = new RAMDirectory();
        try {
            IndexWriter writer = new IndexWriter(ramDir, /*
                                                          * new
                                                          * StandardAnalyzer()/
                                                          */XFactory.getWriterAnalyzer());
            Document doc = new Document();
            Field fd = new Field(FIELD_NAME, CONTENT, Field.Store.YES, Field.Index.TOKENIZED, Field.TermVector.WITH_POSITIONS_OFFSETS);
            doc.add(fd);
            writer.addDocument(doc);
            writer.optimize();
            writer.close();

            IndexReader reader = IndexReader.open(ramDir);
            String queryString = QUERY;
            QueryParser parser = new QueryParser(FIELD_NAME, /*
                                                              * new
                                                              * StandardAnalyzer
                                                              * ()/
                                                              */XFactory.getWriterAnalyzer());
            Query query = parser.parse(queryString);
            System.out.println(query);
            Searcher searcher = new IndexSearcher(ramDir);
            query = query.rewrite(reader);
            System.out.println(query);
            System.out.println("Searching for: " + query.toString(FIELD_NAME));
            Hits hits = searcher.search(query);

            BoldFormatter formatter = new BoldFormatter();
            Highlighter highlighter = new Highlighter(formatter, new QueryScorer(query));
            highlighter.setTextFragmenter(new SimpleFragmenter(50));
            for (int i = 0; i < hits.length(); i++) {
                String text = hits.doc(i).get(FIELD_NAME);
                int maxNumFragmentsRequired = 5;
                String fragmentSeparator = "...";
                TermPositionVector tpv = (TermPositionVector) reader.getTermFreqVector(hits.id(i), FIELD_NAME);
                TokenStream tokenStream = TokenSources.getTokenStream(tpv);
                /*
                 * TokenStream tokenStream2= (new StandardAnalyzer())
                 * //XFactory.getWriterAnalyzer() .tokenStream(FIELD_NAME,new
                 * StringReader(text));
                 *
                 * do { Token t = tokenStream2.next(); if(t==null)break;
                 * System.out.println("\t" + t.startOffset() + "," +
                 * t.endOffset() + "\t" + t.termText()); }while(true);
                 */
                String result = highlighter.getBestFragments(tokenStream, text, maxNumFragmentsRequired, fragmentSeparator);
                System.out.println("\n" + result);
            }
            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

class BoldFormatter implements Formatter {
    public String highlightTerm(String originalText, TokenGroup group) {
        if (group.getTotalScore() <= 0) {
            return originalText;
        }
        return "<b>" + originalText + "</b>";
    }
}
