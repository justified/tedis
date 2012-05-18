/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.support.matcher;

/**
 * 做AutoComplete的对象需要实现此接口
 *
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-9-20 上午11:44:48
 * @version 1.0
 */
public interface Matchable {

    /**
     * @return 返回要Match的字符串，比如商品的标题等等
     */
    String matchString();

    /**
     * @return 返回Match对象的在tedis存储的key，比如商品的Id等等
     */
    <K> K matchKey();

    /**
     * @return 返回Match的score，match的时候按照此score排序
     */
    double matchScore();
}
