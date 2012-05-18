/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.config;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-19 обнГ1:43:22
 * @version 1.0
 */
public interface ConfigManager {

    Router getRouter();

    void destroy();

}
