/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.config;

import java.util.List;

import com.taobao.common.tedis.Single;
import com.taobao.common.tedis.config.HAConfig.ServerProperties;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2012-4-23 ÉÏÎç10:09:15
 * @version 1.0
 */
public interface Router {

    public final class RouteData {

        public int[] weights;
        public Single[] group;
        public List<ServerProperties> props;

        public RouteData(List<ServerProperties> props, int[] weights, Single[] group) {
            this.props = props;
            this.weights = weights;
            this.group = group;
        }

        @Override
        public String toString() {
            return "RouteData{" + "weights=" + weights + ", group=" + group + ", props=" + props + '}';
        }
    }

    Single route() throws Exception;

    RouteData getRouteData();

    RouteData getAllRouteData();

    void onError(Single single);

    Single getAtomic(String key);

    void destroy();

}
