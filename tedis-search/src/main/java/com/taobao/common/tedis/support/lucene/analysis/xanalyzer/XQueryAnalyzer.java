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
package com.taobao.common.tedis.support.lucene.analysis.xanalyzer;

import com.taobao.common.tedis.support.paoding.Knife;

/**
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @since 1.1
 */
public final class XQueryAnalyzer extends XAnalyzer {

    public XQueryAnalyzer() {
        super.setMode(QUERY_MODE);
    }

    public XQueryAnalyzer(Knife knife) {
        super.setMode(QUERY_MODE);
        setKnife(knife);
    }

    public final void setMode(int mode) {
        throw new IllegalStateException("this is a query mode, cound not change it.");
    }

}
