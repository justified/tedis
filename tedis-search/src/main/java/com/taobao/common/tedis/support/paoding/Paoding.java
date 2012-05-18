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
 * Paoding是一个背着“刀箱”(内藏各种“刀”)毕生精力“解牛”的人，即“庖丁”。
 * <p>
 * 正因为他拥有各种不同的“刀”，而且能够识别什么“肉(字符)”应该用什么“刀”分割，所以他能游刃有余地把整头牛切割，成为合适的“肉片(词语)”。 <br>
 * 这里的“刀”由Knife扮演，各种“刀”由“刀箱”KnifeBox管理(Paoding对象本身就是一个KnifeBox)，
 * 并由KnifeBox决策什么时候出什么“刀”。
 *
 * @author Zhiliang Wang [qieqie.wang@gmail.com]
 *
 * @see Knife
 * @see KnifeBox
 * @see KnifeBoxBean
 *
 * @since 1.0
 */
public final class Paoding extends KnifeBox implements Knife {

    // -------------------------------------------------

    public int dissect(Collector collector, CharSequence beaf, int offset) {
        while (offset >= 0 && offset < beaf.length()) {
            offset = super.dissect(collector, beaf, offset);
        }
        return offset;
    }

}
