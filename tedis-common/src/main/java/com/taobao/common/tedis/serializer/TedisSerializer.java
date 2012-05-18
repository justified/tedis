/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.serializer;

/**
 * @author juxin.zj E-mail:juxin.zj@taobao.com
 * @since 2011-7-25 上午11:55:30
 * @version 1.0
 */
public interface TedisSerializer<T> {

    /**
     * Serialize the given object to binary data.
     * 
     * @param t
     *            object to serialize
     * @return the equivalent binary data
     */
    byte[] serialize(T t) throws SerializationException;

    /**
     * Deserialize an object from the given binary data.
     * 
     * @param bytes
     *            object binary representation
     * @return the equivalent object instance
     */
    T deserialize(byte[] bytes) throws SerializationException;

}
