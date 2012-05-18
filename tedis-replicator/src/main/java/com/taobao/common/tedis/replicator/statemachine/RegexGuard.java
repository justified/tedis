/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.statemachine;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexGuard implements Guard {
    Pattern pattern;

    public RegexGuard(String regex) {
        pattern = Pattern.compile(regex);
    }

    public boolean accept(Event message, Entity entity, State state) {
        Object o = message.getData();
        if (o != null && o instanceof String) {
            Matcher m = pattern.matcher((String) o);
            return m.matches();
        } else
            return false;
    }
}
