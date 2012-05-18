/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
package com.taobao.common.tedis.replicator.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public final class PropertiesUtils {

    public static String getProperty(String filePath, String key) throws IOException {
        if (filePath == null) {
            return null;
        }
        Properties _properties = new Properties();
        FileInputStream fis = new FileInputStream(filePath);
        _properties.load(fis);
        fis.close();
        return _properties.getProperty(key);
    }

    public static void setProperty(String filePath, String key, String value) throws Exception {
        if (filePath == null) {
            return;
        }
        FileInputStream fin = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        List<String> contentList = new ArrayList<String>();
        try {
            fin = new FileInputStream(filePath);
            inputStreamReader = new InputStreamReader(fin);
            bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            key = key + "=";
            boolean insert = true;
            while ((line = bufferedReader.readLine()) != null) {
                if (line.trim().indexOf(key) != -1) {
                    key = key + value;
                    contentList.add(key);
                    insert = false;
                } else {
                    contentList.add(line);
                }
            }
            if (insert) {
                contentList.add(key + value);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
            if (fin != null) {
                fin.close();
            }
        }

        FileWriter fileWriter = null;
        try {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
            file.canWrite();
            fileWriter = new FileWriter(file, true);

            Iterator<String> it = contentList.iterator();
            while (it.hasNext()) {
                fileWriter.write(it.next().toString() + "\r\n");
            }
            fileWriter.flush();
        } catch (Exception e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    public static void setProperty(String filePath, Map<String, String> props) throws Exception {
        if (filePath == null) {
            return;
        }
        FileInputStream fin = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        List<String> contentList = new ArrayList<String>();
        try {
            fin = new FileInputStream(filePath);
            inputStreamReader = new InputStreamReader(fin);
            bufferedReader = new BufferedReader(inputStreamReader);
            String line = "";
            String key;
            int index = -1;
            List<String> insertList = new ArrayList<String>(props.keySet());
            while ((line = bufferedReader.readLine()) != null) {
                if ((index = line.trim().indexOf("=")) != -1) {
                    key = line.substring(0, index);
                    if (props.keySet().contains(key)) {
                        contentList.add(key + "=" + props.get(key));
                        insertList.remove(key);
                    } else {
                        contentList.add(line);
                    }
                } else {
                    contentList.add(line);
                }
            }
            for(String insert : insertList) {
                contentList.add(insert + "=" + props.get(insert));
            }
        } catch (Exception e) {
            throw e;
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
            if (inputStreamReader != null) {
                inputStreamReader.close();
            }
            if (fin != null) {
                fin.close();
            }
        }

        FileWriter fileWriter = null;
        try {
            File file = new File(filePath);
            if (file.exists()) {
                file.delete();
                file.createNewFile();
            }
            file.canWrite();
            fileWriter = new FileWriter(file, true);

            Iterator<String> it = contentList.iterator();
            while (it.hasNext()) {
                fileWriter.write(it.next().toString() + "\r\n");
            }
            fileWriter.flush();
        } catch (Exception e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println(PropertiesUtils.getProperty("./conf/relay.properties", "service.name"));
        Map<String, String> maps = new HashMap<String, String>();
        maps.put("service.name", "relay");
        PropertiesUtils.setProperty("./conf/relay.properties", maps);
        System.out.println(PropertiesUtils.getProperty("./conf/relay.properties", "service.name"));
        System.out.println(PropertiesUtils.getProperty("./conf/relay.properties", "test"));
    }

}
