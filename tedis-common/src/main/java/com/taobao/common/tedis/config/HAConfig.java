/**
 * (C) 2011-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 */
/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.taobao.common.tedis.config;

import java.util.Arrays;
import java.util.List;

/**
 * 
 * @author jianxing <jianxing.qx@taobao.com>
 */
public class HAConfig {

    public List<ServerProperties> groups;
    public int pool_size = 50;
    public int timeout = 3000;
    public boolean failover = true;
    public String password;

    public HAConfig() {
    };

    public HAConfig(List<ServerProperties> servers) {
        this.groups = servers;
    }

    public List<ServerProperties> getServers() {
        return groups;
    }

    @Override
    public String toString() {
        return "HAConfig{" + "groups=" + groups + '}';
    }

    public static class ServerInfo {
        public String addr;
        public int port;

        public String generateKey() {
            return addr + "_" + port;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ServerInfo other = (ServerInfo) obj;
            if ((this.addr == null) ? (other.addr != null) : !this.addr.equals(other.addr)) {
                return false;
            }
            if (this.port != other.port) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = 3;
            hash = 41 * hash + addr.hashCode();
            hash = 41 * hash + this.port;
            return hash;
        }

        @Override
        public String toString() {
            return "addr=" + addr + ", port=" + port;
        }
    }

    public static class ServerProperties {
        public ServerInfo[] servers;
        public int readWeight = 10;
        public int pool_size;
        public int timeout;
        public String password;

        public ServerProperties() {
        }

        public ServerProperties(ServerInfo[] servers, int pool_size, int timeout) {
            this(servers, pool_size, timeout, null);
        }

        public ServerProperties(ServerInfo[] servers, int pool_size, int timeout, String password) {
            this.servers = servers;
            this.pool_size = pool_size;
            this.timeout = timeout;
            this.password = password;
        }

        public String generateKey() {
            return servers[0].generateKey() + "_" + pool_size + "_" + timeout;
        }

        @Override
        public String toString() {
            return "servers=" + Arrays.toString(servers) + ",read_weight=" + readWeight + ",pool_size=" + pool_size + ",timeout=" + timeout;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            final ServerProperties other = (ServerProperties) obj;
            if (this.servers == null || other.servers == null || !this.servers.equals(other.servers)) {
                return false;
            }
            if (this.readWeight != other.readWeight) {
                return false;
            }
            if (this.pool_size != other.pool_size) {
                return false;
            }
            if (this.timeout != other.timeout) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            int hash = this.servers.hashCode();
            hash = 41 * hash + this.readWeight;
            hash = 41 * hash + this.pool_size;
            hash = 41 * hash + this.timeout;
            return hash;
        }

    }

}
