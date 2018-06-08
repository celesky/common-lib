package com.youhaoxi.base.luttuce.pool.cluster;

import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class ClusterClient {
    public static void main(String[] args) throws Exception{
        RedisClusterClient clusterClient = RedisClusterClient.create(RedisURI.create("47.106.140.44", 6300));

        GenericObjectPool<StatefulRedisClusterConnection<String, String>> pool = ConnectionPoolSupport
                .createGenericObjectPool(() -> clusterClient.connect(), new GenericObjectPoolConfig());

        // execute work
        try (StatefulRedisClusterConnection<String, String> connection = pool.borrowObject()) {
            connection.sync().set("key", "value");
            connection.sync().blpop(10, "list");
        }

        // terminating
        pool.close();
        clusterClient.shutdown();
    }
}
