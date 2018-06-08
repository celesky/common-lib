package com.youhaoxi.base.luttuce.pool.standard;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.support.ConnectionPoolSupport;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class StandardClient {
    public static void main(String[] args) throws Exception{
        RedisClient client = RedisClient.create(RedisURI.create("47.106.140.44", 5354));

        GenericObjectPool<StatefulRedisConnection<String, String>> pool = ConnectionPoolSupport
                .createGenericObjectPool(() -> client.connect(), new GenericObjectPoolConfig());

        // executing work
        try (StatefulRedisConnection<String, String> connection = pool.borrowObject()) {

            RedisCommands<String, String> commands = connection.sync();
            commands.multi();
            commands.set("key", "value");
            commands.set("key2", "value2");
            commands.exec();
        }

        // terminating
        pool.close();
        client.shutdown();
    }

}
