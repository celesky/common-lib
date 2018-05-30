package com.youhaoxi.base.redis.command;

import com.youhaoxi.base.redis.JedisProviderFactoryBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

import static com.youhaoxi.base.redis.JedisProviderFactory.*;

/**
 * 集群客户端
 */
public class RedisCluster {
    protected static final Logger logger = LoggerFactory.getLogger(RedisCluster.class);

    /**
     *  默认缓存时长（7 天）
     */
    protected static final int DEFAULT_EXPIRE_TIME = 60 * 60 * 24 * 7;

    protected static final String RESP_OK = "OK";

    protected String groupName; //用默认的

    public RedisCluster(){
        this.groupName= JedisProviderFactoryBean.DEFAULT_GROUP_NAME;
    }
    //集群组
    public RedisCluster(String groupName) {
        this.groupName = groupName;
    }

    /* ==========================对value操作====================== */
    /**
     * 将字符串值 value 关联到 key 。 如果 key 已经持有其他值， SET 就覆写旧值，无视类型。
     * 对于某个原本带有生存时间（TTL）的键来说， 当 SET 命令成功在这个键上执行时， 这个键原有的 TTL 将被清除。
     *
     * 返回 OK
     *
     * @param key
     * @param value
     * @return
     */
    public boolean set(String key, String value) {
        if (value == null)
            return false;
        try {
            boolean result = false;
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).set(key, value).equals(RESP_OK);
            } else {
                result = getJedisCommands(groupName).set(key, value).equals(RESP_OK);
            }
            if (result) {
                result = setExpire(key,DEFAULT_EXPIRE_TIME);
            }
            return result;
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 为给定 key 设置生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除。
     *
     * @param seconds
     *            超时时间，单位：秒
     * @return true：超时设置成功
     *
     *         false：key不存在或超时未设置成功
     */
    public boolean setExpire(String key,long seconds) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).pexpire(key, seconds * 1000) == 1;
            } else {
                return getJedisCommands(groupName).pexpire(key, seconds * 1000) == 1;
            }

        } finally {
            getJedisProvider(groupName).release();
        }

    }
    /**
     * 删除给定的一个key
     *
     * 返回值：被删除key的数量
     *
     * @param key
     * @return
     */
    public long del(String key) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).del(key) ;
            } else {
                return getJedisCommands(groupName).del(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)
     *
     * 返回值： 当 key 不存在时，返回 -2 。 当 key 存在但没有设置剩余生存时间时，返回 -1 。 否则，以秒为单位，返回 key
     * 的剩余生存时间。
     *
     *
     * @param key
     * @return
     */
    public long ttl(String key) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).ttl(key);
            } else {
                return getJedisCommands(groupName).ttl(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 检查给定 key 是否存在
     *
     * 返回值： 若 key 存在，返回 1 ，否则返回 0 。
     *
     * @param key
     * @return
     */
    public boolean exists(String key) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).exists(key) ;
            } else {
                return getJedisCommands(groupName).exists(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 检查给定 key 是否存在
     *
     * 返回值： 若 key 存在，返回 1 ，否则返回 0 。
     *
     * @param key
     * @return
     */
    public boolean exists(byte[] key) {
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).exists(key) ;
            } else {
                return getBinaryJedisCommands(groupName).exists(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 返回 key 所储存的值的类型;
     *
     * 返回值： none (key不存在) string (字符串) list (列表) set (集合) zset (有序集) hash (哈希表)
     *
     * @param key
     * @return
     */
    public String type(String key) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).type(key) ;
            } else {
                return getJedisCommands(groupName).type(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 返回 key 所储存的值的类型;
     *
     * 返回值： none (key不存在) string (字符串) list (列表) set (集合) zset (有序集) hash (哈希表)
     *
     * @param key
     * @return
     */
    public String type(byte[] key) {
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).type(key) ;
            } else {
                return getBinaryJedisCommands(groupName).type(key);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 为给定 key 设置或更新生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除；
     *
     * 返回值： 设置成功返回 1 。 当 key 不存在或者不能为 key 设置生存时间时(比如在低于 2.1.3 版本的 Redis 中你尝试更新
     * key 的生存时间)，返回 0
     *
     * @param key
     * @param seconds
     * @return
     */
    public long expire(String key, int seconds) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).expire(key,seconds) ;
            } else {
                return getJedisCommands(groupName).expire(key,seconds);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 为给定 key 设置或更新生存时间，当 key 过期时(生存时间为 0 )，它会被自动删除；
     *
     * 返回值： 设置成功返回 1 。 当 key 不存在或者不能为 key 设置生存时间时(比如在低于 2.1.3 版本的 Redis 中你尝试更新
     * key 的生存时间)，返回 0
     *
     * @param key
     * @param seconds
     * @return
     */
    public long expire(byte[] key, int seconds) {
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).expire(key,seconds) ;
            } else {
                return getBinaryJedisCommands(groupName).expire(key,seconds);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 为给定 key 设置或更新过期时间；
     *
     * 返回值： 如果生存时间设置成功，返回 1 。 当 key 不存在或没办法设置生存时间，返回 0 。
     *
     * @param key
     * @param expiry
     * @return
     */
    public long expireAt(String key, Date expiry) {
        long unixTime = expiry.getTime() / 1000;
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).expireAt(key,unixTime) ;
            } else {
                return getJedisCommands(groupName).expireAt(key,unixTime);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 为给定 key 设置或更新过期时间；
     *
     * 返回值： 如果生存时间设置成功，返回 1 。 当 key 不存在或没办法设置生存时间，返回 0 。
     *
     * @param key
     * @param expiry
     * @return
     */
    public long expireAt(byte[] key, Date expiry) {
        long unixTime = expiry.getTime() / 1000;
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).expireAt(key,unixTime) ;
            } else {
                return getBinaryJedisCommands(groupName).expireAt(key,unixTime);
            }
        } finally {
            getJedisProvider(groupName).release();
        }
    }





    /**
     * 集群不支持mset
     *
     * 同时设置一个或多个 key-value 对。 如果某个给定 key 已经存在，那么 MSET
     * 会用新值覆盖原来的旧值，如果这不是你所希望的效果，请考虑使用 MSETNX 命令：它只会在所有给定 key 都不存在的情况下进行设置操作。
     * MSET 是一个原子性(atomic)操作，所有给定 key 都会在同一时间内被设置，某些给定 key 被更新而另一些给定 key
     * 没有改变的情况，不可能发生。
     *
     * 总是返回 OK (因为 MSET 不可能失败)
     *
     * @param keyValueMap
     * @return
     */
    @Deprecated
    public boolean mset(Map<String, String> keyValueMap) {
        return false;
    }

    /**
     * 对 key 所储存的字符串值，设置或清除指定偏移量上的位(bit)。 位的设置或清除取决于 value 参数，可以是 0 也可以是 1 。 当
     * key 不存在时，自动生成一个新的字符串值。 字符串会进行伸展(grown)以确保它可以将 value
     * 保存在指定的偏移量上。当字符串值进行伸展时，空白位置以 0 填充。 offset 参数必须大于或等于 0 ，小于 2^32 (bit 映射被限制在
     * 512 MB 之内)。
     *
     * 返回 指定偏移量原来储存的位。
     *
     * @param key
     * @param value
     * @return
     */
    public boolean setBit(String key, long offset, boolean value) {
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).setbit(key,offset,value) ;
            } else {
                return getJedisCommands(groupName).setbit(key,offset,value);
            }
        } catch (Exception e){
            logger.error("RedisCluster.setBit falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return false;
    }

    /**
     * 将 key 的值设为 value ，当且仅当 key 不存在。 若给定的 key 已经存在，则 SETNX 不做任何动作。 SETNX 是『SET
     * if Not eXists』(如果不存在，则 SET)的简写。
     *
     * 返回值： 设置成功，返回 1 。 设置失败，返回 0 。
     *
     * @param key
     * @param value
     * @return
     */
    public long setnx(String key, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                return getJedisClusterCommands(groupName).setnx(key,value) ;
            } else {
                return getJedisCommands(groupName).setnx(key,value);
            }
        } catch (Exception e){
            logger.error("RedisCluster.setnx falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }

        return result;

    }

    /**
     * 将 key 的值设为 value ，当且仅当 key 不存在。 若给定的 key 已经存在，则 SETNX 不做任何动作。 SETNX 是『SET
     * if Not eXists』(如果不存在，则 SET)的简写。
     *
     * 返回值： 设置成功，返回 1 。 设置失败，返回 0 。
     *
     * @param key
     * @param value
     * @return
     */
    public Long setnx(byte[] key, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).setnx(key,value) ;
            } else {
                return getBinaryJedisCommands(groupName).setnx(key,value);
            }
        } catch (Exception e){
            logger.error("RedisCluster.setnx falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;

    }

    /**
     * 将值 value 关联到 key ，并将 key 的生存时间设为 seconds (以秒为单位)。 如果 key 已经存在， SETEX
     * 命令将覆写旧值。
     *
     * 返回值： 设置成功时返回 OK 。 当 seconds 参数不合法时，返回一个错误。
     *
     * @param key
     * @param seconds
     * @param value
     * @return
     */
    public boolean setex(String key, int seconds, String value) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                return  getJedisClusterCommands(groupName).setex(key, seconds, value).equals(RESP_OK) ;

            } else {
                return  getJedisCommands(groupName).setex(key, seconds, value).equals(RESP_OK);

            }
        } catch (Exception e){
            logger.error("RedisCluster.setex falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 关联到 key ，并将 key 的生存时间设为 seconds (以秒为单位)。 如果 key 已经存在， SETEX
     * 命令将覆写旧值。
     *
     * 返回值： 设置成功时返回 OK 。 当 seconds 参数不合法时，返回一个错误。
     *
     * @param key
     * @param seconds
     * @param value
     * @return
     */
    public boolean setex(byte[] key, int seconds, byte[] value) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                return getBinaryJedisClusterCommands(groupName).setex(key, seconds, value).equals(RESP_OK) ;

            } else {
                return getBinaryJedisCommands(groupName).setex(key, seconds, value).equals(RESP_OK) ;
            }
        } catch (Exception e){
            logger.error("RedisCluster.setrange falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 用 value 参数覆写(overwrite)给定 key 所储存的字符串值，从偏移量 offset 开始。 不存在的 key
     * 当作空白字符串处理。
     *
     * 返回值： 被 SETRANGE 修改之后，字符串的长度。
     *
     * @param key
     * @param offset
     * @param value
     * @return
     */
    public long setrange(String key, long offset, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                Long _result =  getJedisClusterCommands(groupName).setrange(key, offset, value) ;
                if (_result != null) {
                    result = _result;
                }
            } else {
                Long _result =  getJedisCommands(groupName).setrange(key, offset, value);
                if (_result != null) {
                    result = _result;
                }
            }
        } catch (Exception e){
            logger.error("RedisCluster.setrange falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;

    }

    /**
     * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。 如果 key 不存在， APPEND
     * 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。
     *
     * 返回值： 追加 value 之后， key 中字符串的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long append(String key, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                Long _result =  getJedisClusterCommands(groupName).append(key, value) ;
                if (_result != null) {
                    result = _result;
                }
            } else {
                Long _result =  getJedisCommands(groupName).append(key, value);
                if (_result != null) {
                    result = _result;
                }
            }
        } catch (Exception e){
            logger.error("RedisCluster.append falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 如果 key 已经存在并且是一个字符串， APPEND 命令将 value 追加到 key 原来的值的末尾。 如果 key 不存在， APPEND
     * 就简单地将给定 key 设为 value ，就像执行 SET key value 一样。
     *
     * 返回值： 追加 value 之后， key 中字符串的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long append(byte[] key, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                Long _result =  getBinaryJedisClusterCommands(groupName).append(key, value) ;
                if (_result != null) {
                    result = _result;
                }
            } else {
                Long _result =  getBinaryJedisCommands(groupName).append(key, value);
                if (_result != null) {
                    result = _result;
                }
            }
        } catch (Exception e){
            logger.error("RedisCluster.append falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回 key 所关联的字符串值。 如果 key 不存在那么返回特殊值 nil 。 假如 key 储存的值不是字符串类型，返回一个错误，因为 GET
     * 只能用于处理字符串值。
     *
     * 返回值： 当 key 不存在时，返回 nil ，否则，返回 key 的值。 如果 key 不是字符串类型，那么返回一个错误。
     *
     * @param key
     * @return
     */
    public String get(String key) {
        String value=null;
        try {

            if (isCluster(groupName)) {
                value = getJedisClusterCommands(groupName).get(key);
            } else {
                value = getJedisCommands(groupName).get(key);
            }
            return value;
        } catch (Exception e){
            logger.error("RedisCluster.get falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return value;
    }

    /**
     * 返回 key 所关联的字符串值。 如果 key 不存在那么返回特殊值 nil 。 假如 key 储存的值不是字符串类型，返回一个错误，因为 GET
     * 只能用于处理字符串值。
     *
     * 返回值： 当 key 不存在时，返回 nil ，否则，返回 key 的值。 如果 key 不是字符串类型，那么返回一个错误。
     *
     * @param key
     * @return
     */
    public byte[] get(byte[] key) {
        byte[] value = null;
        try {

            if (isCluster(groupName)) {
                value = getBinaryJedisClusterCommands(groupName).get(key);;
            } else {
                value = getBinaryJedisCommands(groupName).get(key);
            }
            return value;
        } catch (Exception e){
            logger.error("RedisCluster.get falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return value;

    }

    /**
     * 返回所有(一个或多个)给定 key 的值。 如果给定的 key 里面，有某个 key 不存在，那么这个 key 返回特殊值 nil
     * 。因此，该命令永不失败。
     *
     * 一个包含所有给定 key 的值的列表。
     *
     * @param keys
     * @return
     */
    @Deprecated
    public List<String> mget(String... keys) {
        return null;
    }

    /**
     * 对 key 所储存的字符串值，获取指定偏移量上的位(bit)。当 offset 比字符串值的长度大，或者 key 不存在时，返回 0
     *
     * 返回值：字符串值指定偏移量上的位(bit)。
     *
     * true 或false 标识业务
     * 异常直接抛出
     * @param key
     * @return
     */
    public boolean getBit(String key, long offset) {
        try {
            boolean value;
            if (isCluster(groupName)) {
                value = getJedisClusterCommands(groupName).getbit(key,offset);
            } else {
                value = getJedisCommands(groupName).getbit(key,offset);
            }
            return value;
        } finally {
            getJedisProvider(groupName).release();
        }
    }

    /**
     * 返回 key 中字符串值的子字符串，字符串的截取范围由 start 和 end 两个偏移量决定(包括 start 和 end 在内)。
     * 负数偏移量表示从字符串最后开始计数， -1 表示最后一个字符， -2 表示倒数第二个，以此类推。
     *
     * 返回值： 截取得出的子字符串。
     *
     * @param key
     * @param startOffset
     * @param endOffset
     * @return
     */
    public String getrange(String key, long startOffset, long endOffset) {
        String value = null;
        try {

            if (isCluster(groupName)) {
                value = getJedisClusterCommands(groupName).getrange( key,  startOffset,  endOffset);
            } else {
                value = getJedisCommands(groupName).getrange( key,  startOffset,  endOffset);
            }
            return value;
        } catch (Exception e){
            logger.error("RedisCluster.getrange falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return value;
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。 当 key 存在但不是字符串类型时，返回一个错误。
     *
     * 返回值： 返回给定 key 的旧值。 当 key 没有旧值时，也即是， key 不存在时，返回 nil 。
     *
     * @param key
     * @param value
     * @return
     */
    public String getSet(String key, String value) {
        String oldValue=null;
        try {

            if (isCluster(groupName)) {
                oldValue = getJedisClusterCommands(groupName).getSet( key,  value);
            } else {
                oldValue = getJedisCommands(groupName).getSet( key,  value);
            }

        } catch (Exception e){
            logger.error("RedisCluster.getSet falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return oldValue;
    }

    /**
     * 将给定 key 的值设为 value ，并返回 key 的旧值(old value)。 当 key 存在但不是字符串类型时，返回一个错误。
     *
     * 返回值： 返回给定 key 的旧值。 当 key 没有旧值时，也即是， key 不存在时，返回 nil 。
     *
     * @param key
     * @param value
     * @return
     */
    public byte[] getSet(byte[] key, byte[] value) {
        byte[] oldValue=null;
        try {
            if (isCluster(groupName)) {
                oldValue = getBinaryJedisClusterCommands(groupName).getSet( key,  value);
            } else {
                oldValue = getBinaryJedisCommands(groupName).getSet( key,  value);
            }
            return oldValue;
        }catch (Exception e){
            logger.error("RedisCluster.decr falid", e);

        }finally {
            getJedisProvider(groupName).release();
        }
        return oldValue;
    }

    /**
     * 将 key 中储存的数字值减一。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 DECR 命令之后 key 的值。
     *
     * @param key
     * @return
     */
    public long decr(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).decr( key);
            } else {
                result = getJedisCommands(groupName).decr( key);
            }
            return result;

        }catch (Exception e){
            logger.error("RedisCluster.decr falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 中储存的数字值减一。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 DECR 命令之后 key 的值。
     *
     * @param key
     * @return
     */
    public long decr(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).decr( key);
            } else {
                result = getBinaryJedisCommands(groupName).decr( key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.decr falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 所储存的值减去减量 decrement 。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY
     * 操作。 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 减去 decrement 之后， key 的值。
     *
     * @param key
     * @param integer
     * @return
     */
    public long decrBy(String key, long integer) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).decrBy( key,integer);
            } else {
                result = getJedisCommands(groupName).decrBy( key,integer);
            }
        }catch (Exception e){
            logger.error("RedisCluster.decrBy falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 所储存的值减去减量 decrement 。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 DECRBY
     * 操作。 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 减去 decrement 之后， key 的值。
     *
     * @param key
     * @param integer
     * @return
     */
    public long decrBy(byte[] key, long integer) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).decrBy( key,integer);
            } else {
                result = getBinaryJedisCommands(groupName).decrBy( key,integer);
            }
        }catch (Exception e){
            logger.error("RedisCluster.decrBy falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 中储存的数字值增一。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 INCR 命令之后 key 的值。
     *
     * @param key
     * @return
     */
    public long incr(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).incr( key);
            } else {
                result = getJedisCommands(groupName).incr( key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.incr falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 中储存的数字值增一。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCR 操作。
     * 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 INCR 命令之后 key 的值。
     *
     * @param key
     * @return
     */
    public long incr(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).incr( key);
            } else {
                result = getBinaryJedisCommands(groupName).incr( key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.incr falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 所储存的值加上增量 increment 。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY
     * 命令。 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 加上 increment 之后， key 的值。
     *
     * @param key
     * @param integer
     * @return
     */
    public long incrBy(String key, long integer) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).incrBy( key,integer);
            } else {
                result = getJedisCommands(groupName).incrBy( key,integer);
            }
        }catch (Exception e){
            logger.error("RedisCluster.incrBy falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将 key 所储存的值加上增量 increment 。 如果 key 不存在，那么 key 的值会先被初始化为 0 ，然后再执行 INCRBY
     * 命令。 如果值包含错误的类型，或字符串类型的值不能表示为数字，那么返回一个错误。 本操作的值限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 加上 increment 之后， key 的值。
     *
     * @param key
     * @param integer
     * @return
     */
    public long incrBy(byte[] key, long integer) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).incrBy( key,integer);
            } else {
                result = getBinaryJedisCommands(groupName).incrBy( key,integer);
            }
        }catch (Exception e){
            logger.error("RedisCluster.incrBy falid", e);
            result = -10000;
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 排序，排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较；
     *
     * 返回键值从小到大排序的结果
     *
     * @param key
     * @return
     */
    public List<String> sort(String key) {
        List<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).sort( key);
            } else {
                result = getJedisCommands(groupName).sort( key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sort falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 排序，排序默认以数字作为对象，值被解释为双精度浮点数，然后进行比较；
     *
     * 返回键值从小到大排序的结果
     *
     * @param key
     * @return
     */
    public List<byte[]> sort(byte[] key) {
        List<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).sort( key);
            } else {
                result = getBinaryJedisCommands(groupName).sort( key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sort falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 排序，按SortingParams中的规则排序；
     *
     * 返回排序后的结果
     *
     * @param key
     * @param sortingParameters
     * @return
     */
    public List<String> sort(String key, SortingParams sortingParameters) {
        List<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).sort(key, sortingParameters);
            } else {
                result = getJedisCommands(groupName).sort(key, sortingParameters);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sort falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 排序，按SortingParams中的规则排序；
     *
     * 返回排序后的结果
     *
     * @param key
     * @param sortingParameters
     * @return
     */
    public List<byte[]> sort(byte[] key, SortingParams sortingParameters) {
        List<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).sort(key, sortingParameters);
            } else {
                result = getBinaryJedisCommands(groupName).sort(key, sortingParameters);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sort falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /* ==========================对Hash(哈希表)操作====================== */
    /**
     * 将哈希表 key 中的域 field 的值设为 value 。 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。 如果域
     * field 已经存在于哈希表中，旧值将被覆盖。
     *
     * 返回值： 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。 如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0
     * 。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hset(String key, String field, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hset(key,  field,  value);
            } else {
                result = getJedisCommands(groupName).hset(key,   field,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将哈希表 key 中的域 field 的值设为 value 。 如果 key 不存在，一个新的哈希表被创建并进行 HSET 操作。 如果域
     * field 已经存在于哈希表中，旧值将被覆盖。
     *
     * 返回值： 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。 如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0
     * 。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hset(byte[] key, byte[] field, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hset(key,  field,  value);
            } else {
                result = getBinaryJedisCommands(groupName).hset(key,   field,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。 若域 field 已经存在，该操作无效。 如果
     * key 不存在，一个新哈希表被创建并执行 HSETNX 命令。
     *
     * 返回值： 设置成功，返回 1 。 如果给定域已经存在且没有操作被执行，返回 0 。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hsetnx(String key, String field, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hsetnx(key,  field,  value);
            } else {
                result = getJedisCommands(groupName).hsetnx(key,   field,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hsetnx falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将哈希表 key 中的域 field 的值设置为 value ，当且仅当域 field 不存在。 若域 field 已经存在，该操作无效。 如果
     * key 不存在，一个新哈希表被创建并执行 HSETNX 命令。
     *
     * 返回值： 设置成功，返回 1 。 如果给定域已经存在且没有操作被执行，返回 0 。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hsetnx(byte[] key, byte[] field, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hsetnx(key, field,  value);
            } else {
                result = getBinaryJedisCommands(groupName).hsetnx(key, field,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hsetnx falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。 此命令会覆盖哈希表中已存在的域。 如果 key
     * 不存在，一个空哈希表被创建并执行 HMSET 操作。
     *
     * 返回值： 如果命令执行成功，返回 OK 。 当 key 不是哈希表(hash)类型时，返回一个错误。
     *
     * @param key
     * @param hash
     * @return
     */
    public boolean hmset(String key, Map<String, String> hash) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getJedisClusterCommands(groupName).hmset(key, hash);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status  = getJedisCommands(groupName).hmset(key,  hash);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        }catch (Exception e){
            logger.error("RedisCluster.hmset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中。 此命令会覆盖哈希表中已存在的域。 如果 key
     * 不存在，一个空哈希表被创建并执行 HMSET 操作。
     *
     * 返回值： 如果命令执行成功，返回 OK 。 当 key 不是哈希表(hash)类型时，返回一个错误。
     *
     * @param key
     * @param hash
     * @return
     */
    public boolean hmset(byte[] key, Map<byte[], byte[]> hash) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getBinaryJedisClusterCommands(groupName).hmset(key, hash);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status  = getBinaryJedisCommands(groupName).hmset(key,  hash);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        }catch (Exception e){
            logger.error("RedisCluster.hmset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 获取哈希表 key 中给定域 field 的值。
     *
     * 返回值： 给定域的值。 当给定域不存在或是给定 key 不存在时，返回 nil 。
     *
     * @param key
     * @param field
     * @return
     */
    public String hget(String key, String field) {
        String value = null;
        try {
            if (isCluster(groupName)) {
                value = getJedisClusterCommands(groupName).hget(key,field);
            } else {
                value = getJedisCommands(groupName).hget(key,field);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hmset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return value;
    }

    /**
     * 获取哈希表 key 中给定域 field 的值。
     *
     * 返回值： 给定域的值。 当给定域不存在或是给定 key 不存在时，返回 nil 。
     *
     * @param key
     * @param field
     * @return
     */
    public byte[] hget(byte[] key, byte[] field) {
        byte[] value = null;
        try {
            if (isCluster(groupName)) {
                value = getBinaryJedisClusterCommands(groupName).hget(key,field);
            } else {
                value  = getBinaryJedisCommands(groupName).hget(key,field);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hget falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return value;
    }

    /**
     * 返回哈希表 key 中，一个或多个给定域的值。 如果给定的域不存在于哈希表，那么返回一个 nil 值。 因为不存在的 key
     * 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表。
     *
     * 返回值： 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样。
     *
     * @param key
     * @param fields
     * @return
     */
    public List<String> hmget(String key, String... fields) {
        List<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hmget(key,fields);
            } else {
                result = getJedisCommands(groupName).hmget(key,fields);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hmget falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中，一个或多个给定域的值。 如果给定的域不存在于哈希表，那么返回一个 nil 值。 因为不存在的 key
     * 被当作一个空哈希表来处理，所以对一个不存在的 key 进行 HMGET 操作将返回一个只带有 nil 值的表。
     *
     * 返回值： 一个包含多个给定域的关联值的表，表值的排列顺序和给定域参数的请求顺序一样。
     *
     * @param key
     * @param fields
     * @return
     */
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        List<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hmget(key,fields);
            } else {
                result = getBinaryJedisCommands(groupName).hmget(key,fields);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hmget falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中，所有的域和值。 在返回值里，紧跟每个域名(field
     * name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
     *
     * 返回值： 以列表形式返回哈希表的域和域的值。 若 key 不存在，返回空列表。
     *
     * @param key
     * @return
     */
    public Map<String, String> hgetAll(String key) {
        Map<String, String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hgetAll(key);
            } else {
                result = getJedisCommands(groupName).hgetAll(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hgetAll falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中，所有的域和值。 在返回值里，紧跟每个域名(field
     * name)之后是域的值(value)，所以返回值的长度是哈希表大小的两倍。
     *
     * 返回值： 以列表形式返回哈希表的域和域的值。 若 key 不存在，返回空列表。
     *
     * @param key
     * @return
     */
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Map<byte[], byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hgetAll(key);
            } else {
                result = getBinaryJedisCommands(groupName).hgetAll(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hgetAll falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
     *
     * 返回值: 被成功移除的域的数量，不包括被忽略的域。
     *
     * @param key
     * @param fields
     * @return
     */
    public long hdel(String key, String... fields) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hdel(key);
            } else {
                result = getJedisCommands(groupName).hdel(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hdel falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 删除哈希表 key 中的一个或多个指定域，不存在的域将被忽略。
     *
     * 返回值: 被成功移除的域的数量，不包括被忽略的域。
     *
     * @param key
     * @param fields
     * @return
     */
    public long hdel(byte[] key, byte[]... fields) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hdel(key);
            } else {
                result = getBinaryJedisCommands(groupName).hdel(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hdel falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中域的数量。
     *
     * 返回值： 哈希表中域的数量。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long hlen(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hlen(key);
            } else {
                result = getJedisCommands(groupName).hlen(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hlen falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中域的数量。
     *
     * 返回值： 哈希表中域的数量。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long hlen(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hlen(key);
            } else {
                result = getBinaryJedisCommands(groupName).hlen(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hlen falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 查看哈希表 key 中，给定域 field 是否存在。
     *
     * 返回值： 如果哈希表含有给定域，返回 1 。 如果哈希表不含有给定域，或 key 不存在，返回 0 。
     *
     * @param key
     * @param field
     * @return
     */
    public boolean hexists(String key, String field) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hexists(key,field);
            } else {
                result = getJedisCommands(groupName).hexists(key,field);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hexists falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 查看哈希表 key 中，给定域 field 是否存在。
     *
     * 返回值： 如果哈希表含有给定域，返回 1 。 如果哈希表不含有给定域，或 key 不存在，返回 0 。
     *
     * @param key
     * @param field
     * @return
     */
    public boolean hexists(byte[] key, byte[] field) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hexists(key,field);
            } else {
                result = getBinaryJedisCommands(groupName).hexists(key,field);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hexists falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 为哈希表 key 中的域 field 的值加上增量 increment 。 增量也可以为负数，相当于对给定域进行减法操作。 如果 key
     * 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。 本操作的值被限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 HINCRBY 命令之后，哈希表 key 中域 field 的值。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hincrBy(String key, String field, long value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hincrBy(key,field,value);
            } else {
                result = getJedisCommands(groupName).hincrBy(key,field,value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hincrBy falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 为哈希表 key 中的域 field 的值加上增量 increment 。 增量也可以为负数，相当于对给定域进行减法操作。 如果 key
     * 不存在，一个新的哈希表被创建并执行 HINCRBY 命令。 如果域 field 不存在，那么在执行命令前，域的值被初始化为 0 。
     * 对一个储存字符串值的域 field 执行 HINCRBY 命令将造成一个错误。 本操作的值被限制在 64 位(bit)有符号数字表示之内。
     *
     * 返回值： 执行 HINCRBY 命令之后，哈希表 key 中域 field 的值。
     *
     * @param key
     * @param field
     * @param value
     * @return
     */
    public long hincrBy(byte[] key, byte[] field, long value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hincrBy(key,field,value);
            } else {
                result = getBinaryJedisCommands(groupName).hincrBy(key,field,value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hincrBy falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中的所有域。
     *
     * 返回值： 一个包含哈希表中所有域的表。 当 key 不存在时，返回一个空表。
     *
     * @param key
     * @return
     */
    public Set<String> hkeys(String key) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hkeys(key);
            } else {
                result = getJedisCommands(groupName).hkeys(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hkeys falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中的所有域。
     *
     * 返回值： 一个包含哈希表中所有域的表。 当 key 不存在时，返回一个空表。
     *
     * @param key
     * @return
     */
    public Set<byte[]> hkeys(byte[] key) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hkeys(key);
            } else {
                result = getBinaryJedisCommands(groupName).hkeys(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hkeys falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中所有域的值。
     *
     * 返回值： 一个包含哈希表中所有值的表。 当 key 不存在时，返回一个空表。
     *
     * @param key
     * @return
     */
    public List<String> hvals(String key) {
        List<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).hvals(key);
            } else {
                result = getJedisCommands(groupName).hvals(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hvals falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回哈希表 key 中所有域的值。
     *
     * 返回值： 一个包含哈希表中所有值的表。 当 key 不存在时，返回一个空表。
     *
     * @param key
     * @return
     */
    public Collection<byte[]> hvals(byte[] key) {
        Collection<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).hvals(key);
            } else {
                result = getBinaryJedisCommands(groupName).hvals(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.hvals falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /* ==========================对Set(集合)操作====================== */
    /**
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。 假如 key 不存在，则创建一个只包含
     * member 元素作成员的集合。 当 key 不是集合类型时，返回一个错误。
     *
     * 返回值: 被添加到集合中的新元素的数量，不包括被忽略的元素。
     *
     * @param key
     * @param members
     * @return
     */
    public long sadd(String key, String... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).sadd(key,members);
            } else {
                result = getJedisCommands(groupName).sadd(key,members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将一个或多个 member 元素加入到集合 key 当中，已经存在于集合的 member 元素将被忽略。 假如 key 不存在，则创建一个只包含
     * member 元素作成员的集合。 当 key 不是集合类型时，返回一个错误。
     *
     * 返回值: 被添加到集合中的新元素的数量，不包括被忽略的元素。
     *
     * @param key
     * @param members
     * @return
     */
    public long sadd(byte[] key, byte[]... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).sadd(key,members);
            } else {
                result = getBinaryJedisCommands(groupName).sadd(key,members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。 当 key 不是集合类型，返回一个错误。
     *
     * 返回值: 被成功移除的元素的数量，不包括被忽略的元素。
     *
     * @param key
     * @param members
     * @return
     */
    public long srem(String key, String... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).srem(key,members);
            } else {
                result = getJedisCommands(groupName).srem(key,members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.srem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除集合 key 中的一个或多个 member 元素，不存在的 member 元素会被忽略。 当 key 不是集合类型，返回一个错误。
     *
     * 返回值: 被成功移除的元素的数量，不包括被忽略的元素。
     *
     * @param key
     * @param members
     * @return
     */
    public long srem(byte[] key, byte[]... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).srem(key,members);
            } else {
                result = getBinaryJedisCommands(groupName).srem(key,members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.srem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回集合 key 中的所有成员。 不存在的 key 被视为空集合。
     *
     * 返回值: 集合中的所有成员。
     *
     * @param key
     * @return
     */
    public Set<String> smembers(String key) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).smembers(key);
            } else {
                result = getJedisCommands(groupName).smembers(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.smembers falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回集合 key 中的所有成员。 不存在的 key 被视为空集合。
     *
     * 返回值: 集合中的所有成员。
     *
     * @param key
     * @return
     */
    public Set<byte[]> smembers(byte[] key) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).smembers(key);
            } else {
                result = getBinaryJedisCommands(groupName).smembers(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.smembers falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 判断 member 元素是否集合 key 的成员。
     *
     * 返回值: 如果 member 元素是集合的成员，返回 1 。 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
     *
     * @param key
     * @param member
     * @return
     */
    public boolean sismember(String key, String member) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).sismember(key,member);
            } else {
                result = getJedisCommands(groupName).sismember(key,member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sismember falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 判断 member 元素是否集合 key 的成员。
     *
     * 返回值: 如果 member 元素是集合的成员，返回 1 。 如果 member 元素不是集合的成员，或 key 不存在，返回 0 。
     *
     * @param key
     * @param member
     * @return
     */
    public boolean sismember(byte[] key, byte[] member) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).sismember(key,member);
            } else {
                result = getBinaryJedisCommands(groupName).sismember(key,member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.sismember falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回集合 key 的基数(集合中元素的数量)。
     *
     * 返回值： 集合的基数。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long scard(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).scard(key);
            } else {
                result = getJedisCommands(groupName).scard(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.scard falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回集合 key 的基数(集合中元素的数量)。
     *
     * 返回值： 集合的基数。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long scard(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).scard(key);
            } else {
                result = getBinaryJedisCommands(groupName).scard(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.scard falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回集合中的一个随机元素。
     *
     * 返回值: 被移除的随机元素。 当 key 不存在或 key 是空集时，返回 nil 。
     *
     * @param key
     * @return
     */
    public String spop(String key) {
        String result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).spop(key);
            } else {
                result = getJedisCommands(groupName).spop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.spop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回集合中的一个随机元素。
     *
     * 返回值: 被移除的随机元素。 当 key 不存在或 key 是空集时，返回 nil 。
     *
     * @param key
     * @return
     */
    public byte[] spop(byte[] key) {
        byte[] result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).spop(key);
            } else {
                result = getBinaryJedisCommands(groupName).spop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.spop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而
     * SRANDMEMBER 则仅仅返回随机元素，而不对集合进行任何改动。
     *
     * 返回值: 只提供 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count
     * 参数，那么返回一个数组；如果集合为空，返回空数组。
     *
     * @param key
     * @return
     */
    public String srandmember(String key) {
        String result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).srandmember(key);
            } else {
                result = getJedisCommands(groupName).srandmember(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.srandmember falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 如果命令执行时，只提供了 key 参数，那么返回集合中的一个随机元素。该操作和 SPOP 相似，但 SPOP 将随机元素从集合中移除并返回，而
     * SRANDMEMBER 则仅仅返回随机元素，而不对集合进行任何改动。
     *
     * 返回值: 只提供 key 参数时，返回一个元素；如果集合为空，返回 nil 。 如果提供了 count
     * 参数，那么返回一个数组；如果集合为空，返回空数组。
     *
     * @param key
     * @return
     */
    public byte[] srandmember(byte[] key) {
        byte[] result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).srandmember(key);
            } else {
                result = getBinaryJedisCommands(groupName).srandmember(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.srandmember falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /* ==========================对List(列表)操作====================== */
    /**
     * 将一个或多个值 value 插入到列表 key 的表头 如果有多个 value 值，那么各个 value
     * 值按从左到右的顺序依次插入到表头LPUSH mylist a b c ，列表的值将是 c b a;
     *
     * 返回值： 执行 LPUSH 命令后，列表的长度。
     *
     * @param key
     * @param values
     * @return
     */
    public long lpush(String key, String... values) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lpush(key);
            } else {
                result = getJedisCommands(groupName).lpush(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表头 如果有多个 value 值，那么各个 value
     * 值按从左到右的顺序依次插入到表头LPUSH mylist a b c ，列表的值将是 c b a;
     *
     * 返回值： 执行 LPUSH 命令后，列表的长度。
     *
     * @param key
     * @param values
     * @return
     */
    public long lpush(byte[] key, byte[]... values) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lpush(key);
            } else {
                result = getBinaryJedisCommands(groupName).lpush(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 的表头，当且仅当 key 存在并且是一个列表。当 key 不存在时， LPUSHX 命令什么也不做。
     *
     * 返回值： LPUSHX 命令执行之后，表的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long lpushx(String key, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lpushx(key);
            } else {
                result = getJedisCommands(groupName).lpushx(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 的表头，当且仅当 key 存在并且是一个列表。当 key 不存在时， LPUSHX 命令什么也不做。
     *
     * 返回值： LPUSHX 命令执行之后，表的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long lpushx(byte[] key, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lpushx(key);
            } else {
                result = getBinaryJedisCommands(groupName).lpushx(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾(最右边)。 如果有多个 value 值，那么各个 value
     * 值按从左到右的顺序依次插入到表尾：比如对一个空列表 mylist 执行 RPUSH mylist a b c ，得出的结果列表为 a b c。如果
     * key 不存在，一个空列表会被创建并执行 RPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
     *
     * 返回值： 执行 RPUSH 操作后，表的长度。
     *
     * @param key
     * @param values
     * @return
     */
    public long rpush(String key, String... values) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).rpush(key,values);
            } else {
                result = getJedisCommands(groupName).rpush(key,values);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将一个或多个值 value 插入到列表 key 的表尾(最右边)。 如果有多个 value 值，那么各个 value
     * 值按从左到右的顺序依次插入到表尾：比如对一个空列表 mylist 执行 RPUSH mylist a b c ，得出的结果列表为 a b c。如果
     * key 不存在，一个空列表会被创建并执行 RPUSH 操作。 当 key 存在但不是列表类型时，返回一个错误。
     *
     * 返回值： 执行 RPUSH 操作后，表的长度。
     *
     * @param key
     * @param values
     * @return
     */
    public long rpush(byte[] key, byte[]... values) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).rpush(key,values);
            } else {
                result = getBinaryJedisCommands(groupName).rpush(key,values);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpush falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 的表尾，当且仅当 key 存在并且是一个列表。 和 RPUSH 命令相反，当 key 不存在时，
     * RPUSHX 命令什么也不做。
     *
     * 返回值： RPUSHX 命令执行之后，表的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long rpushx(String key, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).rpushx(key,value);
            } else {
                result = getJedisCommands(groupName).rpushx(key,value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpushx falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 的表尾，当且仅当 key 存在并且是一个列表。 和 RPUSH 命令相反，当 key 不存在时，
     * RPUSHX 命令什么也不做。
     *
     * 返回值： RPUSHX 命令执行之后，表的长度。
     *
     * @param key
     * @param value
     * @return
     */
    public long rpushx(byte[] key, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).rpushx(key,value);
            } else {
                result = getBinaryJedisCommands(groupName).rpushx(key,value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpushx falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的头元素。
     *
     * 返回值： 列表的头元素。 当 key 不存在时，返回 nil 。
     *
     * @param key
     * @return
     */
    public String lpop(String key) {
        String result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lpop(key);
            } else {
                result = getJedisCommands(groupName).lpop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的头元素。
     *
     * 返回值： 列表的头元素。 当 key 不存在时，返回 nil 。
     *
     * @param key
     * @return
     */
    public byte[] lpop(byte[] key) {
        byte[] result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lpop(key);
            } else {
                result = getBinaryJedisCommands(groupName).lpop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lpop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的尾元素。
     *
     * 返回值： 列表的尾元素。 当 key 不存在时，返回 nil 。
     *
     * @param key
     * @return
     */
    public String rpop(String key) {
        String result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).rpop(key);
            } else {
                result = getJedisCommands(groupName).rpop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除并返回列表 key 的尾元素。
     *
     * 返回值： 列表的尾元素。 当 key 不存在时，返回 nil 。
     *
     * @param key
     * @return
     */
    public byte[] rpop(byte[] key) {
        byte[] result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).rpop(key);
            } else {
                result = getBinaryJedisCommands(groupName).rpop(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.rpop falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 的长度。 如果 key 不存在，则 key 被解释为一个空列表，返回 0 . 如果 key 不是列表类型，返回一个错误。
     *
     * 返回值： 列表 key 的长度。
     *
     * @param key
     * @return
     */
    public long llen(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).llen(key);
            } else {
                result = getJedisCommands(groupName).llen(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.llen falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 的长度。 如果 key 不存在，则 key 被解释为一个空列表，返回 0 . 如果 key 不是列表类型，返回一个错误。
     *
     * 返回值： 列表 key 的长度。
     *
     * @param key
     * @return
     */
    public long llen(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).llen(key);
            } else {
                result = getBinaryJedisCommands(groupName).llen(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.llen falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 stop 下标也在 LRANGE
     * 命令的取值范围之内(闭区间)。超出范围的下标值不会引起错误。
     *
     * 返回值: 一个列表，包含指定区间内的元素。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<String> lrange(String key, long start, long end) {
        List<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lrange(key, start,  end);
            } else {
                result = getJedisCommands(groupName).lrange(key, start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 中指定区间内的元素，区间以偏移量 start 和 stop 指定。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 stop 下标也在 LRANGE
     * 命令的取值范围之内(闭区间)。超出范围的下标值不会引起错误。
     *
     * 返回值: 一个列表，包含指定区间内的元素。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public List<byte[]> lrange(byte[] key, int start, int end) {
        List<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lrange(key, start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).lrange(key, start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素。
     *
     * count 的值可以是以下几种： count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。 count
     * < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。 count = 0 : 移除表中所有与
     * value 相等的值。
     *
     * 返回值： 被移除元素的数量。 因为不存在的 key 被视作空表(empty list)，所以当 key 不存在时， LREM 命令总是返回 0 。
     *
     * @param key
     * @param count
     * @param value
     * @return
     */
    public long lrem(String key, long count, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lrem(key, count,  value);
            } else {
                result = getJedisCommands(groupName).lrem(key, count,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lrem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 根据参数 count 的值，移除列表中与参数 value 相等的元素。
     *
     * count 的值可以是以下几种： count > 0 : 从表头开始向表尾搜索，移除与 value 相等的元素，数量为 count 。 count
     * < 0 : 从表尾开始向表头搜索，移除与 value 相等的元素，数量为 count 的绝对值。 count = 0 : 移除表中所有与
     * value 相等的值。
     *
     * 返回值： 被移除元素的数量。 因为不存在的 key 被视作空表(empty list)，所以当 key 不存在时， LREM 命令总是返回 0 。
     *
     * @param key
     * @param count
     * @param value
     * @return
     */
    public long lrem(byte[] key, int count, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lrem(key, count,  value);
            } else {
                result = getBinaryJedisCommands(groupName).lrem(key, count,  value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lrem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将列表 key 下标为 index 的元素的值设置为 value 。
     *
     * 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
     *
     * 返回值： 操作成功返回 ok ，否则返回错误信息。
     *
     * @param key
     * @param index
     * @param value
     * @return
     */
    public boolean lset(String key, long index, String value) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getJedisClusterCommands(groupName).lset(key, index,  value);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status = getJedisCommands(groupName).lset(key, index,  value);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        }catch (Exception e){
            logger.error("RedisCluster.lset falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将列表 key 下标为 index 的元素的值设置为 value 。
     *
     * 当 index 参数超出范围，或对一个空列表( key 不存在)进行 LSET 时，返回一个错误。
     *
     * 返回值： 操作成功返回 ok ，否则返回错误信息。
     *
     * @param key
     * @param index
     * @param value
     * @return
     */
    public boolean lset(byte[] key, int index, byte[] value) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getBinaryJedisClusterCommands(groupName).lset(key, index, value);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status = getBinaryJedisCommands(groupName).lset(key, index, value);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        } catch (Exception e) {
            logger.error("RedisCluster.lset falid", e);
        } finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。 举个例子，执行命令 LTRIM list
     * 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 当 key
     * 不是列表类型时，返回一个错误。超出范围的下标值不会引起错误。
     *
     * 返回值: 命令执行成功时，返回 ok 。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public boolean ltrim(String key, long start, long end) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getJedisClusterCommands(groupName).ltrim(key, start,  end);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status = getJedisCommands(groupName).ltrim(key, start,  end);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        }catch (Exception e){
            logger.error("RedisCluster.ltrim falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 对一个列表进行修剪(trim)，就是说，让列表只保留指定区间内的元素，不在指定区间之内的元素都将被删除。 举个例子，执行命令 LTRIM list
     * 0 2 ，表示只保留列表 list 的前三个元素，其余元素全部删除。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 当 key
     * 不是列表类型时，返回一个错误。超出范围的下标值不会引起错误。
     *
     * 返回值: 命令执行成功时，返回 ok 。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public boolean ltrim(byte[] key, int start, int end) {
        boolean result = false;
        try {
            if (isCluster(groupName)) {
                String status = getBinaryJedisClusterCommands(groupName).ltrim(key, start,  end);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            } else {
                String status = getBinaryJedisCommands(groupName).ltrim(key, start,  end);
                if ("OK".equalsIgnoreCase(status)) {
                    result = true;
                }
            }
        }catch (Exception e){
            logger.error("RedisCluster.ltrim falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 中，下标为 index 的元素。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 如果 key 不是列表类型，返回一个错误。
     *
     * 返回值: 列表中下标为 index 的元素。 如果 index 参数的值不在列表的区间范围内(out of range)，返回 nil 。
     *
     * @param key
     * @param index
     * @return
     */
    public String lindex(String key, long index) {
        String result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).lindex(key, index);
            } else {
                result = getJedisCommands(groupName).lindex(key, index);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lindex falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回列表 key 中，下标为 index 的元素。
     *
     * 下标(index)参数 start 和 stop 都以 0 为底，也就是说，以 0 表示列表的第一个元素，以 1 表示列表的第二个元素，以此类推。
     * 你也可以使用负数下标，以 -1 表示列表的最后一个元素， -2 表示列表的倒数第二个元素，以此类推。 如果 key 不是列表类型，返回一个错误。
     *
     * 返回值: 列表中下标为 index 的元素。 如果 index 参数的值不在列表的区间范围内(out of range)，返回 nil 。
     *
     * @param key
     * @param index
     * @return
     */
    public byte[] lindex(byte[] key, int index) {
        byte[] result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).lindex(key, index);
            } else {
                result = getBinaryJedisCommands(groupName).lindex(key, index);
            }
        }catch (Exception e){
            logger.error("RedisCluster.lindex falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 当中，位于值 pivot 之前或之后。
     *
     * 当 pivot 不存在于列表 key 时，不执行任何操作。 当 key 不存在时， key 被视为空列表，不执行任何操作。 如果 key
     * 不是列表类型，返回一个错误。
     *
     * 返回值: 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1 。 如果 key 不存在或为空列表，返回
     * 0 。
     *
     * @param key
     * @param where
     * @param pivot
     * @param value
     * @return
     */
    public long linsert(String key, BinaryClient.LIST_POSITION where, String pivot, String value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).linsert(key, where, pivot, value);
            } else {
                result = getJedisCommands(groupName).linsert(key, where, pivot, value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.linsert falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将值 value 插入到列表 key 当中，位于值 pivot 之前或之后。
     *
     * 当 pivot 不存在于列表 key 时，不执行任何操作。 当 key 不存在时， key 被视为空列表，不执行任何操作。 如果 key
     * 不是列表类型，返回一个错误。
     *
     * 返回值: 如果命令执行成功，返回插入操作完成之后，列表的长度。 如果没有找到 pivot ，返回 -1 。 如果 key 不存在或为空列表，返回
     * 0 。
     *
     * @param key
     * @param where
     * @param pivot
     * @param value
     * @return
     */
    public long linsert(byte[] key, BinaryClient.LIST_POSITION where, byte[] pivot, byte[] value) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).linsert(key, where, pivot, value);
            } else {
                result = getBinaryJedisCommands(groupName).linsert(key, where, pivot, value);
            }
        }catch (Exception e){
            logger.error("RedisCluster.linsert falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /* ==========================对Sorted set(有序集)操作====================== */
    /**
     * 将一个member 元素及其 score 值加入到有序集 key 当中。
     *
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该
     * member 在正确的位置上。 score 值可以是整数值或双精度浮点数。 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。 当
     * key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public long zadd(String key, double score, String member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zadd(key, score, member);
            } else {
                result = getJedisCommands(groupName).zadd(key, score, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将一个member 元素及其 score 值加入到有序集 key 当中。
     *
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该
     * member 在正确的位置上。 score 值可以是整数值或双精度浮点数。 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。 当
     * key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public long zadd(byte[] key, double score, byte[] member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zadd(key, score, member);
            } else {
                result = getBinaryJedisCommands(groupName).zadd(key, score, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将多个member 元素及其 score 值加入到有序集 key 当中。
     *
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该
     * member 在正确的位置上。 score 值可以是整数值或双精度浮点数。 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。 当
     * key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
     *
     * @param key
     * @param scoreMembers
     * @return
     */
    public long zadd(String key, Map<String, Double> scoreMembers) {
        if (scoreMembers == null || scoreMembers.size() == 0) {
            return 0;
        }
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zadd(key, scoreMembers);
            } else {
                result = getJedisCommands(groupName).zadd(key, scoreMembers);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 将多个member 元素及其 score 值加入到有序集 key 当中。
     *
     * 如果某个 member 已经是有序集的成员，那么更新这个 member 的 score 值，并通过重新插入这个 member 元素，来保证该
     * member 在正确的位置上。 score 值可以是整数值或双精度浮点数。 如果 key 不存在，则创建一个空的有序集并执行 ZADD 操作。 当
     * key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功添加的新成员的数量，不包括那些被更新的、已经存在的成员。
     *
     * @param key
     * @param scoreMembers
     * @return
     */
    public long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        if (scoreMembers == null || scoreMembers.size() == 0) {
            return 0;
        }
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zadd(key, scoreMembers);
            } else {
                result = getBinaryJedisCommands(groupName).zadd(key, scoreMembers);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。 当 key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功移除的成员的数量，不包括被忽略的成员。
     *
     * @param key
     * @param members
     * @return
     */
    public long zrem(String key, String... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrem(key, members);
            } else {
                result = getJedisCommands(groupName).zrem(key, members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中的一个或多个成员，不存在的成员将被忽略。 当 key 存在但不是有序集类型时，返回一个错误。
     *
     * 返回值: 被成功移除的成员的数量，不包括被忽略的成员。
     *
     * @param key
     * @param members
     * @return
     */
    public long zrem(byte[] key, byte[]... members) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrem(key, members);
            } else {
                result = getBinaryJedisCommands(groupName).zrem(key, members);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrem falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 的集合中的数量。
     *
     * 返回值: 当 key 存在且是有序集类型时，返回有序集的基数。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long zcard(String key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zcard(key);
            } else {
                result = getJedisCommands(groupName).zcard(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcard falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 的集合中的数量。
     *
     * 返回值: 当 key 存在且是有序集类型时，返回有序集的基数。 当 key 不存在时，返回 0 。
     *
     * @param key
     * @return
     */
    public long zcard(byte[] key) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zcard(key);
            } else {
                result = getBinaryJedisCommands(groupName).zcard(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcard falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
     *
     * 返回值: score 值在 min 和 max 之间的成员的数量。
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public long zcount(String key, double min, double max) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zcount( key,  min,  max);
            } else {
                result = getJedisCommands(groupName).zcount( key,  min,  max);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcount falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值在 min 和 max 之间(默认包括 score 值等于 min 或 max )的成员的数量。
     *
     * 返回值: score 值在 min 和 max 之间的成员的数量。
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public long zcount(byte[] key, double min, double max) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zcount( key,  min,  max);
            } else {
                result = getBinaryJedisCommands(groupName).zcount( key,  min,  max);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcount falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， member的score值在minmember和maxmember的score值之间(默认包括
     * member的score值等于minmember或maxmemberscore值)的成员的数量。
     *
     * 返回值: member的score值在minmember和maxmember的score值之间的成员的数量。
     *
     * @param key
     * @param minmember
     * @param maxmember
     * @return
     */
    public long zcount(String key, String minmember, String maxmember) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zcount( key,  minmember,  maxmember);
            } else {
                result = getJedisCommands(groupName).zcount( key,  minmember,  maxmember);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcount falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， member的score值在minmember和maxmember的score值之间(默认包括
     * member的score值等于minmember或maxmemberscore值)的成员的数量。
     *
     * 返回值: member的score值在minmember和maxmember的score值之间的成员的数量。
     *
     * @param key
     * @param minmember
     * @param maxmember
     * @return
     */
    public long zcount(byte[] key, byte[] minmember, byte[] maxmember) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zcount( key,  minmember,  maxmember);
            } else {
                result = getBinaryJedisCommands(groupName).zcount( key,  minmember,  maxmember);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zcount falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，成员 member 的 score 值。如果 member 元素不是有序集 key 的成员，或 key 不存在，返回
     * nil 。
     *
     * 返回值: member 成员的 score 值，以字符串形式表示。
     *
     * @param key
     * @param member
     * @return
     */
    public double zscore(String key, String member) {
        double result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zscore( key,  member);
            } else {
                result = getJedisCommands(groupName).zscore( key,  member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zscore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，成员 member 的 score 值。如果 member 元素不是有序集 key 的成员，或 key 不存在，返回
     * nil 。
     *
     * 返回值: member 成员的 score 值，以字符串形式表示。
     *
     * @param key
     * @param member
     * @return
     */
    public double zscore(byte[] key, byte[] member) {
        double result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zscore( key,  member);
            } else {
                result = getBinaryJedisCommands(groupName).zscore( key,  member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zscore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 为有序集 key 的成员 member 的 score 值加上增量 increment 。
     *
     * 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让
     * member 的 score 值减去 5 。 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key
     * increment member 等同于 ZADD key increment member 。 当 key 不是有序集类型时，返回一个错误。
     * score 值可以是整数值或双精度浮点数。
     *
     * 返回值: member 成员的新 score 值，以字符串形式表示。
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public double zincrby(String key, double score, String member) {
        double result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zincrby( key,  score,  member);
            } else {
                result = getJedisCommands(groupName).zincrby( key,  score,  member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zincrby falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 为有序集 key 的成员 member 的 score 值加上增量 increment 。
     *
     * 可以通过传递一个负数值 increment ，让 score 减去相应的值，比如 ZINCRBY key -5 member ，就是让
     * member 的 score 值减去 5 。 当 key 不存在，或 member 不是 key 的成员时， ZINCRBY key
     * increment member 等同于 ZADD key increment member 。 当 key 不是有序集类型时，返回一个错误。
     * score 值可以是整数值或双精度浮点数。
     *
     * 返回值: member 成员的新 score 值，以字符串形式表示。
     *
     * @param key
     * @param score
     * @param member
     * @return
     */
    public double zincrby(byte[] key, double score, byte[] member) {
        double result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zincrby( key,  score,  member);
            } else {
                result = getBinaryJedisCommands(groupName).zincrby( key,  score,  member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zincrby falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从小到大)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> zrange(String key, long start, long end) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrange( key,  start,  end);
            } else {
                result = getJedisCommands(groupName).zrange( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从小到大)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<byte[]> zrange(byte[] key, int start, int end) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrange( key,  start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).zrange( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从大到小)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<String> zrevrange(String key, long start, long end) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrange( key,  start,  end);
            } else {
                result = getJedisCommands(groupName).zrevrange( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从大到小)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<byte[]> zrevrange(byte[] key, int start, int end) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrevrange( key,  start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).zrevrange( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrange falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从大到小)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<Tuple> zrevrangeWithScores(String key, long start, long end) {
        Set<Tuple> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrangeWithScores( key,  start,  end);
            } else {
                result = getJedisCommands(groupName).zrevrangeWithScores( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeWithScores falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，指定区间内的成员。 其中成员的位置按 score 值递增(从大到小)来排序。 具有相同 score
     * 值的成员按字典序(lexicographical order )来排列。如果你需要成员按 score 值递减(从大到小)来排列，请使用
     * ZREVRANGE 命令。
     *
     * 下标参数 start 和 stop 都以 0 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。
     * 你也可以使用负数下标，以 -1 表示最后一个成员， -2 表示倒数第二个成员，以此类推。 超出范围的下标并不会引起错误。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public Set<Tuple> zrevrangeWithScores(byte[] key, int start, int end) {
        Set<Tuple> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrevrangeWithScores( key,  start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).zrevrangeWithScores( key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.getBinaryJedisClusterCommands falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score
     * 值递增(从小到大)次序排列。 具有相同 score 值的成员按字典序(lexicographical
     * order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param max
     * @param min
     * @return
     */
    public Set<String> zrangeByScore(String key, double min, double max) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrangeByScore( key,  min,  max);
            } else {
                result = getJedisCommands(groupName).zrangeByScore( key,  min,  max);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score
     * 值递增(从小到大)次序排列。 具有相同 score 值的成员按字典序(lexicographical
     * order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrangeByScore( key,  min,  max);
            } else {
                result = getBinaryJedisCommands(groupName).zrangeByScore( key,  min,  max);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score
     * 值递增(从小到大)次序排列。 具有相同 score 值的成员按字典序(lexicographical
     * order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrangeByScore( key,  min,  max,  offset,  count);
            } else {
                result = getJedisCommands(groupName).zrangeByScore( key,  min,  max,  offset,  count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。有序集成员按 score
     * 值递增(从小到大)次序排列。 具有相同 score 值的成员按字典序(lexicographical
     * order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     */
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max, int offset, int count) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrangeByScore( key,  min,  max,  offset,  count);
            } else {
                result = getBinaryJedisCommands(groupName).zrangeByScore( key,  min,  max,  offset,  count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 member的score 值介于 minmember 和 maxmember 的score 值之间(包括等于
     * minmember 或 maxmember 的score 值 )的成员。有序集成员按 score 值递增(从小到大)次序排列。 具有相同
     * score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param minmember
     * @param maxmember
     * @return
     */
    public Set<String> zrangeByScore(String key, String minmember, String maxmember) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrangeByScore( key,  minmember,maxmember);
            } else {
                result = getJedisCommands(groupName).zrangeByScore( key,  minmember,maxmember);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 member的score 值介于 minmember 和 maxmember 的score 值之间(包括等于
     * minmember 或 maxmember 的score 值 )的成员。有序集成员按 score 值递增(从小到大)次序排列。 具有相同
     * score 值的成员按字典序(lexicographical order)来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param minmember
     * @param maxmember
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrangeByScore(String key, String minmember, String maxmember, int offset, int count) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrangeByScore( key,  minmember,maxmember,offset,count);
            } else {
                result = getJedisCommands(groupName).zrangeByScore( key,  minmember,maxmember,offset,count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score
     * 值递减(从大到小)的次序排列。具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param max
     * @param min
     * @return
     */
    public Set<String> zrevrangeByScore(String key, double max, double min) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrangeByScore( key, max, min);
            } else {
                result = getJedisCommands(groupName).zrevrangeByScore( key, max, min);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score
     * 值递减(从大到小)的次序排列。具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @return
     */
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrevrangeByScore( key, max, min);
            } else {
                result = getBinaryJedisCommands(groupName).zrevrangeByScore( key, max, min);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score
     * 值递减(从大到小)的次序排列。具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrevrangeByScore(String key, double max, double min, int offset, int count) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrangeByScore( key,  max,  min,  offset, count);
            } else {
                result = getJedisCommands(groupName).zrevrangeByScore( key,  max,  min,  offset, count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中， score 值介于 max 和 min 之间(默认包括等于 max 或 min )的所有的成员。有序集成员按 score
     * 值递减(从大到小)的次序排列。具有相同 score 值的成员按字典序的逆序(reverse lexicographical order )排列。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param min
     * @param max
     * @param offset
     * @param count
     * @return
     */
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min, int offset, int count) {
        Set<byte[]> result = null;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrevrangeByScore( key,  max,  min,  offset, count);
            } else {
                result = getBinaryJedisCommands(groupName).zrevrangeByScore( key,  max,  min,  offset, count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 member的score 值介于 的score 值之间(包括等于 maxmember 和 minmember
     * 的score 值 )的成员。有序集成员按 score 值递增(从大到小)次序排列。 具有相同 score 值的成员按字典逆序(reverse
     * lexicographical order )来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param maxmember
     * @param minmember
     * @return
     */
    public Set<String> zrevrangeByScore(String key, String maxmember, String minmember) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrangeByScore(key, maxmember, minmember);
            } else {
                result = getJedisCommands(groupName).zrevrangeByScore(key, maxmember, minmember);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中，所有 member的score 值介于 的score 值之间(包括等于 maxmember 和 minmember
     * 的score 值 )的成员。有序集成员按 score 值递增(从大到小)次序排列。 具有相同 score 值的成员按字典逆序(reverse
     * lexicographical order )来排列(该属性是有序集提供的，不需要额外的计算)。
     *
     * 可选的 LIMIT 参数指定返回结果的数量及区间(就像SQL中的 SELECT LIMIT offset, count )，注意当 offset
     * 很大时，定位 offset 的操作可能需要遍历整个有序集，此过程最坏复杂度为 O(N) 时间。
     *
     * 返回值: 指定区间内，带有 score 值(可选)的有序集成员的列表。
     *
     * @param key
     * @param maxmember
     * @param minmember
     * @param offset
     * @param count
     * @return
     */
    public Set<String> zrevrangeByScore(String key, String maxmember, String minmember, int offset, int count) {
        Set<String> result = null;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrangeByScore(key, maxmember, minmember, offset, count);
            } else {
                result = getJedisCommands(groupName).zrevrangeByScore(key, maxmember, minmember, offset, count);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。 排名以 0 为底，也就是说，
     * score 值最小的成员排名为 0 。
     *
     * 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil
     * 。
     *
     * @param key
     * @param member
     * @return
     */
    public long zrank(String key, String member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrank(key, member);
            } else {
                result = getJedisCommands(groupName).zrank(key, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递增(从小到大)顺序排列。 排名以 0 为底，也就是说，
     * score 值最小的成员排名为 0 。
     *
     * 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil
     * 。
     *
     * @param key
     * @param member
     * @return
     */
    public long zrank(byte[] key, byte[] member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrank(key, member);
            } else {
                result = getBinaryJedisCommands(groupName).zrank(key, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。 排名以 0 为底，也就是说， score
     * 值最大的成员排名为 0 。
     *
     * 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil
     * 。
     *
     * @param key
     * @param member
     * @return
     */
    public long zrevrank(String key, String member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zrevrank(key, member);
            } else {
                result = getJedisCommands(groupName).zrevrank(key, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 返回有序集 key 中成员 member 的排名。其中有序集成员按 score 值递减(从大到小)排序。 排名以 0 为底，也就是说， score
     * 值最大的成员排名为 0 。
     *
     * 返回值: 如果 member 是有序集 key 的成员，返回 member 的排名。 如果 member 不是有序集 key 的成员，返回 nil
     * 。
     *
     * @param key
     * @param member
     * @return
     */
    public long zrevrank(byte[] key, byte[] member) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zrevrank(key, member);
            } else {
                result = getBinaryJedisCommands(groupName).zrevrank(key, member);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zrevrank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中，指定排名(rank)区间内的所有成员。
     *
     * 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。 下标参数 start 和 stop 都以 0
     * 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。 你也可以使用负数下标，以 -1 表示最后一个成员， -2
     * 表示倒数第二个成员，以此类推。
     *
     * 返回值: 被移除成员的数量。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public long zremrangeByRank(String key, long start, long end) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zremrangeByRank(key,  start,  end);
            } else {
                result = getJedisCommands(groupName).zremrangeByRank(key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zremrangeByRank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中，指定排名(rank)区间内的所有成员。
     *
     * 区间分别以下标参数 start 和 stop 指出，包含 start 和 stop 在内。 下标参数 start 和 stop 都以 0
     * 为底，也就是说，以 0 表示有序集第一个成员，以 1 表示有序集第二个成员，以此类推。 你也可以使用负数下标，以 -1 表示最后一个成员， -2
     * 表示倒数第二个成员，以此类推。
     *
     * 返回值: 被移除成员的数量。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public long zremrangeByRank(byte[] key, int start, int end) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zremrangeByRank(key,  start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).zremrangeByRank(key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zremrangeByRank falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
     *
     * 返回值: 被移除成员的数量。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public long zremrangeByScore(String key, double start, double end) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).zremrangeByScore(key,  start,  end);
            } else {
                result = getJedisCommands(groupName).zremrangeByScore(key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zremrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    /**
     * 移除有序集 key 中，所有 score 值介于 min 和 max 之间(包括等于 min 或 max )的成员。
     *
     * 返回值: 被移除成员的数量。
     *
     * @param key
     * @param start
     * @param end
     * @return
     */
    public long zremrangeByScore(byte[] key, double start, double end) {
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getBinaryJedisClusterCommands(groupName).zremrangeByScore(key,  start,  end);
            } else {
                result = getBinaryJedisCommands(groupName).zremrangeByScore(key,  start,  end);
            }
        }catch (Exception e){
            logger.error("RedisCluster.zremrangeByScore falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }


    public long pfAdd(String key,String... elements){
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).pfadd(key);
            } else {
                result = getJedisCommands(groupName).pfadd(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.pfadd falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }

    public long pfCount(String key){
        long result = -10000;
        try {
            if (isCluster(groupName)) {
                result = getJedisClusterCommands(groupName).pfcount(key);
            } else {
                result = getJedisCommands(groupName).pfcount(key);
            }
        }catch (Exception e){
            logger.error("RedisCluster.pfcount falid", e);
        }finally {
            getJedisProvider(groupName).release();
        }
        return result;
    }
}
