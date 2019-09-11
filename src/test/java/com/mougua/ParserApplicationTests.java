package com.mougua;

import com.mougua.redis.rdb.RdbHashParser;
import com.mougua.redis.rdb.ZipList;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;


public class ParserApplicationTests {
    @Test
    public void contextLoads() {
    }

    @Ignore
    @Test
    public void testJedis() throws Exception {
        Jedis jedis = new Jedis("localhost");
        byte[] bytes = jedis.dump("bbcc");
        RdbHashParser parser = new RdbHashParser(bytes);
        ZipList list = parser.readZipList();

        System.out.println(list.getHashMap());
    }
}

