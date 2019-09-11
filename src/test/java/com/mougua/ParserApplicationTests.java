package com.mougua;

import com.mougua.redis.rdb.RdbHashParser;
import com.mougua.redis.rdb.ZipList;
import org.junit.Ignore;
import org.junit.Test;
import redis.clients.jedis.Jedis;


public class ParserApplicationTests {
    private static String byte2hex(byte[] buffer) {
        String h = "";
        for (int i = 0; i < buffer.length; i++) {
            String temp = Integer.toHexString(buffer[i] & 0xFF);
            if (temp.length() == 1) {
                temp = "0" + temp;
            }
            h = h + " " + temp;
        }

        return h;
    }

    @Ignore
    @Test
    public void testJedis() throws Exception {
        Jedis jedis = new Jedis("localhost");
        byte[] bytes = jedis.dump("STAT_AGT:00000");
        System.out.println(byte2hex(bytes));
        RdbHashParser parser = new RdbHashParser(bytes);

        System.out.println(parser.read());
    }
}

