package com.mougua.redis.rdb;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;

/**
 * @author 某瓜
 */
public class RdbHashParser {
    private final ByteBuffer buf;
    private static final Charset ASCII = Charset.forName("ASCII");

    public RdbHashParser(byte[] bytes) {
        buf = ByteBuffer.wrap(bytes);
    }

    private int readByte() throws IOException {
        return buf.get() & 0xff;
    }

    private int readSignedByte() throws IOException {
        return buf.get();
    }

    private byte[] readBytes(int numBytes) throws IOException {
        int rem = numBytes;
        int pos = 0;
        byte[] bs = new byte[numBytes];
        while (rem > 0) {
            int avail = buf.remaining();
            if (avail >= rem) {
                buf.get(bs, pos, rem);
                pos += rem;
                rem = 0;
            } else {
                buf.get(bs, pos, avail);
                pos += avail;
                rem -= avail;
            }
        }
        return bs;
    }

    private byte[] readStringEncoded() throws IOException {
        int firstByte = readByte();
        // the first two bits determine the encoding
        int flag = (firstByte & 0xc0) >> 6;
        int len;
        switch (flag) {
            case 0: // length is read from the lower 6 bits
                len = firstByte & 0x3f;
                return readBytes(len);
            case 1: // one additional byte is read for a 14 bit encoding
                len = ((firstByte & 0x3f) << 8) | (readByte() & 0xff);
                return readBytes(len);
            case 2: // read next four bytes as unsigned big-endian
                byte[] bs = readBytes(4);
                len = ((int) bs[0] & 0xff) << 24
                        | ((int) bs[1] & 0xff) << 16
                        | ((int) bs[2] & 0xff) << 8
                        | ((int) bs[3] & 0xff) << 0;
                if (len < 0) {
                    throw new IllegalStateException("Strings longer than " + Integer.MAX_VALUE
                            + "bytes are not supported.");
                }
                return readBytes(len);
            case 3:
                return readSpecialStringEncoded(firstByte & 0x3f);
            default: // never reached
                return null;
        }
    }

    private byte[] readSpecialStringEncoded(int type) throws IOException {
        switch (type) {
            case 0:
                return readInteger8Bits();
            case 1:
                return readInteger16Bits();
            case 2:
                return readInteger32Bits();
            case 3:
                return readLzfString();
            default:
                throw new IllegalStateException("Unknown special encoding: " + type);
        }
    }

    private long readLength() throws IOException {
        int firstByte = readByte();
        // The first two bits determine the encoding.
        int flag = (firstByte & 0xc0) >> 6;
        if (flag == 0) { // 00|XXXXXX: len is the last 6 bits of this byte.
            return firstByte & 0x3f;
        } else if (flag == 1) { // 01|XXXXXX: len is encoded on the next 14 bits.
            return (((long) firstByte & 0x3f) << 8) | ((long) readByte() & 0xff);
        } else if (firstByte == 0x80) {
            // 10|000000: len is a 32-bit integer encoded on the next 4 bytes.
            byte[] bs = readBytes(4);
            return ((long) bs[0] & 0xff) << 24
                    | ((long) bs[1] & 0xff) << 16
                    | ((long) bs[2] & 0xff) << 8
                    | ((long) bs[3] & 0xff) << 0;
        } else if (firstByte == 0x81) {
            // 10|000001: len is a 64-bit integer encoded on the next 8 bytes.
            byte[] bs = readBytes(8);
            return ((long) bs[0] & 0xff) << 56
                    | ((long) bs[1] & 0xff) << 48
                    | ((long) bs[2] & 0xff) << 40
                    | ((long) bs[3] & 0xff) << 32
                    | ((long) bs[4] & 0xff) << 24
                    | ((long) bs[5] & 0xff) << 16
                    | ((long) bs[6] & 0xff) << 8
                    | ((long) bs[7] & 0xff) << 0;
        } else {
            // 11|XXXXXX: special encoding.
            throw new IllegalStateException("Expected a length, but got a special string encoding.");
        }
    }

    private byte[] readInteger8Bits() throws IOException {
        return String.valueOf(readSignedByte()).getBytes(ASCII);
    }

    private byte[] readInteger16Bits() throws IOException {
        long val = ((long) readByte() & 0xff) << 0
                | (long) readSignedByte() << 8; // Don't apply 0xff mask to preserve sign.
        return String.valueOf(val).getBytes(ASCII);
    }

    private byte[] readInteger32Bits() throws IOException {
        byte[] bs = readBytes(4);
        long val = (long) bs[3] << 24 // Don't apply 0xff mask to preserve sign.
                | ((long) bs[2] & 0xff) << 16
                | ((long) bs[1] & 0xff) << 8
                | ((long) bs[0] & 0xff) << 0;
        return String.valueOf(val).getBytes(ASCII);
    }

    private byte[] readLzfString() throws IOException {
        int clen = (int) readLength();
        int ulen = (int) readLength();
        byte[] src = readBytes(clen);
        byte[] dest = new byte[ulen];
        Lzf.expand(src, dest);
        return dest;
    }

    private ZipList readZipList() throws IOException {
        return new ZipList(readStringEncoded());
    }

    private HashMap<String, String> readHash() throws IOException {
        long len = readLength();
        if (len > (Integer.MAX_VALUE / 2)) {
            throw new IllegalArgumentException("Hashes with more than " + (Integer.MAX_VALUE / 2)
                    + " elements are not supported.");
        }
        int size = (int) len;
        HashMap<String, String> map = new HashMap<>();
        String key, value;
        for (int i = 0; i < size; ++i) {
            key = new String(readStringEncoded(), ASCII);
            value = new String(readStringEncoded(), ASCII);
            map.put(key, value);
        }
        return map;
    }

    public HashMap<String, String> read() throws IOException {
        HashMap<String, String> map;
        switch (readByte()) {
            case 4:
                map = readHash();
                break;
            case 13:
                ZipList list = readZipList();
                map = list.getHashMap();
                break;
            default:
                map = null;
        }
        return map;
    }
}
