package com.kvstore.wal;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.zip.CRC32;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WAL implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(WAL.class);
    private static final int HEADER_SIZE = 8 + 1 + 4 + 4 + 8; // lsn+op+keyLen+valLen+timestamp
    private static final int CHECKSUM_SIZE = 8;

    private final FileChannel channel;
    private final AtomicLong lsn = new AtomicLong(0);

    public WAL(Path walPath) throws IOException {
        this.channel = FileChannel.open(walPath,
            StandardOpenOption.CREATE,
            StandardOpenOption.APPEND,
            StandardOpenOption.DSYNC);
        log.info("WAL opened at {}", walPath);
    }

    public long append(WALEntry.OpType opType, String key, byte[] value) throws IOException {
        long seq = lsn.incrementAndGet();
        byte[] keyBytes = key.getBytes();
        byte[] valBytes = value != null ? value : new byte[0];

        int totalSize = HEADER_SIZE + keyBytes.length + valBytes.length + CHECKSUM_SIZE;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);

        buf.putLong(seq);
        buf.put((byte) opType.ordinal());
        buf.putInt(keyBytes.length);
        buf.putInt(valBytes.length);
        buf.putLong(System.currentTimeMillis());
        buf.put(keyBytes);
        buf.put(valBytes);

        CRC32 crc = new CRC32();
        crc.update(buf.array(), 0, HEADER_SIZE + keyBytes.length + valBytes.length);
        buf.putLong(crc.getValue());

        buf.flip();
        channel.write(buf);

        log.debug("WAL append lsn={} op={} key={}", seq, opType, key);
        return seq;
    }

    public long getCurrentLsn() {
        return lsn.get();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        log.info("WAL closed");
    }
}
