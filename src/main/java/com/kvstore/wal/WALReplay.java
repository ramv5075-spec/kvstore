package com.kvstore.wal;

import java.io.*;
import java.nio.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.zip.CRC32;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WALReplay {

    private static final Logger log = LoggerFactory.getLogger(WALReplay.class);
    private static final int HEADER_SIZE = 8 + 1 + 4 + 4 + 8;
    private static final int CHECKSUM_SIZE = 8;

    public static List<WALEntry> replay(Path walPath) throws IOException {
        List<WALEntry> entries = new ArrayList<>();

        if (!Files.exists(walPath)) {
            log.info("No WAL file found at {}, starting fresh", walPath);
            return entries;
        }

        try (FileChannel ch = FileChannel.open(walPath, StandardOpenOption.READ)) {
            long fileSize = ch.size();
            int recovered = 0;
            int skipped = 0;

            while (ch.position() < fileSize) {
                long entryStart = ch.position();

                ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);
                int bytesRead = ch.read(header);
                if (bytesRead < HEADER_SIZE) break;
                header.flip();

                long seq        = header.getLong();
                byte opOrdinal  = header.get();
                int keyLen      = header.getInt();
                int valLen      = header.getInt();
                long timestamp  = header.getLong();

                if (keyLen < 0 || valLen < 0 || keyLen > 1_000_000 || valLen > 10_000_000) {
                    log.warn("Corrupt entry at position {}, stopping replay", entryStart);
                    break;
                }

                byte[] keyBytes = new byte[keyLen];
                byte[] valBytes = new byte[valLen];
                ByteBuffer payload = ByteBuffer.allocate(keyLen + valLen + CHECKSUM_SIZE);
                bytesRead = ch.read(payload);
                if (bytesRead < keyLen + valLen + CHECKSUM_SIZE) break;
                payload.flip();
                payload.get(keyBytes);
                payload.get(valBytes);
                long storedCrc = payload.getLong();

                CRC32 crc = new CRC32();
                crc.update(header.array());
                crc.update(keyBytes);
                crc.update(valBytes);

                if (crc.getValue() != storedCrc) {
                    log.warn("CRC mismatch at lsn={}, torn write detected, stopping replay", seq);
                    skipped++;
                    break;
                }

                WALEntry.OpType opType = WALEntry.OpType.values()[opOrdinal];
                entries.add(new WALEntry(seq, opType, new String(keyBytes),
                    valLen > 0 ? valBytes : null, timestamp));
                recovered++;
            }

            log.info("WAL replay complete: {} entries recovered, {} skipped", recovered, skipped);
        }

        return entries;
    }
}
