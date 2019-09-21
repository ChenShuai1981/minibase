package org.apache.minibase;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

public class DefaultWal implements MiniBase.Wal {

    private Config conf;
    private WalWriter walWriter;
    private final AtomicLong dataSize = new AtomicLong();

    public DefaultWal(Config config) throws IOException {
        this.conf = config;
        dataSize.set(0);
        String fileName = new File(conf.getWalDir(), "wal.log").toString();
        walWriter = new WalWriter(fileName);
    }

    @Override
    public void add(KeyValue kv) throws IOException {
        walWriter.append(kv);
        dataSize.addAndGet(kv.getSerializeSize());
        if (dataSize.get() > conf.getMaxWalSize()) {
            truncate();
        }
    }

    @Override
    public void truncate() throws IOException {
        walWriter.truncate();
        dataSize.set(0);
    }

    public static class WalWriter implements Closeable {

        private FileOutputStream out;

        public WalWriter(String fname) throws IOException {
            File f = new File(fname);
            f.createNewFile();
            out = new FileOutputStream(f, true);
        }

        public synchronized void append(KeyValue kv) throws IOException {
            out.write(kv.toBytes());
        }

        public synchronized void truncate() throws IOException {
            out.getChannel().truncate(0);
        }

        @Override
        public void close() throws IOException {
            if (out != null) {
                try {
                    out.flush();
                    FileDescriptor fd = out.getFD();
                    fd.sync();
                } finally {
                    out.close();
                }
            }
        }
    }
}
