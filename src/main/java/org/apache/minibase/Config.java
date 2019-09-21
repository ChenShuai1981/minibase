package org.apache.minibase;

public class Config {

  private long maxMemstoreSize = 16 * 1024 * 1024;
  private long maxWalSize = 16 * 1024 * 1024;
  private int flushMaxRetries = 10;
  private String dataDir = "MiniBase";
  private String walDir = "MiniBase";
  private int maxDiskFiles = 10;
  private int maxThreadPoolSize = 5;

  private static final Config DEFAULT = new Config();

  public Config setMaxMemstoreSize(long maxMemstoreSize) {
    this.maxMemstoreSize = maxMemstoreSize;
    return this;
  }

  public long getMaxMemstoreSize() {
    return this.maxMemstoreSize;
  }

  public Config setMaxWalSize(long maxWalSize) {
    this.maxWalSize = maxWalSize;
    return this;
  }

  public long getMaxWalSize() {
    return this.maxWalSize;
  }

  public Config setFlushMaxRetries(int flushMaxRetries) {
    this.flushMaxRetries = flushMaxRetries;
    return this;
  }

  public int getFlushMaxRetries() {
    return this.flushMaxRetries;
  }

  public Config setWalDir(String walDir) {
    this.walDir = walDir;
    return this;
  }

  public String getWalDir() {
    return this.walDir;
  }

  public Config setDataDir(String dataDir) {
    this.dataDir = dataDir;
    return this;
  }

  public String getDataDir() {
    return this.dataDir;
  }

  public Config setMaxDiskFiles(int maxDiskFiles) {
    this.maxDiskFiles = maxDiskFiles;
    return this;
  }

  public int getMaxDiskFiles() {
    return this.maxDiskFiles;
  }

  public Config setMaxThreadPoolSize(int maxThreadPoolSize) {
    this.maxThreadPoolSize = maxThreadPoolSize;
    return this;
  }

  public int getMaxThreadPoolSize() {
    return this.maxThreadPoolSize;
  }

  public static Config getDefault() {
    return DEFAULT;
  }
}
