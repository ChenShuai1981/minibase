package org.apache.minibase;

import static org.apache.minibase.DiskFile.BLOOM_FILTER_HASH_COUNT;

public class BloomFilter {
  private int k;
  private int bitsPerKey;
  private int bitLen;
  private byte[] result;

  public BloomFilter(int k, int bitsPerKey) {
    this.k = k;
    this.bitsPerKey = bitsPerKey;
  }

  public byte[] generate(byte[][] keys) {
    assert keys != null;
    bitLen = keys.length * bitsPerKey;
    bitLen = ((bitLen + 7) / 8) << 3; // align the bitLen.
    bitLen = bitLen < 64 ? 64 : bitLen;
    result = new byte[bitLen >> 3];
    for (int i = 0; i < keys.length; i++) {
      assert keys[i] != null;
      int h = Bytes.hash(keys[i]);
      for (int t = 0; t < k; t++) {
        int idx = (h % bitLen + bitLen) % bitLen;
        result[idx / 8] |= (1 << (idx % 8));
        int delta = (h >> 17) | (h << 15);
        h += delta;
      }
    }
    return result;
  }

  public boolean contains(byte[] key) {
    assert result != null;
    return contains(result, key);
  }

  public static boolean contains(byte[] bloomFilter, byte[] key) {
    int k = BLOOM_FILTER_HASH_COUNT;
    int bitLen = bloomFilter.length * 8;
    int h = Bytes.hash(key);
    for (int t = 0; t < k; t++) {
      int idx = (h % bitLen + bitLen) % bitLen;
      if ((bloomFilter[idx / 8] & (1 << (idx % 8))) == 0) {
        return false;
      }
      int delta = (h >> 17) | (h << 15);
      h += delta;
    }
    return true;
  }
}
