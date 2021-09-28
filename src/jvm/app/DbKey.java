package app;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
// import static org.hamcrest.CoreMatchers.is;
// import static org.hamcrest.CoreMatchers.notNullValue;
// import static org.hamcrest.CoreMatchers.startsWith;
// import static org.hamcrest.MatcherAssert.assertThat;
// import static org.junit.Assert.assertNotNull;
// import static org.junit.Assert.assertNull;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.DbiFlags.MDB_DUPSORT;
import static org.lmdbjava.DirectBufferProxy.PROXY_DB;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.GetOp.MDB_SET;
import static org.lmdbjava.SeekOp.MDB_FIRST;
import static org.lmdbjava.SeekOp.MDB_LAST;
import static org.lmdbjava.SeekOp.MDB_PREV;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class DbKey {
  /// unix timestamp, in seconds
  public long quoteTimestamp;
  /// must be a string of a length of 5
  public String root;

  // P or C
  public char optionType;

  // "YYYY-MM-DD"
  public String expirationDate;

  public int strike;

  private DbKey() {}

  public DbKey(
      long quoteTimestamp,
      String root,
      char optionType,
      String expirationDate,
      String strikeString) {
    this.quoteTimestamp = quoteTimestamp;
    this.root = root;
    this.optionType = optionType;
    this.expirationDate = expirationDate;
    this.strike = Integer.parseInt(strikeString);
  }

  public static int toDateStringInt(String s) {
    final var year = Integer.parseInt(s.substring(0, 4));
    final var month = Integer.parseInt(s.substring(5, 7));
    final var day = Integer.parseInt(s.substring(8, 10));
    return day + (100 * month) + (10000 * year);
  }

  public static String toDateString(int i) {
    final String day = String.valueOf(i % 100);
    i = i / 100;
    final String month = String.valueOf(i % 100);
    i = i / 100;
    final String year = String.valueOf(i);
    return year + "-" + month + "-" + day;
  }

  public UnsafeBuffer toBuffer() {
    final var bb = ByteBuffer.allocateDirect(8 + 5 + 1 + 4 + 4);
    final var buf = new UnsafeBuffer(bb);
    var i = 0;

    buf.putLong(i, quoteTimestamp, ByteOrder.BIG_ENDIAN);
    i += 8;
    buf.putStringWithoutLengthUtf8(i, root);
    i += 5;
    buf.putChar(i, optionType);
    i += 1;
    buf.putInt(i, toDateStringInt(expirationDate), ByteOrder.BIG_ENDIAN);
    i += 4;
    buf.putInt(i, strike, ByteOrder.BIG_ENDIAN);
    i += 4;

    return buf;
  }

  public static DbKey fromBuffer(DirectBuffer buf) {
    final var k = new DbKey();
    var i = 0;

    k.quoteTimestamp = buf.getLong(i, ByteOrder.BIG_ENDIAN);
    i += 8;
    k.root = buf.getStringWithoutLengthUtf8(i, 5);
    i += 5;
    k.optionType = buf.getChar(i);
    i += 1;
    k.expirationDate = toDateString(buf.getInt(i, ByteOrder.BIG_ENDIAN));
    i += 4;
    k.strike = buf.getInt(i, ByteOrder.BIG_ENDIAN);
    i += 4;

    return k;
  }
}
