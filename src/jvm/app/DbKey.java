package app;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.collections4.map.LRUMap;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.apache.commons.lang3.StringUtils;

public final class DbKey {
  private static LRUMap<String, Long> timestampMsLookupMap = new LRUMap<String, Long>(1024 * 1024);
  private static DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

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

  static long toTimestampMs(String s) {
    if (timestampMsLookupMap.containsKey(s)) {
      return timestampMsLookupMap.get(s);
    }

    final long t = Instant.parse(s, dateTimeFormatter).getMillis();
    timestampMsLookupMap.put(s, t);
    return t;
  }

  public static DbKey fromCsvLineTokens(String[] tokens) {
    DbKey k = new DbKey();
    // [0] is always "^SPX"

    // [1] is quote_datetime
    k.quoteTimestamp = toTimestampMs(tokens[1]);

    // [2] is root
    {
      var root = tokens[2];
      final var n = root.length();
      if (n < 5) {
        var numSpaces = 5 - n;
        for (int i = 0; i < numSpaces; ++i) {
          root = root + " ";
        }
      }
      k.root = root;
    }

    // [3] is expiration date
    k.expirationDate = tokens[3];

    // [4] is strike
    {
      var strike = tokens[4];
      final var n = strike.length();
      final var suffix = strike.substring(n - 4, n);
      if (suffix.equals(".000")) {
        strike = strike.substring(0, n - 4);
      } else {
        throw new RuntimeException("strike suffix must be '.000', got input: " + strike);
      }
      k.strike = Integer.parseInt(strike);
    }

    // [5] is optionType
    char optionType = tokens[5].charAt(0);
    k.optionType = optionType;

    return k;
  }

  public static int toDateStringInt(String s) {
    final var year = Integer.parseInt(s.substring(0, 4));
    final var month = Integer.parseInt(s.substring(5, 7));
    final var day = Integer.parseInt(s.substring(8, 10));
    return day + (100 * month) + (10000 * year);
  }

  public static String toDateString(int i) {
    final String day = StringUtils.leftPad(String.valueOf(i % 100), 2);
    i = i / 100;
    final String month = StringUtils.leftPad(String.valueOf(i % 100), 2);
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
    buf.putByte(i, (byte)optionType);
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
    k.optionType = (char)buf.getByte(i);
    i += 1;
    k.expirationDate = toDateString(buf.getInt(i, ByteOrder.BIG_ENDIAN));
    i += 4;
    k.strike = buf.getInt(i, ByteOrder.BIG_ENDIAN);
    i += 4;

    return k;
  }
}
