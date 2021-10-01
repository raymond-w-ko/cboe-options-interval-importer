package app.types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public final class Utils {
  private Utils() {}

  private static LRUMap<String, Instant> timestampMsLookupMap =
      new LRUMap<String, Instant>(1024 * 1024);
  private static DateTimeFormatter dateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

  public static Instant QuoteDateTimeToJodaInstant(String s) {
    if (timestampMsLookupMap.containsKey(s)) {
      return timestampMsLookupMap.get(s);
    }

    final var t = Instant.parse(s, dateTimeFormatter);
    timestampMsLookupMap.put(s, t);
    return t;
  }

  ////////////////////////////////////////////////////////////////////////////////

  public static int ExpDateToInt(String s) {
    final var year = Integer.parseInt(s.substring(0, 4));
    final var month = Integer.parseInt(s.substring(5, 7));
    final var day = Integer.parseInt(s.substring(8, 10));
    return day + (100 * month) + (10000 * year);
  }

  public static String IntToExpDate(int i) {
    final String day = StringUtils.leftPad(String.valueOf(i % 100), 2, "0");
    i = i / 100;
    final String month = StringUtils.leftPad(String.valueOf(i % 100), 2, "0");
    i = i / 100;
    final String year = String.valueOf(i);
    return year + "-" + month + "-" + day;
  }

  public static ByteBuffer InstantToByteBuffer(Instant t) {
    final long ms = t.getMillis() / 1000;
    final var buf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(ms);
    return buf;
  }

  public static Instant ByteBufferToInstant(DirectBuffer srcBuf, int i) {
    final var dstBuf = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
    srcBuf.getBytes(i, dstBuf, 4, 4);
    final long ms = dstBuf.getLong(0) * 1000;
    return new Instant(ms);
  }

  private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

  public static String DirectBufferToHexString(DirectBuffer src) {
    final byte[] bytes = new byte[src.capacity()];
    src.getBytes(0, bytes);
    char[] hexChars = new char[bytes.length * 2];
    for (int j = 0; j < bytes.length; j++) {
      int v = bytes[j] & 0xFF;
      hexChars[j * 2] = HEX_ARRAY[v >>> 4];
      hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
    }
    return new String(hexChars);
  }

  ////////////////////////////////////////////////////////////////////////////////

  public static float ParsePriceString(String s) {
    // final var n = s.length();
    // final char ch = s.charAt(n - 3);
    // if (ch != '.') {
    //   throw new RuntimeException("third to last char must be a period");
    // }
    // final String a = s.substring(0, n - 3);
    // final String b = s.substring(n - 2, n);
    // return Integer.parseInt(a + b);

    // final var f = Float.parseFloat(s) * 100;
    // return (int)Math.floor(f);
    
    return Float.parseFloat(s);
  }
}
