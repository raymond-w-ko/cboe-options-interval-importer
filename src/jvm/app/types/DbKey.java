package app.types;

import static app.types.Utils.ByteBufferToInstant;
import static app.types.Utils.ExpDateToInt;
import static app.types.Utils.InstantToByteBuffer;
import static app.types.Utils.IntToExpDate;
import static app.types.Utils.QuoteDateTimeToJodaInstant;

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

public final class DbKey {

  /// use joda time for instant
  public org.joda.time.Instant quoteDateTime;
  /// must be a string of a length of 5
  public String root;
  /// P or C
  public char optionType;
  /// "YYYY-MM-DD"
  public String expirationDate;
  public int strike;

  public DbKey() {}

  public static DbKey fromCsvLineTokens(String[] tokens) {
    DbKey k = new DbKey();
    // [0] is always "^SPX"

    // [1] is quote_datetime
    k.quoteDateTime = QuoteDateTimeToJodaInstant(tokens[1]);

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

  public UnsafeBuffer toBuffer() {
    final var bb = ByteBuffer.allocateDirect(4 + 5 + 1 + 4 + 4);
    final var buf = new UnsafeBuffer(bb);
    var i = 0;

    buf.putBytes(i, InstantToByteBuffer(quoteDateTime), 4, 4);
    i += 4;
    buf.putStringWithoutLengthUtf8(i, root);
    i += 5;
    buf.putByte(i, (byte) optionType);
    i += 1;
    buf.putInt(i, ExpDateToInt(expirationDate), ByteOrder.BIG_ENDIAN);
    i += 4;
    buf.putInt(i, strike, ByteOrder.BIG_ENDIAN);
    i += 4;

    return buf;
  }

  public static DbKey fromBuffer(DirectBuffer buf) {
    final var k = new DbKey();
    var i = 0;

    k.quoteDateTime = ByteBufferToInstant(buf, i);
    i += 4;
    k.root = buf.getStringWithoutLengthUtf8(i, 5);
    i += 5;
    k.optionType = (char) buf.getByte(i);
    i += 1;
    k.expirationDate = IntToExpDate(buf.getInt(i, ByteOrder.BIG_ENDIAN));
    i += 4;
    k.strike = buf.getInt(i, ByteOrder.BIG_ENDIAN);
    i += 4;

    return k;
  }
}
