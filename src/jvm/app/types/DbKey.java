package app.types;

import static app.types.Utils.ByteBufferToInstant;
import static app.types.Utils.ExpDateToInt;
import static app.types.Utils.InstantToByteBuffer;
import static app.types.Utils.IntToExpDate;
import static app.types.Utils.QuoteDateTimeToInstant;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.apache.commons.collections4.map.LRUMap;
import org.apache.commons.lang3.StringUtils;

public final class DbKey {

  /// use joda time for instant
  public java.time.Instant quoteDateTime;
  /// "YYYY-MM-DD"
  public String expirationDate;
  /// as of year 2022, SPX all time high is around 4800
  public short strike;

  public DbKey() {}

  public static DbKey fromCsvLineTokens(String[] tokens) {
    DbKey k = new DbKey();
    // [0] is the underlying such as "^SPX", this depends on the directory of the Env
    // [1] is quote_datetime
    k.quoteDateTime = QuoteDateTimeToInstant(tokens[1]);
    // [2] is root, which is max 5 characters, this depends on the directory of the Env or the name of the DB
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
      k.strike = Short.parseShort(strike);
    }
    // [5] is the option type (P, C) which depends on the directory of the Env

    return k;
  }

  public UnsafeBuffer toBuffer() {
    final var bb = ByteBuffer.allocateDirect(4 + 4 + 2);
    final var buf = new UnsafeBuffer(bb);
    var i = 0;

    buf.putBytes(i, InstantToByteBuffer(quoteDateTime), 0, 4);
    i += 4;
    buf.putInt(i, ExpDateToInt(expirationDate), ByteOrder.BIG_ENDIAN);
    i += 4;
    buf.putShort(i, strike, ByteOrder.BIG_ENDIAN);
    i += 2;

    return buf;
  }

  public static DbKey fromBuffer(DirectBuffer buf) {
    final var k = new DbKey();
    var i = 0;

    k.quoteDateTime = ByteBufferToInstant(buf, i);
    i += 4;
    k.expirationDate = IntToExpDate(buf.getInt(i, ByteOrder.BIG_ENDIAN));
    i += 4;
    k.strike = buf.getShort(i, ByteOrder.BIG_ENDIAN);
    i += 2;

    return k;
  }
}
