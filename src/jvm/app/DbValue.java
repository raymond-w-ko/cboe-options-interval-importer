package app;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class DbValue {
  public int open;
  public int high;
  public int low;
  public int close;
  public int trade_volume;
  public int bid_size;
  public int bid;
  public int ask_size;
  public int ask;
  public int underlying_bid;
  public int underlying_ask;
  public int implied_underlying_price;
  public int active_underlying_price;
  public float implied_volatility;
  public float delta;
  public float gamma;
  public float theta;
  public float vega;
  public float rho;
  public int open_interest;
  int i;

  private DbValue() {
    this.i = 0;
  }

  public static int priceStringToInt(String s) {
    final var n = s.length();
    final char ch = s.charAt(n - 3);
    if (ch != '.') {
      throw new RuntimeException("third to last char must be a period");
    }
    final String a = s.substring(0, n - 3);
    final String b = s.substring(n - 2, n);
    return Integer.parseInt(a + b);
  }

  public static DbValue fromCsvLineTokens(String[] tokens) {
    var v = new DbValue();
    v.i = 0;

    v.open = priceStringToInt(tokens[6]);
    v.high = priceStringToInt(tokens[7]);
    v.low = priceStringToInt(tokens[8]);
    v.close = priceStringToInt(tokens[9]);

    v.trade_volume = Integer.parseInt(tokens[10]);
    v.bid_size = Integer.parseInt(tokens[11]);
    v.bid = priceStringToInt(tokens[12]);
    v.ask_size = Integer.parseInt(tokens[13]);
    v.ask = priceStringToInt(tokens[14]);

    v.underlying_bid = priceStringToInt(tokens[15]);
    v.underlying_ask = priceStringToInt(tokens[16]);

    v.implied_underlying_price = priceStringToInt(tokens[17]);
    v.active_underlying_price = priceStringToInt(tokens[18]);

    v.implied_volatility = Float.parseFloat(tokens[19]);
    v.delta = Float.parseFloat(tokens[20]);
    v.gamma = Float.parseFloat(tokens[21]);
    v.theta = Float.parseFloat(tokens[22]);
    v.vega = Float.parseFloat(tokens[23]);
    v.rho = Float.parseFloat(tokens[24]);

    v.open_interest = Integer.parseInt(tokens[25]);

    return v;
  }

  private void putInt(UnsafeBuffer buf, int x) {
    buf.putInt(this.i, x, ByteOrder.BIG_ENDIAN);
    this.i += 4;
  }

  private void putFloat(UnsafeBuffer buf, float x) {
    buf.putFloat(this.i, x, ByteOrder.BIG_ENDIAN);
    this.i += 4;
  }

  public UnsafeBuffer toBuffer() {
    final var bb = ByteBuffer.allocateDirect(20 * 4);
    final var buf = new UnsafeBuffer(bb);

    this.i = 0;

    this.putInt(buf, open);
    this.putInt(buf, high);
    this.putInt(buf, low);
    this.putInt(buf, close);

    this.putInt(buf, trade_volume);
    this.putInt(buf, bid_size);
    this.putInt(buf, bid);
    this.putInt(buf, ask_size);
    this.putInt(buf, ask);

    this.putInt(buf, underlying_bid);
    this.putInt(buf, underlying_ask);

    this.putInt(buf, implied_underlying_price);
    this.putInt(buf, active_underlying_price);

    this.putFloat(buf, implied_volatility);
    this.putFloat(buf, delta);
    this.putFloat(buf, gamma);
    this.putFloat(buf, theta);
    this.putFloat(buf, vega);
    this.putFloat(buf, rho);

    this.putInt(buf, open_interest);

    return buf;
  }

  private int getInt(DirectBuffer buf) {
    final var x = buf.getInt(this.i, ByteOrder.BIG_ENDIAN);
    this.i += 4;
    return x;
  }

  private float getFloat(DirectBuffer buf) {
    final var x = buf.getFloat(this.i, ByteOrder.BIG_ENDIAN);
    this.i += 4;
    return x;
  }

  public static DbValue fromBuffer(DirectBuffer buf) {
    final var v = new DbValue();
    v.i = 0;

    v.open = v.getInt(buf);
    v.high = v.getInt(buf);
    v.low = v.getInt(buf);
    v.close = v.getInt(buf);
    v.trade_volume = v.getInt(buf);
    v.bid_size = v.getInt(buf);
    v.bid = v.getInt(buf);
    v.ask_size = v.getInt(buf);
    v.ask = v.getInt(buf);
    v.underlying_bid = v.getInt(buf);
    v.underlying_ask = v.getInt(buf);
    v.implied_underlying_price = v.getInt(buf);
    v.active_underlying_price = v.getInt(buf);

    v.implied_volatility = v.getFloat(buf);
    v.delta = v.getFloat(buf);
    v.gamma = v.getFloat(buf);
    v.theta = v.getFloat(buf);
    v.vega = v.getFloat(buf);
    v.rho = v.getFloat(buf);

    v.open_interest = v.getInt(buf);

    return v;
  }
}
