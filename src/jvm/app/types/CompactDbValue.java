package app.types;

import static app.types.Utils.ParsePriceString;

import app.types.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public final class CompactDbValue extends app.types.Buffer {

  // public float open;
  // public float high;
  // public float low;
  // public float close;
  public int trade_volume;
  // public int bid_size;
  public float bid;
  // public int ask_size;
  public float ask;
  // public float underlying_bid;
  // public float underlying_ask;
  // public float implied_underlying_price;
  // public float active_underlying_price;
  public float implied_volatility;
  public float delta;
  // public float gamma;
  // public float theta;
  // public float vega;
  // public float rho;
  public int open_interest;

  private CompactDbValue() {
    super();
  }

  public static CompactDbValue fromCsvLineTokens(String[] tokens) {
    var v = new CompactDbValue();
    v.i = 0;

    // v.open = ParsePriceString(tokens[6]);
    // v.high = ParsePriceString(tokens[7]);
    // v.low = ParsePriceString(tokens[8]);
    // v.close = ParsePriceString(tokens[9]);

    v.trade_volume = Integer.parseInt(tokens[10]);
    // v.bid_size = Integer.parseInt(tokens[11]);
    v.bid = ParsePriceString(tokens[12]);
    // v.ask_size = Integer.parseInt(tokens[13]);
    v.ask = ParsePriceString(tokens[14]);

    // v.underlying_bid = ParsePriceString(tokens[15]);
    // v.underlying_ask = ParsePriceString(tokens[16]);

    // v.implied_underlying_price = ParsePriceString(tokens[17]);
    // v.active_underlying_price = ParsePriceString(tokens[18]);

    v.implied_volatility = Float.parseFloat(tokens[19]);
    v.delta = Float.parseFloat(tokens[20]);
    // v.gamma = Float.parseFloat(tokens[21]);
    // v.theta = Float.parseFloat(tokens[22]);
    // v.vega = Float.parseFloat(tokens[23]);
    // v.rho = Float.parseFloat(tokens[24]);

    v.open_interest = Integer.parseInt(tokens[25]);

    return v;
  }

  public UnsafeBuffer toBuffer() {
    final var bb = ByteBuffer.allocateDirect(20 * 4);
    final var buf = new UnsafeBuffer(bb);

    this.i = 0;

    // this.putFloat(buf, open);
    // this.putFloat(buf, high);
    // this.putFloat(buf, low);
    // this.putFloat(buf, close);

    this.putInt(buf, trade_volume);
    // this.putInt(buf, bid_size);
    this.putFloat(buf, bid);
    // this.putInt(buf, ask_size);
    this.putFloat(buf, ask);

    // this.putFloat(buf, underlying_bid);
    // this.putFloat(buf, underlying_ask);

    // this.putFloat(buf, implied_underlying_price);
    // this.putFloat(buf, active_underlying_price);

    this.putFloat(buf, implied_volatility);
    this.putFloat(buf, delta);
    // this.putFloat(buf, gamma);
    // this.putFloat(buf, theta);
    // this.putFloat(buf, vega);
    // this.putFloat(buf, rho);

    this.putInt(buf, open_interest);

    return buf;
  }

  public static CompactDbValue fromBuffer(DirectBuffer buf) {
    final var v = new CompactDbValue();
    v.i = 0;

    // v.open = v.getFloat(buf);
    // v.high = v.getFloat(buf);
    // v.low = v.getFloat(buf);
    // v.close = v.getFloat(buf);

    v.trade_volume = v.getInt(buf);
    // v.bid_size = v.getInt(buf);
    v.bid = v.getFloat(buf);
    // v.ask_size = v.getInt(buf);
    v.ask = v.getFloat(buf);
    // v.underlying_bid = v.getFloat(buf);
    // v.underlying_ask = v.getFloat(buf);
    // v.implied_underlying_price = v.getFloat(buf);
    // v.active_underlying_price = v.getFloat(buf);

    v.implied_volatility = v.getFloat(buf);
    v.delta = v.getFloat(buf);
    // v.gamma = v.getFloat(buf);
    // v.theta = v.getFloat(buf);
    // v.vega = v.getFloat(buf);
    // v.rho = v.getFloat(buf);

    v.open_interest = v.getInt(buf);

    return v;
  }
}
