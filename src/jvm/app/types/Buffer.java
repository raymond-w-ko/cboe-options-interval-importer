package app.types;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

class Buffer {

  /// used internally to keep track of buffer write offsets
  protected int i;

  public Buffer() {
    this.i = 0;
  }

  protected void putInt(UnsafeBuffer buf, int x) {
    buf.putInt(this.i, x, ByteOrder.BIG_ENDIAN);
    this.i += 4;
  }

  protected void putFloat(UnsafeBuffer buf, float x) {
    buf.putFloat(this.i, x, ByteOrder.BIG_ENDIAN);
    this.i += 4;
  }

  protected int getInt(DirectBuffer buf) {
    final var x = buf.getInt(this.i, ByteOrder.BIG_ENDIAN);
    this.i += 4;
    return x;
  }

  protected float getFloat(DirectBuffer buf) {
    final var x = buf.getFloat(this.i, ByteOrder.BIG_ENDIAN);
    this.i += 4;
    return x;
  }
}
