/*
 * Copyright 2019 The JimDB Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package io.jimdb.engine.client;

import static io.netty.util.internal.ObjectUtil.checkPositive;
import static io.netty.util.internal.ObjectUtil.checkPositiveOrZero;

import java.nio.ByteOrder;
import java.util.List;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.TooLongFrameException;
import io.netty.handler.codec.serialization.ObjectDecoder;

/**
 * @version V1.0
 */
public final class JimCommandDecoder extends ByteToMessageDecoder {
  private final ByteOrder byteOrder;
  private final int maxFrameLength;
  private final int lengthFieldOffset;
  private final int lengthFieldLength;
  private final int lengthFieldEndOffset;
  private final int lengthAdjustment;
  private final int initialBytesToStrip;
  private final boolean failFast;
  private boolean discardingTooLongFrame;
  private long tooLongFrameLength;
  private long bytesToDiscard;

  /**
   * Creates a new instance.
   *
   * @param maxFrameLength    the maximum length of the frame.  If the length of the frame is
   *                          greater than this value, {@link TooLongFrameException} will be thrown.
   * @param lengthFieldOffset the offset of the length field
   * @param lengthFieldLength the length of the length field
   */
  public JimCommandDecoder(int maxFrameLength, int lengthFieldOffset, int lengthFieldLength) {
    checkPositive(maxFrameLength, "maxFrameLength");
    checkPositiveOrZero(lengthFieldOffset, "lengthFieldOffset");

    if (lengthFieldOffset > maxFrameLength - lengthFieldLength) {
      throw new IllegalArgumentException("maxFrameLength (" + maxFrameLength + ") " + "must be equal to or greater than "
              + "lengthFieldOffset (" + lengthFieldOffset + ") + " + "lengthFieldLength (" + lengthFieldLength + ").");
    }

    this.byteOrder = ByteOrder.BIG_ENDIAN;
    this.maxFrameLength = maxFrameLength;
    this.lengthFieldOffset = lengthFieldOffset;
    this.lengthFieldLength = lengthFieldLength;
    this.lengthAdjustment = 0;
    lengthFieldEndOffset = lengthFieldOffset + lengthFieldLength;
    this.initialBytesToStrip = 0;
    this.failFast = true;
  }

  @Override
  protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
    while (true) {
      final Object decoded = decode(ctx, in);
      if (decoded == null) {
        return;
      }

      out.add(decoded);
    }
  }

  private void discardingTooLongFrame(ByteBuf in) {
    long bytesToDiscard = this.bytesToDiscard;
    int localBytesToDiscard = (int) Math.min(bytesToDiscard, in.readableBytes());
    in.skipBytes(localBytesToDiscard);
    bytesToDiscard -= localBytesToDiscard;
    this.bytesToDiscard = bytesToDiscard;

    failIfNecessary(false);
  }

  private static void failOnNegativeLengthField(ByteBuf in, long frameLength, int lengthFieldEndOffset) {
    in.skipBytes(lengthFieldEndOffset);
    throw new CorruptedFrameException("negative pre-adjustment length field: " + frameLength);
  }

  private static void failOnFrameLengthLessThanLengthFieldEndOffset(ByteBuf in, long frameLength, int lengthFieldEndOffset) {
    in.skipBytes(lengthFieldEndOffset);
    throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less "
            + "than lengthFieldEndOffset: " + lengthFieldEndOffset);
  }

  private void exceededFrameLength(ByteBuf in, long frameLength) {
    long discard = frameLength - in.readableBytes();
    tooLongFrameLength = frameLength;

    if (discard < 0) {
      // buffer contains more bytes then the frameLength so we can discard all now
      in.skipBytes((int) frameLength);
    } else {
      // Enter the discard mode and discard everything received so far.
      discardingTooLongFrame = true;
      bytesToDiscard = discard;
      in.skipBytes(in.readableBytes());
    }
    failIfNecessary(true);
  }

  private static void failOnFrameLengthLessThanInitialBytesToStrip(ByteBuf in, long frameLength, int initialBytesToStrip) {
    in.skipBytes((int) frameLength);
    throw new CorruptedFrameException("Adjusted frame length (" + frameLength + ") is less " + "than initialBytesToStrip: " + initialBytesToStrip);
  }

  /**
   * Create a frame out of the {@link ByteBuf} and return it.
   *
   * @param ctx the {@link ChannelHandlerContext} which this {@link ByteToMessageDecoder} belongs to
   * @param in  the {@link ByteBuf} from which to read data
   * @return frame           the {@link ByteBuf} which represent the frame or {@code null} if no frame could
   * be created.
   */
  protected Object decode(ChannelHandlerContext ctx, ByteBuf in) {
    if (discardingTooLongFrame) {
      discardingTooLongFrame(in);
    }

    if (in.readableBytes() < lengthFieldEndOffset) {
      return null;
    }

    int actualLengthFieldOffset = in.readerIndex() + lengthFieldOffset;
    long frameLength = getUnadjustedFrameLength(in, actualLengthFieldOffset, lengthFieldLength, byteOrder);

    if (frameLength < 0) {
      failOnNegativeLengthField(in, frameLength, lengthFieldEndOffset);
    }

    frameLength += lengthAdjustment + lengthFieldEndOffset;

    if (frameLength < lengthFieldEndOffset) {
      failOnFrameLengthLessThanLengthFieldEndOffset(in, frameLength, lengthFieldEndOffset);
    }

    if (frameLength > maxFrameLength) {
      exceededFrameLength(in, frameLength);
      return null;
    }

    // never overflows because it's less than maxFrameLength
    int frameLengthInt = (int) frameLength;
    if (in.readableBytes() < frameLengthInt) {
      return null;
    }

    if (initialBytesToStrip > frameLengthInt) {
      failOnFrameLengthLessThanInitialBytesToStrip(in, frameLength, initialBytesToStrip);
    }
    in.skipBytes(initialBytesToStrip);

    // extract frame
    int readerIndex = in.readerIndex();
    int actualFrameLength = frameLengthInt - initialBytesToStrip;
    ByteBuf frame = extractFrame(ctx, in, readerIndex, actualFrameLength);
    in.readerIndex(readerIndex + actualFrameLength);
    return frame;
  }

  /**
   * Decodes the specified region of the buffer into an unadjusted frame length.  The default implementation is
   * capable of decoding the specified region into an unsigned 8/16/24/32/64 bit integer.  Override this method to
   * decode the length field encoded differently.  Note that this method must not modify the state of the specified
   * buffer (e.g. {@code readerIndex}, {@code writerIndex}, and the content of the buffer.)
   *
   * @throws DecoderException if failed to decode the specified region
   */
  protected long getUnadjustedFrameLength(ByteBuf buf, int offset, int length, ByteOrder order) {
    long frameLength;
    switch (length) {
      case 1:
        frameLength = buf.getUnsignedByte(offset);
        break;
      case 2:
        frameLength = byteOrder == ByteOrder.BIG_ENDIAN ? buf.getUnsignedShort(offset) : buf.getUnsignedShortLE(offset);
        break;
      case 3:
        frameLength = byteOrder == ByteOrder.BIG_ENDIAN ? buf.getUnsignedMedium(offset) : buf.getUnsignedMediumLE(offset);
        break;
      case 4:
        frameLength = byteOrder == ByteOrder.BIG_ENDIAN ? buf.getUnsignedInt(offset) : buf.getUnsignedIntLE(offset);
        break;
      case 8:
        frameLength = byteOrder == ByteOrder.BIG_ENDIAN ? buf.getLong(offset) : buf.getLongLE(offset);
        break;
      default:
        throw new DecoderException("unsupported lengthFieldLength: " + lengthFieldLength + " (expected: 1, 2, 3, 4, or 8)");
    }
    return frameLength;
  }

  private void failIfNecessary(boolean firstDetectionOfTooLongFrame) {
    if (bytesToDiscard == 0) {
      // Reset to the initial state and tell the handlers that
      // the frame was too large.
      long tooLongFrameLength = this.tooLongFrameLength;
      this.tooLongFrameLength = 0;
      discardingTooLongFrame = false;
      if (!failFast || firstDetectionOfTooLongFrame) {
        fail(tooLongFrameLength);
      }
    } else {
      // Keep discarding and notify handlers if necessary.
      if (failFast && firstDetectionOfTooLongFrame) {
        fail(tooLongFrameLength);
      }
    }
  }

  /**
   * Extract the sub-region of the specified buffer.
   * <p>
   * If you are sure that the frame and its content are not accessed after
   * the current {@link #decode(ChannelHandlerContext, ByteBuf)}
   * call returns, you can even avoid memory copy by returning the sliced
   * sub-region (i.e. <tt>return buffer.slice(index, length)</tt>).
   * It's often useful when you convert the extracted frame into an object.
   * Refer to the source code of {@link ObjectDecoder} to see how this method
   * is overridden to avoid memory copy.
   */
  protected ByteBuf extractFrame(ChannelHandlerContext ctx, ByteBuf buffer, int index, int length) {
    return buffer.retainedSlice(index, length);
  }

  private void fail(long frameLength) {
    if (frameLength > 0) {
      throw new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + ": " + frameLength + " - discarded");
    } else {
      throw new TooLongFrameException("Adjusted frame length exceeds " + maxFrameLength + " - discarding");
    }
  }
}
