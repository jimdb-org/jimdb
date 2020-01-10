/*
 * Copyright 2019 The JIMDB Authors.
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
package com.google.protobuf;

import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_SURROGATE;
import static java.lang.Character.isSurrogatePair;
import static java.lang.Character.toCodePoint;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
public final class NettyOutput extends CodedOutputStream {
  private final ByteBuf buf;

  public NettyOutput(ByteBuf buf) {
    this.buf = buf;
  }

  @Override
  public void writeTag(final int fieldNumber, final int wireType) throws IOException {
    writeUInt32NoTag(WireFormat.makeTag(fieldNumber, wireType));
  }

  @Override
  public void writeInt32(final int fieldNumber, final int value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeInt32NoTag(value);
  }

  @Override
  public void writeUInt32(final int fieldNumber, final int value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeUInt32NoTag(value);
  }

  @Override
  public void writeFixed32(final int fieldNumber, final int value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED32);
    writeFixed32NoTag(value);
  }

  @Override
  public void writeUInt64(final int fieldNumber, final long value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    writeUInt64NoTag(value);
  }

  @Override
  public void writeFixed64(final int fieldNumber, final long value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_FIXED64);
    writeFixed64NoTag(value);
  }

  @Override
  public void writeBool(final int fieldNumber, final boolean value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_VARINT);
    write((byte) (value ? 1 : 0));
  }

  @Override
  public void writeString(final int fieldNumber, final String value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeStringNoTag(value);
  }

  @Override
  public void writeBytes(final int fieldNumber, final ByteString value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeBytesNoTag(value);
  }

  @Override
  public void writeByteArray(final int fieldNumber, final byte[] value) throws IOException {
    writeByteArray(fieldNumber, value, 0, value.length);
  }

  @Override
  public void writeByteArray(int fieldNumber, byte[] value, int offset, int length) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeByteArrayNoTag(value, offset, length);
  }

  @Override
  public void writeByteBuffer(final int fieldNumber, final ByteBuffer value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeUInt32NoTag(value.capacity());
    writeRawBytes(value);
  }

  @Override
  public void writeMessage(final int fieldNumber, final MessageLite value) throws IOException {
    writeTag(fieldNumber, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    writeMessageNoTag(value);
  }

  @Override
  public void writeMessageSetExtension(final int fieldNumber, final MessageLite value) throws IOException {
    writeTag(WireFormat.MESSAGE_SET_ITEM, WireFormat.WIRETYPE_START_GROUP);
    writeUInt32(WireFormat.MESSAGE_SET_TYPE_ID, fieldNumber);
    writeMessage(WireFormat.MESSAGE_SET_MESSAGE, value);
    writeTag(WireFormat.MESSAGE_SET_ITEM, WireFormat.WIRETYPE_END_GROUP);
  }

  @Override
  public void writeRawMessageSetExtension(final int fieldNumber, final ByteString value) throws IOException {
    writeTag(WireFormat.MESSAGE_SET_ITEM, WireFormat.WIRETYPE_START_GROUP);
    writeUInt32(WireFormat.MESSAGE_SET_TYPE_ID, fieldNumber);
    writeBytes(WireFormat.MESSAGE_SET_MESSAGE, value);
    writeTag(WireFormat.MESSAGE_SET_ITEM, WireFormat.WIRETYPE_END_GROUP);
  }

  @Override
  public void writeMessageNoTag(final MessageLite value) throws IOException {
    writeUInt32NoTag(value.getSerializedSize());
    value.writeTo(this);
  }

  @Override
  public void write(byte value) throws IOException {
    try {
      buf.writeByte(value);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeBytesNoTag(final ByteString value) throws IOException {
    writeUInt32NoTag(value.size());
    value.writeTo(this);
  }

  @Override
  public void writeByteArrayNoTag(final byte[] value, int offset, int length) throws IOException {
    writeUInt32NoTag(length);
    write(value, offset, length);
  }

  @Override
  public void writeRawBytes(final ByteBuffer value) throws IOException {
    if (value.hasArray()) {
      write(value.array(), value.arrayOffset(), value.capacity());
    } else {
      ByteBuffer duplicated = value.duplicate();
      duplicated.clear();
      write(duplicated);
    }
  }

  @Override
  public void writeInt32NoTag(int value) throws IOException {
    if (value >= 0) {
      writeUInt32NoTag(value);
    } else {
      writeUInt64NoTag(value);
    }
  }

  @Override
  public void writeUInt32NoTag(int value) throws IOException {
    try {
      while (true) {
        if ((value & ~0x7F) == 0) {
          buf.writeByte((byte) value);
          return;
        } else {
          buf.writeByte((byte) ((value & 0x7F) | 0x80));
          value >>>= 7;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeFixed32NoTag(int value) throws IOException {
    try {
      buf.writeIntLE(value);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeUInt64NoTag(long value) throws IOException {
    try {
      while (true) {
        if ((value & ~0x7FL) == 0) {
          buf.writeByte((byte) value);
          return;
        } else {
          buf.writeByte((byte) (((int) value & 0x7F) | 0x80));
          value >>>= 7;
        }
      }
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeFixed64NoTag(long value) throws IOException {
    try {
      buf.writeLongLE(value);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void write(byte[] value, int offset, int length) throws IOException {
    try {
      buf.writeBytes(value, offset, length);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeLazy(byte[] value, int offset, int length) throws IOException {
    write(value, offset, length);
  }

  @Override
  public void write(ByteBuffer value) throws IOException {
    try {
      buf.writeBytes(value);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void writeLazy(ByteBuffer value) throws IOException {
    write(value);
  }

  @Override
  public void writeStringNoTag(String value) throws IOException {
    final int markIndex = buf.writerIndex();
    try {
      final int maxEncodedSize = value.length() * Utf8.MAX_BYTES_PER_CHAR;
      final int maxLengthVarIntSize = computeUInt32SizeNoTag(maxEncodedSize);
      final int minLengthVarIntSize = computeUInt32SizeNoTag(value.length());
      if (minLengthVarIntSize == maxLengthVarIntSize) {
        final int startBytes = buf.writerIndex() + minLengthVarIntSize;
        buf.ensureWritable(startBytes + value.length());
        buf.writerIndex(startBytes);
        encode(value);
        int endBytes = buf.writerIndex();
        buf.writerIndex(markIndex);
        writeUInt32NoTag(endBytes - startBytes);
        buf.writerIndex(endBytes);
      } else {
        final int length = Utf8.encodedLength(value);
        writeUInt32NoTag(length);
        encode(value);
      }
    } catch (Utf8.UnpairedSurrogateException e) {
      buf.writerIndex(markIndex);
      inefficientWriteStringNoTag(value, e);
    } catch (IllegalArgumentException e) {
      throw new OutOfSpaceException(e);
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public int spaceLeft() {
    return buf.maxCapacity() - buf.writerIndex();
  }

  @Override
  public int getTotalBytesWritten() {
    return buf.readableBytes();
  }

  private void encode(String value) throws IOException {
    try {
      final int size = value.length();
      int rIdx = 0;
      for (char c; rIdx < size && (c = value.charAt(rIdx)) < 0x80; ++rIdx) {
        buf.writeByte((byte) c);
      }
      if (rIdx == size) {
        return;
      }

      for (char c; rIdx < size; ++rIdx) {
        c = value.charAt(rIdx);
        if (c < 0x80) {
          buf.writeByte((byte) c);
        } else if (c < 0x800) {
          buf.writeByte((byte) (0xC0 | (c >>> 6)));
          buf.writeByte((byte) (0x80 | (0x3F & c)));
        } else if (c < MIN_SURROGATE || MAX_SURROGATE < c) {
          buf.writeByte((byte) (0xE0 | (c >>> 12)));
          buf.writeByte((byte) (0x80 | (0x3F & (c >>> 6))));
          buf.writeByte((byte) (0x80 | (0x3F & c)));
        } else {
          final char low;
          if (rIdx + 1 == size || !isSurrogatePair(c, low = value.charAt(++rIdx))) {
            throw new Utf8.UnpairedSurrogateException(rIdx, size);
          }

          int codePoint = toCodePoint(c, low);
          buf.writeByte((byte) ((0xF << 4) | (codePoint >>> 18)));
          buf.writeByte((byte) (0x80 | (0x3F & (codePoint >>> 12))));
          buf.writeByte((byte) (0x80 | (0x3F & (codePoint >>> 6))));
          buf.writeByte((byte) (0x80 | (0x3F & codePoint)));
        }
      }
    } catch (IndexOutOfBoundsException e) {
      throw new OutOfSpaceException(e);
    }
  }
}
