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
package io.jimdb.engine.client;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.jimdb.common.exception.CodecException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.pb.Api;
import io.jimdb.pb.Function.FunctionID;
import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandType;
import io.netty.buffer.ByteBuf;

import com.google.protobuf.Message;
import com.google.protobuf.NettyOutput;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CHECKED")
public final class JimCommand implements Command {
  public static final short TYPE_REQUEST = 0x02;

  public static final short TYPE_RESPONSE = 0x12;

  public static final int MAGIC = 0x23232323;

  public static final short VERSION = 1;

  public static final int LENGTH_FIELD_OFFSET = 24;

  public static final int LENGTH_FIELD_SIZE = 4;

  private static final int HEADER_SIZE = 28;

  private static final Map<Short, Class> FUNC_ID_REQUEST = new ConcurrentHashMap<>();
  private static final Map<Short, Class> FUNC_ID_RESPONSE = new ConcurrentHashMap<>();

  static {
    FUNC_ID_REQUEST.put((short) FunctionID.kFuncRangeRequest_VALUE, Api.RangeRequest.class);
    FUNC_ID_RESPONSE.put((short) FunctionID.kFuncRangeRequest_VALUE, Api.RangeResponse.class);
  }

  private int magic;
  private short version;
  private short msgType;
  private Short funcId;
  private Long reqID;
  private byte streamHash;
  private byte protoType;
  private int timeout;
  private int bodyLen;
  private Object body;

  public JimCommand() {
  }

  public JimCommand(final short funcId, final Message body) {
    this.magic = MAGIC;
    this.version = VERSION;
    this.msgType = TYPE_REQUEST;
    this.funcId = funcId;
    this.protoType = 0;
    this.body = body;
    if (body != null) {
      this.bodyLen = body.getSerializedSize();
    }
  }

  @Override
  public int size() {
    int len = HEADER_SIZE;
    if (body != null) {
      len += this.bodyLen;
    }
    return len;
  }

  @Override
  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_HAS_CHECKED")
  public void encode(final ByteBuf buf) throws CodecException {
    try {
      buf.writeInt(this.magic);
      buf.writeShort(this.version);
      buf.writeShort(this.msgType);
      buf.writeShort(this.funcId);
      buf.writeLong(this.reqID);
      buf.writeByte(this.streamHash);
      buf.writeByte(this.protoType);
      buf.writeInt(this.timeout);
      buf.writeInt(this.bodyLen);

      if (body == null) {
        return;
      }

      switch (protoType) {
        case 0:
          if (body instanceof Message) {
            ((Message) body).writeTo(new NettyOutput(buf));
          } else {
            buf.writeBytes((byte[]) body);
          }
          break;

        default:
          throw CodecException.get(ErrorCode.ER_NOT_SUPPORTED_YET, null, "Protocol(" + String.valueOf(this.protoType) + ")");
      }
    } catch (CodecException ex) {
      throw ex;
    } catch (Exception ex) {
      throw CodecException.get(ErrorCode.ER_RPC_REQUEST_CODEC, ex);
    }
  }

  @SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "EXS_EXCEPTION_SOFTENING_HAS_CHECKED", "GC_UNRELATED_TYPES" })
  @Override
  public void decode(final ByteBuf buf) throws CodecException {
    try {
      this.magic = buf.readInt();
      // skip version
      buf.skipBytes(2);
      this.msgType = buf.readShort();
      this.funcId = buf.readShort();
      this.reqID = buf.readLong();
      // skip streamhash prototype time
      buf.skipBytes(2);
      this.timeout = buf.readInt();
      this.bodyLen = buf.readInt();

      Class clazz = null;
      switch (msgType) {
        case TYPE_REQUEST:
          clazz = FUNC_ID_REQUEST.get(funcId);
          break;

        case TYPE_RESPONSE:
          clazz = FUNC_ID_RESPONSE.get(funcId);
          break;

        default:
          clazz = null;
      }

      if (clazz == null) {
        return;
      }
      Method method = clazz.getMethod("parseFrom", ByteBuffer.class);
      this.body = method.invoke(clazz, buf.nioBuffer());
    } catch (Exception ex) {
      throw CodecException.get(ErrorCode.ER_RPC_REQUEST_CODEC, ex);
    }
  }

  @Override
  public Long getReqID() {
    return reqID;
  }

  @Override
  public void setReqID(Long reqID) {
    this.reqID = reqID;
  }

  @Override
  public CommandType getType() {
    return this.msgType == TYPE_REQUEST ? CommandType.REQUEST : CommandType.RESPONSE;
  }

  public boolean isResponse() {
    return this.msgType == TYPE_RESPONSE;
  }

  public int getMagic() {
    return magic;
  }

  public void setMagic(int magic) {
    this.magic = magic;
  }

  public short getVersion() {
    return version;
  }

  public void setVersion(short version) {
    this.version = version;
  }

  public short getMsgType() {
    return msgType;
  }

  public void setMsgType(short msgType) {
    this.msgType = msgType;
  }

  public Short getFuncId() {
    return funcId;
  }

  public void setFuncId(Short funcId) {
    this.funcId = funcId;
  }

  public byte getStreamHash() {
    return streamHash;
  }

  public void setStreamHash(byte streamHash) {
    this.streamHash = streamHash;
  }

  public byte getProtoType() {
    return protoType;
  }

  public void setProtoType(byte protoType) {
    this.protoType = protoType;
  }

  @Override
  public int getTimeout() {
    return timeout;
  }

  public void setTimeout(int timeout) {
    this.timeout = timeout;
  }

  public int getBodyLen() {
    return bodyLen;
  }

  public void setBodyLen(int bodyLen) {
    this.bodyLen = bodyLen;
  }

  public Object getBody() {
    return body;
  }

  public void setBody(Object body) {
    this.body = body;
  }
}
