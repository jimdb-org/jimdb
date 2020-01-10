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
package io.jimdb.mysql.handshake;

import java.util.HashMap;
import java.util.Map;

import io.jimdb.mysql.constant.CapabilityFlags;
import io.jimdb.mysql.constant.MySQLVersion;
import io.jimdb.mysql.util.CodecUtil;
import io.netty.buffer.ByteBuf;

/**
 * @version V1.0
 */
public final class HandshakeInfo {

  private final int capabilityFlags;

  private final int maxPacketSize;

  private final int characterSet;

  private final String username;

  private final byte[] authResponse;

  private final String authPluginName;

  private final String database;

  private final boolean isNewVersion;

  private Map<String, String> connectAttrs;

  public static final String CLIENT_PROTOCOL_41 = "client_protocol_41";

  public HandshakeInfo(ByteBuf byteBuf) {

    int cap = CodecUtil.getInt2(byteBuf, 1);
    if ((cap & CapabilityFlags.MYSQL_CLIENT_PROTOCOL_41.getValue()) > 0) {
      capabilityFlags = CodecUtil.readInt4(byteBuf);
      maxPacketSize = CodecUtil.readInt4(byteBuf);
      characterSet = CodecUtil.readInt1(byteBuf);
      CodecUtil.skipByteReserved(byteBuf, 23);
      username = CodecUtil.readStringWithNull(byteBuf);
      authResponse = readAuthResponse(byteBuf);
      database = readDatabase(byteBuf);
      // Custom authentication is not supported
      authPluginName = null;
      connectAttrs = readClientConnectAttrs(byteBuf);
      isNewVersion = true;
    } else {
      //int32
      capabilityFlags = CodecUtil.readInt2(byteBuf);
      maxPacketSize = CodecUtil.readInt3(byteBuf);
      username = CodecUtil.readStringWithNull(byteBuf);
      if ((capabilityFlags & CapabilityFlags.MYSQL_CLIENT_CONNECT_WITH_DB.getValue()) > 0) {
        authResponse = CodecUtil.readStringNullByBytes(byteBuf);
        database = CodecUtil.readStringWithNull(byteBuf);
      } else {
        authResponse = CodecUtil.readStringNullByBytes(byteBuf);
        database = null;
      }
      characterSet = MySQLVersion.CHARSET;
      authPluginName = null;
      connectAttrs = null;
      isNewVersion = false;
    }
  }

  private byte[] readAuthResponse(ByteBuf byteBuf) {
    if (0 != (capabilityFlags & CapabilityFlags.MYSQL_CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA.getValue())) {
      return CodecUtil.readEncodeStringByBytes(byteBuf);
    }
    if (0 != (capabilityFlags & CapabilityFlags.MYSQL_CLIENT_SECURE_CONNECTION.getValue())) {
      int length = CodecUtil.readInt1(byteBuf);
      return CodecUtil.readStringByBytes(byteBuf, length);
    }
    return CodecUtil.readStringNullByBytes(byteBuf);
  }

  private Map<String, String> readClientConnectAttrs(ByteBuf byteBuf) {
    if ((capabilityFlags & CapabilityFlags.MYSQL_CLIENT_CONNECT_ATTRS.getValue()) > 0) {
      if (byteBuf.readableBytes() == 0) {
        return null;
      }
      Map<String, String> attrs = new HashMap<String, String>();
      while (byteBuf.readableBytes() < 0) {
        String key = CodecUtil.readEncodeString(byteBuf);
        String value = CodecUtil.readEncodeString(byteBuf);
        attrs.put(key, value);
      }
      return attrs;
    }
    return null;
  }

  private String readDatabase(ByteBuf byteBuf) {
    return 0 != (capabilityFlags & CapabilityFlags.MYSQL_CLIENT_CONNECT_WITH_DB.getValue())
            ? CodecUtil.readStringWithNull(byteBuf) : null;
  }

  public boolean isProtocol41() {
    return isNewVersion;
  }


  public String getUsername() {
    return username;
  }

  public byte[] getAuthResponse() {
    return authResponse.clone();
  }

  public String getDatabase() {
    return database;
  }

  public int getCapabilityFlags() {
    return capabilityFlags;
  }

  public int getMaxPacketSize() {
    return maxPacketSize;
  }

  public int getCharacterSet() {
    return characterSet;
  }

  public String getAuthPluginName() {
    return authPluginName;
  }

  public boolean isNewVersion() {
    return isNewVersion;
  }

  public Map<String, String> getConnectAttrs() {
    return connectAttrs;
  }

}
