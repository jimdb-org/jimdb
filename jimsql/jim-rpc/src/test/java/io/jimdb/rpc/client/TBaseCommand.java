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
package io.jimdb.rpc.client;

import io.jimdb.common.exception.CodecException;
import io.jimdb.rpc.client.command.Command;
import io.jimdb.rpc.client.command.CommandType;
import io.netty.buffer.ByteBuf;

/**
 * @version V1.0
 */
public class TBaseCommand implements Command {
  private short funcID = 0;
  private Long reqID = 0L;
  private String body;

  public TBaseCommand() {}

  public TBaseCommand(short funcID, Long reqID, String body) {
    this.funcID = funcID;
    this.reqID = reqID;
    this.body = body;
  }

  public short getFuncID() {
    return funcID;
  }

  public void setFuncID(short funcID) {
    this.funcID = funcID;
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
  public int getTimeout() {
    return 0;
  }

  @Override
  public CommandType getType() {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  public String getBody() {
    return body;
  }

  public void setBody(String body) {
    this.body = body;
  }

  @Override
  public void encode(ByteBuf buf) throws CodecException {
    TCodecUtil.encode(buf, this);
  }

  @Override
  public void decode(ByteBuf buf) throws CodecException {
    TCodecUtil.decode(buf, this);
  }
}
