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

import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.ResultType;
import io.jimdb.mysql.util.RandomGenerator;

import com.google.common.primitives.Bytes;

/**
 * @version V1.0
 */
public final class HandshakeResult extends ExecResult {
  private final int connID;

  public final byte[] authData1;

  public final byte[] authData2;

  public HandshakeResult(final int connID) {
    this.connID = connID;
    this.authData1 = RandomGenerator.generate(8);
    this.authData2 = RandomGenerator.generate(12);
  }

  @Override
  public ResultType getType() {
    return ResultType.HANDSHAKE;
  }

  public int getConnID() {
    return connID;
  }

  public byte[] getAuthData() {
    return Bytes.concat(authData1, authData2);
  }
}
