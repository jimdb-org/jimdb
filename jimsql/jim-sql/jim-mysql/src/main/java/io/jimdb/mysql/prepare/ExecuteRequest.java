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
package io.jimdb.mysql.prepare;

import java.util.ArrayList;
import java.util.List;

import io.jimdb.mysql.util.CodecUtil;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.values.Value;
import io.netty.buffer.ByteBuf;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "WOC_WRITE_ONLY_COLLECTION_FIELD", "EI_EXPOSE_REP2" })
public final class ExecuteRequest {

  private final int stmtId;

  private final int flags;

  private final NullBitMap nullBitmap;

  private List<Value> paramValues;

  private List<SQLType> paramTypes;

  public ExecuteRequest(ByteBuf byteBuf) {
    int paramsNum = 0;
    stmtId = CodecUtil.readInt4(byteBuf);
    flags = CodecUtil.readInt1(byteBuf);
    //skip iteration-count
    CodecUtil.readInt4(byteBuf);
    nullBitmap = new NullBitMap(0, 0);
    for (int i = 0; i < nullBitmap.getBits().length; i++) {
      nullBitmap.getBits()[i] = CodecUtil.readInt1(byteBuf);
    }
    // bound param
    if (CodecUtil.readInt1(byteBuf) == 1) {
      getTypes(byteBuf, paramsNum);
    }

    getParams(byteBuf, paramsNum);
  }

  private void getTypes(ByteBuf byteBuf, final int paramsNum) {
    paramTypes = new ArrayList<>(paramsNum);
    for (int i = 0; i < paramsNum; i++) {
      Basepb.DataType dataType = Basepb.DataType.forNumber(CodecUtil.readInt1(byteBuf));
      SQLType.Builder builder = SQLType.newBuilder();
      builder.setType(dataType)
              .setUnsigned(CodecUtil.readInt1(byteBuf) > 0 ? true : false);
      paramTypes.add(builder.build());
    }
  }

  private void getParams(ByteBuf byteBuf, int paramsNum) {
    paramValues = new ArrayList<>(paramsNum);
    for (int i = 0; i < paramsNum; i++) {
      paramValues.add(CodecUtil.binaryRead(byteBuf, paramTypes.get(i)));
    }
  }

  public int getStmtId() {
    return stmtId;
  }

  public int getFlags() {
    return flags;
  }

  public NullBitMap getNullBitmap() {
    return nullBitmap;
  }

  public List<Value> getParamValues() {
    return paramValues;
  }

  public List<SQLType> getParamTypes() {
    return paramTypes;
  }
}
