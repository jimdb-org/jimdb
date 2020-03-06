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
package io.jimdb.mysql;

import static io.jimdb.mysql.constant.MySQLVariables.MAX_EXECUTION_TIME;
import static io.jimdb.mysql.handshake.HandshakeInfo.CLIENT_PROTOCOL_41;

import java.util.Map;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.BaseException;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.context.PreparedContext;
import io.jimdb.core.context.PreparedStatement;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.PrepareResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.SQLExecutor;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.variable.SysVariable;
import io.jimdb.mysql.constant.MySQLColumnDataType;
import io.jimdb.mysql.constant.MySQLCommandType;
import io.jimdb.mysql.constant.MySQLVariables;
import io.jimdb.mysql.handshake.HandshakeInfo;
import io.jimdb.mysql.handshake.HandshakeResult;
import io.jimdb.mysql.util.CodecUtil;
import io.jimdb.pb.Metapb.SQLType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "HES_EXECUTOR_OVERWRITTEN_WITHOUT_SHUTDOWN", "HES_EXECUTOR_NEVER_SHUTDOWN", "CC_CYCLOMATIC_COMPLEXITY" })
public final class MySQLEngine implements SQLEngine {
  private static final String MYSQL_AUTH = "mysql_authorized";
  private static final String MYSQL_AUTH_DATA = "mysql_authorized_data";
  private static final int PAYLOAD_LENGTH = 3;
  private static final int SEQUENCE_LENGTH = 1;
  private static final int HEADER_LENGTH = PAYLOAD_LENGTH + SEQUENCE_LENGTH;

  private String host;
  private SQLExecutor sqlExecutor;
  private PrivilegeEngine privilegeEngine;

  @Override
  public void init(JimConfig conf) {
    this.host = conf.getServerConfig().getHost();
    this.privilegeEngine = PluginFactory.getPrivilegeEngine();
    int maxFrameSize = conf.getServerConfig().getFrameMaxSize();
    if (maxFrameSize > 0) {
      SysVariable var = MySQLVariables.getVariable(MySQLVariables.MAX_ALLOWED_PACKET);
      SysVariable newVar = new SysVariable(var.getScope(), var.getName(), String.valueOf(maxFrameSize));
      MySQLVariables.addVariable(MySQLVariables.MAX_ALLOWED_PACKET, newVar);
    }
  }

  @Override
  public void setSQLExecutor(final SQLExecutor executor) {
    Preconditions.checkNotNull(executor, "SQLExecutor cant be null.");
    this.sqlExecutor = executor;
  }

  @Override
  public DBType getType() {
    return DBType.MYSQL;
  }

  @Override
  public SysVariable getSysVariable(String name) {
    return MySQLVariables.getVariable(name);
  }

  @Override
  public Map<String, SysVariable> getAllVars() {
    return MySQLVariables.getAllVars();
  }

  @Override
  public short getServerStatusFlag(ServerStatus status) {
    switch (status) {
      case INTRANS:
        return 0x0001;
      case AUTOCOMMIT:
        return 0x0002;
      case MORERESULTSEXISTS:
        return 0x0008;
      case NOGOODINDEXUSED:
        return 0x0010;
      case NOINDEXUSED:
        return 0x0020;
      case CURSOREXISTS:
        return 0x0040;
      case LASTROWSEND:
        return 0x0080;
      case DBDROPPED:
        return 0x0100;
      case NOBACKSLASHESCAPED:
        return 0x0200;
      case METACHANGED:
        return 0x0400;
      case WASSLOW:
        return 0x0800;
      case OUTPARAMS:
        return 0x1000;
      default:
        return 0x0000;
    }
  }

  @Override
  public void handShake(Session session) {
    HandshakeResult result = new HandshakeResult(session.getConnID());
    session.putContext(MYSQL_AUTH_DATA, result.getAuthData());
    session.write(result, true);
  }

  //payloadLength + sequenceId + payload
  @Override
  public ByteBuf frameDecode(Session session, ByteBuf in) {
    int readableLength = in.readableBytes();
    if (readableLength <= HEADER_LENGTH) {
      return null;
    }

    int payloadLength = in.markReaderIndex().readMediumLE();
    int realLength = payloadLength + HEADER_LENGTH;
    if (readableLength < realLength) {
      in.resetReaderIndex();
      return null;
    }

    String varVal = session.getVarContext().getSessionVariable(MySQLVariables.MAX_ALLOWED_PACKET);
    if (StringUtils.isNotBlank(varVal)) {
      if (payloadLength > Integer.parseInt(varVal)) {
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NET_PACKET_TOO_LARGE);
      }
    }

    return in.readRetainedSlice(payloadLength + 1);
  }

  @Override
  public void writeResult(Session session, CompositeByteBuf out, ExecResult result) {
    try {
      switch (result.getType()) {
        case HANDSHAKE:
          CodecUtil.encode(session, out, (HandshakeResult) result);
          break;
        case QUERY:
          CodecUtil.encode(session, out, result);
          break;
        case DML:
          CodecUtil.encode(session, out, (DMLExecResult) result);
          break;
        case ACK:
          CodecUtil.encodeACK(session, out);
          break;
        case PREPARE:
          CodecUtil.encode(session, out, (PrepareResult) result);
          break;
        default:
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "ResultType(" + result.getType().name() + ")");
      }
    } catch (Exception ex) {
      BaseException je;
      if (ex instanceof BaseException) {
        je = (BaseException) ex;
      } else {
        je = DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex);
      }

      out.readerIndex(out.writerIndex());
      out.discardReadComponents();
      writeError(session, out, je);
    }
  }

  @Override
  public void writeError(Session session, CompositeByteBuf out, BaseException ex) {
    CodecUtil.encode(session, out, ex);
  }

  @Override
  public void close() {
  }

  @Override
  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CHECKED")
  public void handleCommand(Session session, ByteBuf in) {
    int stmtID = 0;
    String sql = null;
    final MySQLCommandType cmdType;

    try {
      int sequence = CodecUtil.readInt1(in);
      if (sequence != session.getSeqID()) {
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_SYSTEM_SEQ_INVALID, String.valueOf(sequence), String.valueOf(session.getSeqID()));
      }
      session.incrementAndGetSeqID();
      String timeoutVariable = session.getVarContext().getSessionVariable(MAX_EXECUTION_TIME);
      int timeout = Integer.parseInt(timeoutVariable);
      if (timeout > 0) {
        session.getStmtContext().setTimeout(SystemClock.currentTimeStamp().plusMillis(timeout));
      }

      Boolean isAuth = (Boolean) session.getContext(MYSQL_AUTH);
      if (isAuth == null || !isAuth.booleanValue()) {
        this.doAuth(session, in);
        return;
      }

      cmdType = MySQLCommandType.valueOf(CodecUtil.readInt1(in));
      switch (cmdType) {
        case MYSQL_COM_INIT_DB:
          sql = CodecUtil.readStringEof(in);
          break;
        case MYSQL_COM_QUERY:
          sql = CodecUtil.readStringEof(in);
          break;
        case MYSQL_COM_STMT_PREPARE:
          sql = CodecUtil.readStringEof(in);
          break;
        case MYSQL_COM_STMT_EXECUTE:
          stmtID = handleStmtExecute(session, in);
          break;
        case MYSQL_COM_STMT_RESET:
          if (in.readableBytes() < 4) {
            throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
          }
          stmtID = CodecUtil.readInt4(in);
          break;
        case MYSQL_COM_STMT_CLOSE:
          if (in.readableBytes() >= 4) {
            stmtID = CodecUtil.readInt4(in);
          }
          break;
        case MYSQL_COM_STMT_SEND_LONG_DATA:
          session.resetSeqID();
          handleStmtSendLongData(session, in);
          return;
        default:
          break;
      }
    } catch (BaseException ex) {
      session.writeError(ex);
      return;
    } catch (Exception ex) {
      session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex));
      return;
    } finally {
      in.release();
    }

    this.executeCommand(session, cmdType, sql, stmtID);
  }

  private void executeCommand(Session session, MySQLCommandType cmdType, String sql, int stmtID) {
    try {
      switch (cmdType) {
        case MYSQL_COM_INIT_DB:
          if (sql != null && !sql.startsWith("use")) {
            sql = "use " + sql;
          }
          sqlExecutor.executeQuery(session, sql);
          break;
        case MYSQL_COM_QUERY:
          sqlExecutor.executeQuery(session, sql);
          break;
        case MYSQL_COM_PING:
          session.write(AckExecResult.getInstance(), true);
          break;
        case MYSQL_COM_QUIT:
          break;
        case MYSQL_COM_STMT_PREPARE:
          sqlExecutor.createPrepare(session, sql);
          break;
        case MYSQL_COM_STMT_EXECUTE:
          sqlExecutor.executePrepare(session, stmtID);
          break;
        case MYSQL_COM_STMT_RESET:
          PreparedStatement stmt = session.getPreparedContext().getStatement(stmtID);
          if (stmt == null) {
            throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_STMT_HANDLER, String.valueOf(stmtID), "stmt_reset");
          }
          stmt.reset();
          session.write(AckExecResult.getInstance(), true);
          break;
        case MYSQL_COM_STMT_CLOSE:
          if (stmtID > 0) {
            PreparedStatement stmt1 = session.getPreparedContext().getStatement(stmtID);
            if (stmt1 != null) {
              stmt1.close();
            }
          }
          break;
        default:
          session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "Command(" + cmdType.name() + ")"));
          break;
      }
    } catch (BaseException ex) {
      session.writeError(ex);
    } catch (Exception ex) {
      session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex));
    }
  }

  private void doAuth(Session session, ByteBuf message) {
    HandshakeInfo handshakeInfo = new HandshakeInfo(message);
    byte[] authData = (byte[]) session.getContext(MYSQL_AUTH_DATA);
    session.setUserInfo(new UserInfo(handshakeInfo.getUsername(), session.getRemoteAddress().split(":")[0]));
    if (privilegeEngine.auth(session.getUserInfo(), handshakeInfo.getDatabase(), handshakeInfo.getAuthResponse(), authData)) {
      if (StringUtil.isNotBlank(handshakeInfo.getDatabase())) {
        session.getVarContext().setDefaultCatalog(handshakeInfo.getDatabase());
      }
      session.putContext(CLIENT_PROTOCOL_41, handshakeInfo.isProtocol41());
      session.removeContext(MYSQL_AUTH_DATA);
      session.putContext(MYSQL_AUTH, Boolean.TRUE);
      session.write(AckExecResult.getInstance(), true);
    } else {
      session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_ACCESS_DENIED_ERROR, handshakeInfo.getUsername(), this.host));
    }
  }

  private void handleStmtSendLongData(Session session, ByteBuf in) {
    if (in.readableBytes() < 6) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
    }

    int stmtId = CodecUtil.readInt4(in);
    PreparedStatement stmt = session.getPreparedContext().getStatement(stmtId);
    if (stmt == null) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_STMT_HANDLER, String.valueOf(stmtId), "stmt_send_longdata");
    }

    int paramId = CodecUtil.readInt2(in);
    Value[] boundValues = stmt.getBoundValues();
    if (paramId >= boundValues.length) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_WRONG_ARGUMENTS, "stmt_send_longdata");
    }

    int startPos = 0;
    int size = in.readableBytes();
    if (size == 0) {
      boundValues[paramId] = BinaryValue.EMPTY;
    } else {
      byte[] value = boundValues[paramId] == null ? null : boundValues[paramId].toByteArray();
      byte[] result;
      if (value != null && value.length > 0) {
        startPos = value.length;
        result = new byte[value.length + size];
        System.arraycopy(value, 0, result, 0, value.length);
      } else {
        result = new byte[size];
      }
      in.readBytes(result, startPos, size);
      boundValues[paramId] = BinaryValue.getInstance(result);
    }
  }

  private int handleStmtExecute(Session session, ByteBuf in) {
    if (in.readableBytes() < 9) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
    }

    int stmtId = CodecUtil.readInt4(in);
    PreparedContext context = session.getPreparedContext();
    PreparedStatement stmt = context.getStatement(stmtId);
    if (stmt == null) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_STMT_HANDLER, String.valueOf(stmtId), "stmt_execute");
    }

    // cursor flag
    int flag = CodecUtil.readInt1(in);
    switch (flag) {
      case 0:
        break;
      default:
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "flag " + flag);
    }
    // skip iteration-count
    in.skipBytes(4);

    int paramNum = stmt.getParams();
    if (paramNum <= 0) {
      return stmtId;
    }

    int nullBitLen = (paramNum + 7) >> 3;
    if (in.readableBytes() < (nullBitLen + 1)) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
    }

    ByteBuf nullBit = in.readSlice(nullBitLen);
    // new param bound
    if (CodecUtil.readInt1(in) == 1) {
      if (in.readableBytes() < (paramNum << 1)) {
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
      }

      SQLType[] paramTypes = new SQLType[paramNum];
      for (int i = 0; i < paramNum; i++) {
        MySQLColumnDataType mySQLType = MySQLColumnDataType.valueOf(CodecUtil.readInt1(in));
        paramTypes[i] = Types.buildSQLType(MySQLColumnDataType.valueOfType(mySQLType), CodecUtil.readInt1(in) > 0 ? true : false);
      }
      stmt.setParamTypes(paramTypes);
    }

    Value[] paramValues = parsePrepareParams(in, stmt, nullBit);
    stmt.reset();
    context.setParamTypes(stmt.getParamTypes());
    context.setParamValues(paramValues);
    return stmtId;
  }

  private Value[] parsePrepareParams(ByteBuf in, PreparedStatement stmt, ByteBuf nullBit) {
    int paramNum = stmt.getParams();
    Value[] boundValues = stmt.getBoundValues();
    Value[] paramValues = new Value[paramNum];
    SQLType[] paramTypes = stmt.getParamTypes();
    for (int i = 0; i < paramNum; i++) {
      if (boundValues[i] != null) {
        paramValues[i] = boundValues[i];
        continue;
      }

      if (((nullBit.getByte(i >> 3) & 0xff) & (1 << ((long) i) % 8)) > 0) {
        paramValues[i] = null;
        continue;
      }

      if (i >= paramTypes.length) {
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
      }
      paramValues[i] = CodecUtil.binaryRead(in, paramTypes[i]);
    }
    return paramValues;
  }
}
