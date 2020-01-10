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
package io.jimdb.mysql;

import static io.jimdb.mysql.constant.MySQLVariables.MAX_EXECUTION_TIME;
import static io.jimdb.mysql.handshake.HandshakeInfo.CLIENT_PROTOCOL_41;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.context.PrepareContext;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.model.prepare.JimStatement;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.AckExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.PrepareResult;
import io.jimdb.mysql.constant.MySQLColumnDataType;
import io.jimdb.mysql.constant.MySQLCommandType;
import io.jimdb.mysql.constant.MySQLVariables;
import io.jimdb.mysql.handshake.HandshakeInfo;
import io.jimdb.mysql.handshake.HandshakeResult;
import io.jimdb.mysql.prepare.NullBitMap;
import io.jimdb.mysql.util.CodecUtil;
import io.jimdb.pb.Metapb.SQLType;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.SQLExecutor;
import io.jimdb.core.types.Types;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.common.utils.os.SystemClock;
import io.jimdb.core.values.Value;
import io.jimdb.core.variable.SysVariable;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "HES_EXECUTOR_OVERWRITTEN_WITHOUT_SHUTDOWN", "HES_EXECUTOR_NEVER_SHUTDOWN" })
public final class MySQLEngine implements SQLEngine {
  private static final Logger LOG = LoggerFactory.getLogger(MySQLEngine.class);

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

    byte seqID = session.getSeqID();
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
          stmtID = handleCommandExecute(session, in);
          break;
        case MYSQL_COM_STMT_CLOSE:
          stmtID = CodecUtil.readInt4(in);
          break;
        default:
          //session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "Command(" + cmdType.name() + ")"));
          break;
      }
    } catch (JimException ex) {
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
          sqlExecutor.executePrepare(session, sql);
          break;
        case MYSQL_COM_STMT_EXECUTE:
          sqlExecutor.execute(session, stmtID);
          break;
        case MYSQL_COM_STMT_CLOSE:
          session.getPrepareContext().getPreparedStmts().remove(stmtID);
          break;
        default:
          //throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "Command(" + cmdType + ")");
          session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "Command(" + cmdType.name() + ")"));
          break;
      }
    } catch (JimException ex) {
      session.writeError(ex);
    } catch (Exception ex) {
      session.writeError(DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex));
    }
  }

  private int handleCommandExecute(Session session, ByteBuf in) {
    PrepareContext context = session.getPrepareContext();
    int stmtId = CodecUtil.readInt4(in);
    JimStatement stmt = context.getPreparedStmts().get(stmtId);
    int parametersNum = stmt.getParametersNum();
    //flag
    CodecUtil.readInt1(in);
    //skip iteration-count
    CodecUtil.readInt4(in);

    NullBitMap nullBitmap = new NullBitMap(0, parametersNum);
    for (int i = 0; i < nullBitmap.getBits().length; i++) {
      nullBitmap.getBits()[i] = CodecUtil.readInt1(in);
    }
    List<SQLType> types;
    // bound param
    if (CodecUtil.readInt1(in) == 1) {
      types = getTypes(in, parametersNum);
    } else {
      types = stmt.getPrepareTypes();
    }

    stmt.setPrepareTypes(types);
    context.setPrepareTypes(types);
    context.setPreparedParams(getParams(in, types, parametersNum));
    return stmtId;
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

  @Override
  public void writeResult(Session session, CompositeByteBuf out, ExecResult result) {
    try {
      switch (result.getType()) {
        case HANDSHAKE:
          CodecUtil.encode(session, out, (HandshakeResult) result);
          break;
        case QUERY:
          CodecUtil.encode(session, out, (QueryResult) result);
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
      JimException je;
      if (ex instanceof JimException) {
        je = (JimException) ex;
      } else {
        je = DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex);
      }

      out.readerIndex(out.writerIndex());
      out.discardReadComponents();
      writeError(session, out, je);
    }
  }

  @Override
  public void writeError(Session session, CompositeByteBuf out, JimException ex) {
    CodecUtil.encode(session, out, ex);
  }

  @Override
  public void close() {
  }

  private List<SQLType> getTypes(ByteBuf byteBuf, final int paramsNum) {
    List<SQLType> paramTypes = new ArrayList<>(paramsNum);
    for (int i = 0; i < paramsNum; i++) {

      MySQLColumnDataType mySQLColumnDataType = MySQLColumnDataType.valueOf(CodecUtil.readInt1(byteBuf));
      paramTypes.add(Types.buildSQLType(MySQLColumnDataType.valueOfType(mySQLColumnDataType), CodecUtil.readInt1(byteBuf) > 0 ? true : false));
    }
    return paramTypes;
  }

  private List<Value> getParams(ByteBuf byteBuf, List<SQLType> paramTypes, int paramsNum) {
    List<Value> paramValues = new ArrayList<>(paramsNum);
    for (int i = 0; i < paramsNum; i++) {
      paramValues.add(CodecUtil.binaryRead(byteBuf, paramTypes.get(i)));
    }
    return paramValues;
  }
}
