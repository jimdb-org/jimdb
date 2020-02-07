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
package io.jimdb.mysql.util;

import static io.jimdb.mysql.constant.MySQLVariables.MYSQL_SERVER_ENCODING;
import static io.jimdb.mysql.constant.MySQLVariables.MYSQL_SERVER_VERSION;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Timestamp;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.exception.JimException;
import io.jimdb.core.Session;
import io.jimdb.core.context.StatementContext;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.result.QueryResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.PrepareResult;
import io.jimdb.core.types.Types;
import io.jimdb.core.types.ValueType;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.DateValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.StringValue;
import io.jimdb.core.values.TimeValue;
import io.jimdb.core.values.UnsignedLongValue;
import io.jimdb.core.values.Value;
import io.jimdb.core.values.YearValue;
import io.jimdb.mysql.constant.CapabilityFlags;
import io.jimdb.mysql.constant.MySQLColumnDataType;
import io.jimdb.mysql.constant.MySQLColumnFlag;
import io.jimdb.mysql.constant.MySQLError;
import io.jimdb.mysql.constant.MySQLErrorCode;
import io.jimdb.mysql.constant.MySQLVariables;
import io.jimdb.mysql.constant.MySQLVersion;
import io.jimdb.mysql.handshake.HandshakeInfo;
import io.jimdb.mysql.handshake.HandshakeResult;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Metapb;
import io.jimdb.pb.Metapb.SQLType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.internal.StringUtil;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * byte buffer util
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "CC_CYCLOMATIC_COMPLEXITY" })
public final class CodecUtil {
  private static final int OK_HEAD = 0x00;
  private static final int ERR_HEAD = 0xff;
  private static final int EOF_HEAD = 0xfe;
  private static final int NEXT_LENGTH = 0x0c;
  private static final int DEFAULT_VALUE_ZERO = 0x00;
  private static final int NULL = 0xfb;
  private static final int MAX_PACKET_LENGTH = 2 << 24 - 1;
  private static final double POW_16 = Math.pow(2, 16);
  private static final double POW_24 = Math.pow(2, 24);
  private static final int MAX_HEAD_VALUE = 0xff;

  private static final String SQL_STATE_MARKER = "#";

  private CodecUtil() {
  }

  /**
   * handshake
   *
   * @param session
   * @param out
   * @param result
   * @throws UnsupportedEncodingException
   * @see <a href="https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake">Handshake</a>
   */
  @SuppressFBWarnings("BED_BOGUS_EXCEPTION_DECLARATION")
  public static void encode(Session session, CompositeByteBuf out, HandshakeResult result) throws UnsupportedEncodingException {
    ByteBuf dataBuf = out.alloc().buffer(256);

    //protocol_version (1) -- 0x0a protocol_version
    writeInt1(dataBuf, MySQLVersion.PROTOCOL_VERSION_41);
    //server_version (string.NUL) -- human-readable server version
    writeStringWithNull(dataBuf, MySQLVariables.getVariable(MYSQL_SERVER_VERSION).getValue());
    //connection_id (4) -- connection id
    writeInt4(dataBuf, result.getConnID());
    //auth_plugin_data_part_1 (string.fix_len) -- [len=8] first 8 bytes of the auth-plugin data
    writeStringWithNull(dataBuf, new String(result.authData1, MySQLVariables.getVariable(MYSQL_SERVER_ENCODING)
            .getValue()));

    //capability_flag_1 (2) -- lower 2 bytes of the Protocol::CapabilityFlags (optional)
    writeInt2(dataBuf, CapabilityFlags.getDefaultCapabilityFlagsLower());
    //character_set (1) -- default server character-Fset, only the lower 8-bits Protocol::CharacterSet (optional)
    writeInt1(dataBuf, MySQLVersion.CHARSET);
    //status_flags (2) -- Protocol::StatusFlags (optional)
    writeInt2(dataBuf, session.getVarContext().getStatus());
    //capability_flags_2 (2) -- upper 2 bytes of the Protocol::CapabilityFlags
    writeInt2(dataBuf, CapabilityFlags.getDefaultCapabilityFlagsUpper());
    //auth_plugin_data_len (1) -- length of the combined auth_plugin_data, if auth_plugin_data_len is > 0
    writeInt1(dataBuf, DEFAULT_VALUE_ZERO);
    //string[10]     reserved (all [00])
    writeByteReserved(dataBuf, 10);
    //string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
    writeStringWithNull(dataBuf, new String(result.authData2, MySQLVariables.getVariable(MYSQL_SERVER_ENCODING)
            .getValue()));
    writePacket(session, out, dataBuf);
  }

  /**
   * common query
   *
   * @param session
   * @param out
   * @param resultSet
   * @see <a href="https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-Protocol::ColumnDefinition41">ColumnDefinition41</a>
   */
  public static void encode(Session session, CompositeByteBuf out, QueryResult resultSet) {
    ColumnExpr[] columns = resultSet.getColumns();
    StatementContext stmtContext = session.getStmtContext();
    if (!stmtContext.isReplying()) {
      ByteBuf dataBuf = out.alloc().buffer(8);
      writeEncodeInt(dataBuf, columns.length);
      writePacket(session, out, dataBuf);
      writeColumns(session, columns, out);
      stmtContext.setReplying(true);
    }

    resultSet.forEach(row -> {
      ByteBuf rowBuffer = out.alloc().buffer(1024);
      if (stmtContext.isBinaryProtocol()) {
        encodeBinaryRow(rowBuffer, columns, row);
      } else {
        encodeTextRow(rowBuffer, columns, row);
      }
      writePacket(session, out, rowBuffer);
    });

    //EOF_Packet
    if (resultSet.isEof()) {
      ByteBuf endBuffer = out.alloc().buffer(8);
      writeInt1(endBuffer, EOF_HEAD);
      writeInt2(endBuffer, DEFAULT_VALUE_ZERO);
      writeInt2(endBuffer, DEFAULT_VALUE_ZERO);
      writePacket(session, out, endBuffer);
    }
  }

  /**
   * ack
   *
   * @param session
   * @param out
   * @see <a href="https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html">OK</a>
   */
  public static void encodeACK(Session session, CompositeByteBuf out) {
    ByteBuf dataBuf = out.alloc().buffer(32);
    //message header
    writeInt1(dataBuf, OK_HEAD);
    //affected rows
    writeEncodeInt(dataBuf, DEFAULT_VALUE_ZERO);
    //last_insert_id
    writeEncodeInt(dataBuf, DEFAULT_VALUE_ZERO);
    Boolean isProtocol41 = (Boolean) session.getContext(HandshakeInfo.CLIENT_PROTOCOL_41);
    if (isProtocol41 != null && isProtocol41) {
      //status_flags
      writeInt2(dataBuf, session.getVarContext().getStatus());
      //warningCount
      writeInt2(dataBuf, DEFAULT_VALUE_ZERO);
    }
    //other message
    writeStringEof(dataBuf, StringUtil.EMPTY_STRING);
    writePacket(session, out, dataBuf);
  }

  /**
   * ok
   *
   * @param session
   * @param out
   * @param dmlResult
   * @see <a href="https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html">OK</a>
   */
  public static void encode(Session session, CompositeByteBuf out, DMLExecResult dmlResult) {
    ByteBuf dataBuf = out.alloc().buffer(32);
    //message header
    writeInt1(dataBuf, OK_HEAD);
    //affected rows
    writeEncodeInt(dataBuf, dmlResult.getAffectedRows());
    //last_insert_id
    writeEncodeUnsignedInt(dataBuf, dmlResult.getLastInsertId());
    //status_flags
    writeInt2(dataBuf, session.getVarContext().getStatus());
    //warningCount
    writeInt2(dataBuf, DEFAULT_VALUE_ZERO);
    //other message
    writeStringEof(dataBuf, StringUtil.EMPTY_STRING);
    writePacket(session, out, dataBuf);
  }

  /**
   * error
   *
   * @param session
   * @param out
   * @param ex
   * @see <a href="https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html">Error</a>
   */
  public static void encode(Session session, CompositeByteBuf out, JimException ex) {
    ByteBuf dataBuf = out.alloc().buffer(32);
    MySQLErrorCode mySQLErrorCode = MySQLError.toMySQLErrorCode(ex.getCode());
    //[ff] header of the ERR packet
    writeInt1(dataBuf, ERR_HEAD);
    //error-code
    writeInt2(dataBuf, mySQLErrorCode.getCode());
    //SQL_STATE_MARKER	# marker of the SQL State
    writeString(dataBuf, SQL_STATE_MARKER);
    //SQL State
    writeString(dataBuf, mySQLErrorCode.getState());
    //human readable error message
    writeStringEof(dataBuf, ex.getMessage());
    writePacket(session, out, dataBuf);
  }

  /**
   * prepare
   *
   * @param session
   * @param out
   * @param result
   * @see <a href="https://dev.mysql.com/doc/internals/en/com-stmt-execute.html">Error</a>
   */
  public static void encode(Session session, CompositeByteBuf out, PrepareResult result) {
    ByteBuf dataBuf = out.alloc().buffer();
    writeInt1(dataBuf, OK_HEAD);
    writeInt4(dataBuf, result.getStmtId());
    writeInt2(dataBuf, result.getColumnsNum());
    writeInt2(dataBuf, result.getParametersNum());
    writeByteReserved(dataBuf, 1);
    writeInt2(dataBuf, result.getWarnCount());
    writePacket(session, out, dataBuf);

    if (result.getParametersNum() > 0) {
      ColumnExpr[] columns = new ColumnExpr[result.getParametersNum()];
      for (int i = 0; i < result.getParametersNum(); i++) {
        ColumnExpr columnExpr = new ColumnExpr((long) i);
        columnExpr.setResultType(Types.buildSQLType(Basepb.DataType.BigInt));
        columns[i] = columnExpr;
      }
      writeColumns(session, columns, out);
    }

    if (result.getColumnsNum() > 0) {
      ColumnExpr[] columns = result.getColumns();
      writeColumns(session, columns, out);
    }
  }

  public static void writeColumns(Session session, ColumnExpr[] columns, CompositeByteBuf out) {
    SQLType colType;
    for (ColumnExpr column : columns) {
      ByteBuf columnBuffer = out.alloc().buffer(128);
      colType = column.getResultType();
      //catalog (lenenc_str) -- catalog
      writeEncodeString(columnBuffer, column.getCatalog());
      //schema (lenenc_str) -- schema-name
      writeEncodeString(columnBuffer, column.getCatalog());
      //table (lenenc_str) -- virtual table-name
      writeEncodeString(columnBuffer, column.getAliasTable());
      //org_table (lenenc_str) -- physical table-name
      writeEncodeString(columnBuffer, column.getOriTable());
      //name (lenenc_str) -- virtual column name
      writeEncodeString(columnBuffer, column.getAliasCol());
      //org_name (lenenc_str) -- physical column name
      writeEncodeString(columnBuffer, column.getOriCol());
      //NEXT_LENGTH (lenenc_int) -- length of the following fields (always 0x0c)
      writeEncodeInt(columnBuffer, NEXT_LENGTH);
      // is the column character set
      writeInt2(columnBuffer, CharsetUtil.getCharset(colType.getCharset()));
      //maximum length of the field
      writeInt4(columnBuffer, (int) colType.getPrecision());
      //column_type
      writeInt1(columnBuffer, MySQLColumnDataType.valueOfJDBCType(colType.getType()).getValue());
      //flags
      writeInt2(columnBuffer, getFlag(colType));
      //decimals
      writeInt1(columnBuffer, colType.getScale());
      //reserved
      writeByteReserved(columnBuffer, 2);
      //default value
      if (column.getDefaultValue() != null) {
        writeStringWithNull(columnBuffer, column.getDefaultValue().getString());
      }

      writePacket(session, out, columnBuffer);
    }

    //EOF_Packet
    ByteBuf eofBuffer = out.alloc().buffer(8);
    writeInt1(eofBuffer, EOF_HEAD);
    writeInt2(eofBuffer, DEFAULT_VALUE_ZERO);
    writeInt2(eofBuffer, DEFAULT_VALUE_ZERO);
    writePacket(session, out, eofBuffer);
  }

  public static void writePacket(Session session, CompositeByteBuf out, ByteBuf buffer) {
    ByteBuf header = out.alloc().buffer(16);
    while (buffer.readableBytes() > MAX_PACKET_LENGTH) {
      ByteBuf b = out.alloc().buffer(4);
      b.writeByte(MAX_HEAD_VALUE);
      b.writeByte(MAX_HEAD_VALUE);
      b.writeByte(MAX_HEAD_VALUE);
      b.writeByte(session.incrementAndGetSeqID());
      out.addComponents(true, b, buffer.readBytes(MAX_PACKET_LENGTH));
    }
    if (buffer.readableBytes() > 0) {
      header.writeMediumLE(buffer.readableBytes());
      header.writeByte(session.incrementAndGetSeqID());
      out.addComponents(true, header, buffer);
    }
  }

  private static void encodeTextRow(ByteBuf rowBuffer, ColumnExpr[] columns, ValueAccessor row) {
    Value val;
    for (ColumnExpr column : columns) {
      val = row.get(column.getOffset());
      if (null == val || val.isNull()) {
        writeInt1(rowBuffer, NULL);
        continue;
      }

      switch (val.getType()) {
        case BINARY:
          writeEncodeBytes(rowBuffer, ((BinaryValue) val).getValue());
          break;
        case DATE:
          String dataEncode = ((DateValue) val).convertToString(column.getResultType().getType(), null);
          writeEncodeString(rowBuffer, dataEncode);
          break;
        case TIME:
          String timeEncode = ((TimeValue) val).convertToString();
          writeEncodeString(rowBuffer, timeEncode);
          break;
        case YEAR:
          int year = ((YearValue) val).getValue();
          String str;
          if (year == 0) {
            str = "0000";
          } else {
            str = Integer.toString(year);
          }
          writeEncodeString(rowBuffer, str);
          break;
        default:
          writeEncodeString(rowBuffer, val.getString());
          break;
      }
    }
  }

  private static void encodeBinaryRow(ByteBuf rowBuffer, ColumnExpr[] columns, ValueAccessor row) {
    writeInt1(rowBuffer, OK_HEAD);
    int nullBitOffset = rowBuffer.writerIndex();
    int nullBitLen = (columns.length + 7 + 2) / 8;
    for (int i = 0; i < nullBitLen; i++) {
      rowBuffer.writeByte(0);
    }

    Value value;
    for (int i = 0; i < columns.length; i++) {
      value = row.get(columns[i].getOffset());
      if (value == null || value.isNull()) {
        int nullPos = nullBitOffset + ((i + 2) / 8);
        int bitPos = (i + 2) % 8;
        int nullByte = rowBuffer.getByte(nullPos) & 0xff;
        rowBuffer.setByte(nullPos, (byte) (nullByte | (1 << bitPos)));
        continue;
      }
      binaryWrite(rowBuffer, columns[i].getResultType(), value);
    }
  }

  private static void binaryWrite(ByteBuf buffer, SQLType type, Value val) {
    switch (type.getType()) {
      case TinyInt:
        writeInt1(buffer, (byte) ((LongValue) val).getValue());
        break;
      case SmallInt:
        writeInt2(buffer, (short) (((LongValue) val).getValue()));
        break;
      case MediumInt:
      case Int:
        writeInt4(buffer, (int) ((LongValue) val).getValue());
        break;
      case BigInt:
        if (val.getType() == ValueType.UNSIGNEDLONG) {
          writeInt8(buffer, ((UnsignedLongValue) val).getValue().longValue());
        } else {
          writeInt8(buffer, ((LongValue) val).getValue());
        }
        break;
      case Float:
        buffer.writeFloatLE((float) ((DoubleValue) val).getValue());
        break;
      case Double:
        buffer.writeDoubleLE(((DoubleValue) val).getValue());
        break;
      case Decimal:
        writeEncodeString(buffer, val.getString());
        break;

      case Varchar:
      case Char:
      case TinyBlob:
      case Blob:
      case MediumBlob:
      case LongBlob:
      case TinyText:
      case Text:
      case MediumText:
      case LongText:
      case Bit:
        if (val.getType() == ValueType.BINARY) {
          writeEncodeBytes(buffer, ((BinaryValue) val).getValue());
        } else {
          writeEncodeString(buffer, val.getString());
        }
        break;

      case Date:
      case DateTime:
      case TimeStamp:
        writeDateTime(buffer, (DateValue) val);
        break;
      case Time:
        writeTime(buffer, (TimeValue) val);
        break;
      case Year:
        writeInt2(buffer, (short) (((YearValue) val).getValue()));
        break;

      default:
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "value type " + type.getType().name());
    }
  }

  private static void writeDateTime(ByteBuf buffer, DateValue value) {
    Timestamp ts = value.getValue();
    int year = ts.getYear() + 1900;
    int month = ts.getMonth() + 1;
    int day = ts.getDate();


    if (value.getDateType() == Basepb.DataType.Date) {
      buffer.writeByte(4);
      buffer.writeShortLE(year);
      buffer.writeByte(month);
      buffer.writeByte(day);
      return;
    }

    buffer.writeByte(11);
    buffer.writeShortLE(year);
    buffer.writeByte(month);
    buffer.writeByte(day);
    buffer.writeByte(ts.getHours());
    buffer.writeByte(ts.getMinutes());
    buffer.writeByte(ts.getSeconds());
    buffer.writeIntLE(ts.getSeconds() / 1000 / 1000);

  }

  private static void writeTime(ByteBuf buffer, TimeValue value) {
    long time = value.getValue();
    if (time == 0) {
      buffer.writeByte(0);
      return;
    }

    byte isNeg = 0;
    if (time < 0) {
      isNeg = 1;
      time = -time;
    }

    long day = time / Types.DAY;
    time -= day * Types.DAY;
    long hour = time / Types.HOUR;
    time -= hour * Types.HOUR;
    long minute = time / Types.MINUTE;
    time -= minute * Types.MINUTE;
    long second = time / Types.SECOND;
    time -= second * Types.SECOND;
    if (time == 0) {
      buffer.writeByte(8);
    } else {
      buffer.writeByte(12);
    }
    buffer.writeByte(isNeg);
    buffer.writeByte((byte) day);
    buffer.writeByte(0);
    buffer.writeByte(0);
    buffer.writeByte(0);
    buffer.writeByte((byte) hour);
    buffer.writeByte((byte) minute);
    buffer.writeByte((byte) second);
    if (time > 0) {
      buffer.writeIntLE((int) (time / Types.MICROSECOND));
    }
  }

  public static Value binaryRead(ByteBuf in, SQLType type) {
    switch (type.getType()) {
      case Null:
        return null;
      case TinyInt:
        if (in.readableBytes() < 1) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int tinyIntVale = type.getUnsigned() ? readInt1(in) : in.readByte();
        return LongValue.getInstance(tinyIntVale);
      case SmallInt:
        if (in.readableBytes() < 2) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int smallIntVal = type.getUnsigned() ? readInt2(in) : in.readShortLE();
        return LongValue.getInstance(smallIntVal);
      case MediumInt:
      case Int:
        if (in.readableBytes() < 4) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int intVal = type.getUnsigned() ? readInt4(in) : in.readIntLE();
        return LongValue.getInstance(intVal);
      case BigInt:
        if (in.readableBytes() < 8) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        long longVal = in.readLongLE();
        if (type.getUnsigned()) {
          return UnsignedLongValue.getInstance(new BigInteger(Long.toUnsignedString(longVal)));
        }
        return LongValue.getInstance(longVal);
      case Float:
        if (in.readableBytes() < 4) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        return DoubleValue.getInstance(in.readFloatLE());
      case Double:
        if (in.readableBytes() < 8) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        return DoubleValue.getInstance(in.readDoubleLE());
      case Date:
      case DateTime:
      case TimeStamp:
        if (in.readableBytes() < 1) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int len = readInt1(in);
        switch (len) {
          case 0:
            return DateValue.getInstance(Types.ZERO_DATETIME, type.getType());
          case 4:
            return DateValue.getInstance(String.format("%04d-%02d-%02d", readInt2(in),
                    readInt1(in), readInt1(in)), type.getType());
          case 7:
            return DateValue.getInstance(String.format("%04d-%02d-%02d %02d:%02d:%02d", readInt2(in),
                    readInt1(in), readInt1(in), readInt1(in), readInt1(in), readInt1(in)), type.getType());
          case 11:
            return DateValue.getInstance(String.format("%04d-%02d-%02d %02d:%02d:%02d.%06d", readInt2(in),
                    readInt1(in), readInt1(in), readInt1(in), readInt1(in), readInt1(in), ((long) in.readIntLE()) & 0xffffffff), type.getType());
          default:
            throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }
      case Time:
        if (in.readableBytes() < 1) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int length = readInt1(in);
        switch (length) {
          case 0:
            return TimeValue.TIME_ZERO;
          case 8:
          case 12:
            int isNeg = readInt1(in);
            if (isNeg > 1) {
              throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
            }

            in.skipBytes(4);
            if (length == 8) {
              return TimeValue.getInstance(readInt1(in), readInt1(in), readInt1(in), 0, isNeg == 1);
            }
            return TimeValue.getInstance(readInt1(in), readInt1(in), readInt1(in), readInt4(in), isNeg == 1);

          default:
            throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }
      case Year:
        if (in.readableBytes() < 2) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        int yearVal = type.getUnsigned() ? readInt2(in) : in.readShortLE();
        return YearValue.getInstance(yearVal);

      case Varchar:
      case Char:
      case Decimal:
      case TinyBlob:
      case Blob:
      case MediumBlob:
      case LongBlob:
      case Bit:
      case Enum:
      case Set:
      case Invalid:
        if (in.readableBytes() < 1) {
          throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
        }

        String value = readEncodeString(in);
        return value == null ? null : StringValue.getInstance(value);

      default:
        throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_NOT_SUPPORTED_YET, "stmt field type " + type.getType().name());
    }
  }

  public static int getFlag(Metapb.SQLTypeOrBuilder colType) {
    int flag = 0;
    if (colType.getNotNull()) {
      flag |= MySQLColumnFlag.NOT_NULL_FLAG.getValue();
    }
    if (colType.getUnsigned()) {
      flag |= MySQLColumnFlag.UNSIGNED_FLAG.getValue();
    }
    if (colType.getBinary()) {
      flag |= MySQLColumnFlag.BINARY_FLAG.getValue();
    }
    if (colType.getZerofill()) {
      flag |= MySQLColumnFlag.ZEROFILL_FLAG.getValue();
    }
    return flag;
  }

  public static void readHeader(ByteBuf byteBuf) {
    byteBuf.markReaderIndex().readMediumLE();
  }

  public static int readInt1(ByteBuf byteBuf) {
    return byteBuf.readByte() & 0xff;
  }

  public static void writeInt1(ByteBuf byteBuf, final int value) {
    byteBuf.writeByte(value);
  }

  public static int readInt2(ByteBuf byteBuf) {
    return byteBuf.readShortLE() & 0xffff;
  }

  public static int getInt2(ByteBuf byteBuf, int index) {
    return byteBuf.getShortLE(index) & 0xffff;
  }

  public static void writeInt2(ByteBuf byteBuf, int value) {
    byteBuf.writeShortLE(value);
  }

  public static int readInt3(ByteBuf byteBuf) {
    return byteBuf.readMediumLE() & 0xffffff;
  }

  public static void writeInt3(ByteBuf byteBuf, int value) {
    byteBuf.writeMediumLE(value);
  }

  public static int readInt4(ByteBuf byteBuf) {
    return byteBuf.readIntLE();
  }

  public static void writeInt4(ByteBuf byteBuf, int value) {
    byteBuf.writeIntLE(value);
  }

  public static long readInt8(ByteBuf byteBuf) {
    return byteBuf.readLongLE();
  }

  public static void writeInt8(ByteBuf byteBuf, long value) {
    byteBuf.writeLongLE(value);
  }

  public static long readEncodeInt(ByteBuf byteBuf) {
    int firstByte = readInt1(byteBuf);
    if (firstByte < 0xfb) {
      return firstByte;
    }
    if (0xfb == firstByte) {
      return 0;
    }
    if (0xfc == firstByte) {
      return byteBuf.readShortLE();
    }
    if (0xfd == firstByte) {
      return byteBuf.readMediumLE();
    }
    return byteBuf.readLongLE();
  }

  public static void writeEncodeInt(ByteBuf byteBuf, long value) {
    if (value < 0xfb) {
      byteBuf.writeByte((int) value);
      return;
    }

    if (value < POW_16) {
      byteBuf.writeByte(0xfc);
      byteBuf.writeShortLE((int) value);
      return;
    }

    if (value < POW_24) {
      byteBuf.writeByte(0xfd);
      byteBuf.writeMediumLE((int) value);
      return;
    }

    byteBuf.writeByte(0xfe);
    byteBuf.writeLongLE(value);
  }

  public static void writeEncodeUnsignedInt(ByteBuf byteBuf, long value) {
    if (value <= -1) {
      byteBuf.writeByte(0xfe);
      byteBuf.writeLongLE(value);
      return;
    }

    if (value < 0xfb) {
      byteBuf.writeByte((int) value);
      return;
    }

    if (value < POW_16) {
      byteBuf.writeByte(0xfc);
      byteBuf.writeShortLE((int) value);
      return;
    }

    if (value < POW_24) {
      byteBuf.writeByte(0xfd);
      byteBuf.writeMediumLE((int) value);
      return;
    }

    byteBuf.writeByte(0xfe);
    byteBuf.writeLongLE(value);
  }

  public static byte[] readEncodeStringByBytes(ByteBuf byteBuf) {
    int length = (int) readEncodeInt(byteBuf);
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return result;
  }

  public static String readEncodeString(ByteBuf byteBuf) {
    int length = (int) readEncodeInt(byteBuf);
    if (length < 1) {
      return null;
    }
    if (byteBuf.readableBytes() < length) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_MALFORMED_PACKET);
    }

    try {
      CharSequence sequence = byteBuf.readCharSequence(length,
              Charset.forName(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
      return sequence == null ? null : sequence.toString();
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
  }

  public static void writeEncodeString(ByteBuf byteBuf, String value) {
    if (StringUtil.isNullOrEmpty(value)) {
      byteBuf.writeByte(0);
      return;
    }

    try {
      writeEncodeInt(byteBuf, value.getBytes(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()).length);
      byteBuf.writeBytes(value.getBytes(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
  }

  public static void writeEncodeBytes(ByteBuf byteBuf, byte[] value) {
    if (0 == value.length) {
      byteBuf.writeByte(0);
      return;
    }

    writeEncodeInt(byteBuf, value.length);
    byteBuf.writeBytes(value);
  }

  public static byte[] readStringByBytes(ByteBuf byteBuf, int length) {
    byte[] result = new byte[length];
    byteBuf.readBytes(result);
    return result;
  }

  public static void writeString(ByteBuf byteBuf, String value) {
    try {
      byte[] tt = value.getBytes(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue());
      byteBuf.writeBytes(tt);
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
  }

  public static void writeBytes(ByteBuf byteBuf, byte[] value) {
    byteBuf.writeBytes(value);
  }

  public static String readStringWithNull(ByteBuf byteBuf) {
    int length = byteBuf.bytesBefore((byte) 0);
    try {
      CharSequence sequence = byteBuf.readCharSequence(length,
              Charset.forName(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
      return sequence == null ? null : sequence.toString();
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    } finally {
      byteBuf.skipBytes(1);
    }
  }

  public static byte[] readStringNullByBytes(ByteBuf byteBuf) {
    byte[] result = new byte[byteBuf.bytesBefore((byte) 0)];
    byteBuf.readBytes(result);
    byteBuf.skipBytes(1);
    return result;
  }

  public static void writeStringWithNull(ByteBuf byteBuf, String value) {
    try {
      byteBuf.writeBytes(value.getBytes(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
    byteBuf.writeByte(0);
  }

  public static String readStringEof(ByteBuf byteBuf) {
    try {
      CharSequence sequence = byteBuf.readCharSequence(byteBuf.readableBytes(),
              Charset.forName(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
      return sequence == null ? null : sequence.toString();
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
  }

  public static void writeStringEof(ByteBuf byteBuf, String value) {
    try {
      if (value == null) {
        value = StringUtil.EMPTY_STRING;
      }
      byteBuf.writeBytes(value.getBytes(MySQLVariables.getVariable(MYSQL_SERVER_ENCODING).getValue()));
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_RPC_REQUEST_CODEC, e);
    }
  }

  public static void skipByteReserved(ByteBuf byteBuf, int length) {
    byteBuf.skipBytes(length);
  }

  public static void writeByteReserved(ByteBuf byteBuf, int length) {
    for (int i = 0; i < length; i++) {
      byteBuf.writeByte(0);
    }
  }
}
