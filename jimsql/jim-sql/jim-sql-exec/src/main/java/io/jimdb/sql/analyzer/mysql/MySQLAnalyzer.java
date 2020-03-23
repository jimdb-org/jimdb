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
package io.jimdb.sql.analyzer.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;
import io.jimdb.common.utils.lang.StringUtil;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.Schema;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.types.Types;
import io.jimdb.core.values.StringValue;
import io.jimdb.pb.Basepb;
import io.jimdb.pb.Basepb.DataType;
import io.jimdb.pb.Metapb;
import io.jimdb.sql.analyzer.AnalyzerType;
import io.jimdb.sql.analyzer.AnalyzerUtil;
import io.jimdb.sql.analyzer.ExpressionAnalyzer;
import io.jimdb.sql.analyzer.InsertAnalyzer;
import io.jimdb.sql.analyzer.KeyGetAnalyzer;
import io.jimdb.sql.analyzer.Variables;
import io.jimdb.sql.analyzer.Variables.Variable;
import io.jimdb.sql.analyzer.Variables.VariableAssign;
import io.jimdb.sql.operator.Analyze;
import io.jimdb.sql.operator.Commit;
import io.jimdb.sql.operator.Delete;
import io.jimdb.sql.operator.DualTable;
import io.jimdb.sql.operator.Explain;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.Rollback;
import io.jimdb.sql.operator.Set;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.Use;
import io.jimdb.sql.operator.show.Show;
import io.jimdb.sql.operator.show.ShowCharacter;
import io.jimdb.sql.operator.show.ShowCollation;
import io.jimdb.sql.operator.show.ShowColumns;
import io.jimdb.sql.operator.show.ShowCreateTable;
import io.jimdb.sql.operator.show.ShowDatabases;
import io.jimdb.sql.operator.show.ShowDatabasesInfo;
import io.jimdb.sql.operator.show.ShowIndex;
import io.jimdb.sql.operator.show.ShowStatus;
import io.jimdb.sql.operator.show.ShowTables;
import io.jimdb.sql.operator.show.ShowTablesInfo;
import io.jimdb.sql.operator.show.ShowVariables;
import io.jimdb.sql.optimizer.OptimizeFlag;

import org.apache.commons.lang3.StringUtils;

import com.alibaba.druid.sql.ast.SQLAdhocTableSource;
import com.alibaba.druid.sql.ast.SQLCurrentTimeExpr;
import com.alibaba.druid.sql.ast.SQLCurrentUserExpr;
import com.alibaba.druid.sql.ast.SQLDataTypeRefExpr;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLDecimalExpr;
import com.alibaba.druid.sql.ast.expr.SQLDefaultExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLSizeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLAssignItem;
import com.alibaba.druid.sql.ast.statement.SQLCommitStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLCreateIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropDatabaseStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropIndexStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropTableStatement;
import com.alibaba.druid.sql.ast.statement.SQLDropUserStatement;
import com.alibaba.druid.sql.ast.statement.SQLExplainStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLGrantStatement;
import com.alibaba.druid.sql.ast.statement.SQLRevokeStatement;
import com.alibaba.druid.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSetStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesInfoStatement;
import com.alibaba.druid.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.druid.sql.ast.statement.SQLTableSampling;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUseStatement;
import com.alibaba.druid.sql.ast.statement.SQLValuesQuery;
import com.alibaba.druid.sql.dialect.mysql.ast.expr.MySqlCharExpr;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAlterUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlAnalyzeStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlCreateUserStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlFlushStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCharacterSetStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCollationStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowColumnsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowCreateTableStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesInfoStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowDatabasesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowGrantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowIndexesStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatusStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;

/**
 * @version V1.0
 */
@SuppressFBWarnings({ "PL_PARALLEL_LISTS", "CLI_CONSTANT_LIST_INDEX" })
public final class MySQLAnalyzer extends MySqlVisitorAdapter {
  private static final String VARS_COLLATION = "collation_connection";
  private static final String[] VARS_CHARSET = { "character_set_client", "character_set_results", "character_set_connection" };
  private static final String[] SHOW_VARIABLE_COLUMNS = { "Variable_name", "Value" };
  private static final String[] SHOW_COLLATION_COLUMNS = { "Collation", "Charset", "Id", "Default", "Compiled", "Sortlen" };
  private static final DataType[] SHOW_COLLATION_TYPES = { DataType.Varchar, DataType.Varchar, DataType.BigInt, DataType.Varchar, DataType.Varchar, DataType.BigInt };
  private static final String[] SHOW_CHARSET_COLUMNS = { "Charset", "Description", "Default collation", "Maxlen" };
  private static final DataType[] SHOW_CHARSET_TYPES = { DataType.Varchar, DataType.Varchar, DataType.Varchar, DataType.BigInt };
  private static final String[] SHOW_DATABASE_COLUMNS = { "Database" };
  private static final String[] SHOW_DATABASE_ORC_COLUMNS = { "SCHEMA_NAME" };
  private static final DataType[] SHOW_DATABASE_TYPES = { DataType.Varchar };
  private static final String[] SHOW_DATABASE_INFO_COLUMNS = { "Id", "Database" };
  private static final DataType[] SHOW_DATABASE_INFO_TYPES = { DataType.BigInt, DataType.Varchar };
  private static final String[] SHOW_TABLE_COLUMNS = { "Tables_in_" };
  private static final DataType[] SHOW_TABLE_TYPES = { DataType.Varchar };
  private static final String[] SHOW_TABLE_INFO_COLUMNS = { "DbId", "Id", "Tables_in_" };
  private static final DataType[] SHOW_TABLE_INFO_TYPES = { DataType.BigInt, DataType.BigInt, DataType.Varchar };
  private static final String[] SHOW_STATUS_COLUMNS = {};
  private static final DataType[] SHOW_STATUS_TYPES = {};
  private static final String[] EXPLAIN_COLUMNS = { "op", "count", "task", "operator_info" };
  private static final DataType[] EXPLAIN_TYPES = { DataType.Varchar, DataType.Varchar, DataType.Varchar, DataType.Varchar };
  private static final String[] EXPLAIN_ANALYZE_COLUMNS = { "op", "count", "task", "operator_info", "execution_info", "memory" };
  private static final DataType[] EXPLAIN_ANALYZE_TYPES = { DataType.Varchar, DataType.Varchar, DataType.Varchar, DataType.Varchar, DataType.Varchar };
  private static final String[] SHOW_INDEX_COLUMNS = { "Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
          "Collation", "Cardinality", "Sub_part", "Packed", "Null", "Index_type", "Comment", "Index_comment" };
  private static final DataType[] SHOW_INDEX_TYPES = { DataType.Varchar, DataType.BigInt, DataType.Varchar,
          DataType.BigInt, DataType.Varchar, DataType.Varchar, DataType.BigInt, DataType.BigInt, DataType.Varchar,
          DataType.Varchar, DataType.Varchar, DataType.Varchar, DataType.Varchar };
  private static final String[] SHOW_COLUMNS_COLUMNS = { "Field", "Type", "Null", "Key", "Default", "Extra" };
  private static final String[] SHOW_FULL_COLUMNS_COLUMNS = { "Field", "Type", "Collation", "Null", "Key", "Default",
          "Extra", "Privileges", "Comment" };
  private static final String[] SHOW_CREATE_TABLE_COLUMNS = { "Table", "Create Table" };

  @Override
  protected ExpressionAnalyzer buildExpressionAnalyzer() {
    return new MySQLAnalyzer();
  }

  @Override
  public boolean visit(SQLExplainStatement x) {
    return true;
  }

  @Override
  public boolean visit(MySqlInsertStatement stmt) {
    this.resultOperator = InsertAnalyzer.analyzeInsert(this, stmt, stmt.getDuplicateKeyUpdate());
    return false;
  }

  @Override
  public boolean visit(MySqlUpdateStatement stmt) {
    if (stmt.getWhere() == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "update without condition");
    }

    RelOperator selectOp = null;
    SQLTableSource tableSource = stmt.getTableSource();
    if (tableSource != null) {
      // key update
      final SQLSelectQueryBlock keySelect = new SQLSelectQueryBlock();
      keySelect.addSelectItem(new SQLAllColumnExpr());
      keySelect.setFrom(tableSource);
      keySelect.setWhere(stmt.getWhere());
      keySelect.setLimit(stmt.getLimit());
      keySelect.setOrderBy(stmt.getOrderBy());
      selectOp = KeyGetAnalyzer.analyze(session, keySelect, AnalyzerType.WRITE);
    }

    if (selectOp == null) {
      selectOp = this.analyzeTableSource(tableSource, AnalyzerType.WRITE);
      if (stmt.getWhere() != null) {
        selectOp = this.analyzeFilter(selectOp, stmt.getWhere(), null);
      }
      if (stmt.getOrderBy() != null) {
        selectOp = this.analyzeOrder(selectOp, stmt.getOrderBy(), null);
      }
      if (stmt.getLimit() != null) {
        selectOp = this.analyzeLimit(selectOp, stmt.getLimit());
      }
    }

    this.resultOperator = this.analyzeUpdate(selectOp, stmt.getItems(), tableSource);
    return false;
  }

  @Override
  public boolean visit(MySqlDeleteStatement stmt) {
    if (stmt.getFrom() != null || stmt.getUsing() != null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "delete multiple-table");
    }
    if (stmt.getWhere() == null) {
      throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_NOT_SUPPORTED_YET, "delete without condition");
    }

    // key delete
    final SQLTableSource tableSource = stmt.getTableSource();
    final Table table = AnalyzerUtil.resolveTable(session, tableSource);
    final SQLSelectQueryBlock keySelect = new SQLSelectQueryBlock();
    keySelect.setFrom(tableSource);
    keySelect.setWhere(stmt.getWhere());
    keySelect.setLimit(stmt.getLimit());
    keySelect.setOrderBy(stmt.getOrderBy());

    final Index[] indices = table.getDeletableIndices();
    List<Column> selectList = new ArrayList<>(8);
    if (indices != null && indices.length > 0) {
      Column[] columns;
      final Map<Integer, Boolean> colMap = new HashMap<>(8);
      for (Index index : indices) {
        columns = index.getColumns();
        for (Column column : columns) {
          if (colMap.put(column.getId(), Boolean.TRUE) == null) {
            keySelect.addSelectItem(new SQLSelectItem(new SQLIdentifierExpr(column.getName())));
            selectList.add(column);
          }
        }
      }
    }
    RelOperator selectOp = KeyGetAnalyzer.analyze(session, keySelect, AnalyzerType.DELETE);

    if (selectOp == null) {
      selectOp = this.analyzeTableSource(tableSource, AnalyzerType.DELETE);
      Schema tableSchema = selectOp.getSchema();
      if (stmt.getWhere() != null) {
        selectOp = this.analyzeFilter(selectOp, stmt.getWhere(), null);
      }
      if (stmt.getOrderBy() != null) {
        selectOp = this.analyzeOrder(selectOp, stmt.getOrderBy(), null);
      }
      if (stmt.getLimit() != null) {
        selectOp = this.analyzeLimit(selectOp, stmt.getLimit());
      }
      if (selectList.size() > 0) {
        ColumnExpr columnExpr;
        List<ColumnExpr> columnExprs = new ArrayList<>(8);
        for (Column column : selectList) {
          columnExpr = tableSchema.getColumn(column.getName());
          if (columnExpr == null) {
            throw DBException.get(ErrorModule.PARSER, ErrorCode.ER_BAD_FIELD_ERROR, column.getName(), "index");
          }
          columnExprs.add(columnExpr);
        }

        selectOp = new Projection(columnExprs.toArray(new ColumnExpr[columnExprs.size()]), new Schema(columnExprs), selectOp);
      }
    }

    Delete delete = new Delete(table);
    delete.setSelect(selectOp);
    this.resultOperator = delete;
    return false;
  }

  @Override
  public boolean visit(MySqlSetTransactionStatement stmt) {
    String level = stmt.getIsolationLevel();
    boolean isGlobal = stmt.getGlobal() != null && stmt.getGlobal();
    if (level != null) {
      Variable variable = new Variable("transaction_isolation");
      variable.setSystem(true);
      variable.setGlobal(isGlobal);
      VariableAssign varAssign = new VariableAssign(variable);
      varAssign.setValue(new ValueExpr(StringValue.getInstance(level), Types.buildSQLType(DataType.Varchar)));

      Variable variable1 = new Variable("tx_isolation");
      variable1.setSystem(true);
      variable1.setGlobal(isGlobal);
      VariableAssign varAssign1 = new VariableAssign(variable1);
      varAssign1.setValue(new ValueExpr(StringValue.getInstance(level), Types.buildSQLType(DataType.Varchar)));
      this.resultOperator = new Set(varAssign, varAssign1);
    } else {
      boolean readOnly = "READ ONLY".equalsIgnoreCase(stmt.getAccessModel());
      Variable variable = new Variable("transaction_read_only");
      variable.setSystem(true);
      variable.setGlobal(isGlobal);
      VariableAssign varAssign = new VariableAssign(variable);
      varAssign.setValue(new ValueExpr(readOnly ? StringValue.getInstance("TRUE") : StringValue.getInstance("FALSE"), Types.buildSQLType(DataType.Varchar)));

      Variable variable1 = new Variable("tx_read_only");
      variable1.setSystem(true);
      variable1.setGlobal(isGlobal);
      VariableAssign varAssign1 = new VariableAssign(variable1);
      varAssign1.setValue(new ValueExpr(readOnly ? StringValue.getInstance("TRUE") : StringValue.getInstance("FALSE"), Types.buildSQLType(DataType.Varchar)));
      this.resultOperator = new Set(varAssign, varAssign1);
    }
    return false;
  }

  @Override
  public boolean visit(SQLSetStatement stmt) {
    SQLSetStatement.Option option = stmt.getOption();
    List<PrivilegeInfo> privilegeInfos = session.getStmtContext().getPrivilegeInfos();
    if (option == SQLSetStatement.Option.PASSWORD) {
      privilegeInfos.add(new PrivilegeInfo("", "", "", PrivilegeType.CREATE_USER_PRIV));
      this.resultOperator = PrivilegeAnalyzer.analyzeSetPass(stmt);
      return false;
    }

    List<SQLAssignItem> assigns = stmt.getItems();
    List<VariableAssign> varAssigns = new ArrayList<>(assigns.size());
    List<VariableAssign> charsetAssigns;
    VariableAssign varAssign;
    for (SQLAssignItem assign : assigns) {
      charsetAssigns = this.resolveCharsetAndCollation(assign);
      if (charsetAssigns == null) {
        varAssign = this.resolveVariableAssign(assign);
        varAssigns.add(varAssign);
      } else {
        varAssigns.addAll(charsetAssigns);
      }
    }

    this.resultOperator = new Set(varAssigns.toArray(new VariableAssign[varAssigns.size()]));
    return false;
  }

  @Override
  public boolean visit(SQLUseStatement stmt) {
    String databases = stmt.getDatabase().toString();
    this.resultOperator = new Use(databases);
    return false;
  }

  private List<VariableAssign> resolveCharsetAndCollation(SQLAssignItem assign) {
    boolean isDefault = false;
    SQLExpr target = assign.getTarget();
    SQLExpr value = assign.getValue();
    Expression valueExpr = null;
    List<VariableAssign> result = null;

    if (target instanceof SQLIdentifierExpr) {
      if ("CHARACTER SET".equalsIgnoreCase(((SQLIdentifierExpr) target).getName())) {
        result = new ArrayList<>(VARS_CHARSET.length);
        if (value instanceof SQLDefaultExpr) {
          isDefault = true;
        } else {
          Tuple2<Optional<Expression>, RelOperator> expression = this.analyzeExpression(new DualTable(0), value, null, true, null);
          valueExpr = expression.getT1().orElseGet(null);
        }
      }
    } else if (target instanceof SQLVariantRefExpr) {
      if ("NAMES".equalsIgnoreCase(((SQLVariantRefExpr) target).getName())) {
        result = new ArrayList<>(VARS_CHARSET.length + 1);
        boolean collDefault = true;
        Expression collExpr = null;
        if (value instanceof SQLIdentifierExpr) {
          isDefault = "DEFAULT".equalsIgnoreCase(((SQLIdentifierExpr) value).getName());
          collDefault = isDefault;
          if (!collDefault) {
            valueExpr = new ValueExpr(StringValue.getInstance(((SQLIdentifierExpr) value).getName()), Types.buildSQLType(DataType.Varchar));
            collExpr = valueExpr;
          }
        } else if (value instanceof MySqlCharExpr) {
          MySqlCharExpr charExpr = (MySqlCharExpr) value;
          collDefault = "DEFAULT".equalsIgnoreCase(charExpr.getCollate());
          valueExpr = new ValueExpr(StringValue.getInstance(charExpr.getText()), Types.buildSQLType(DataType.Varchar));
          if (!collDefault) {
            collExpr = new ValueExpr(StringValue.getInstance(charExpr.getCollate()), Types.buildSQLType(DataType.Varchar));
          }
        } else {
          Tuple2<Optional<Expression>, RelOperator> expression = this.analyzeExpression(new DualTable(0), value, null, true, null);
          valueExpr = expression.getT1().orElseGet(null);
          collExpr = valueExpr;
        }

        VariableAssign varAssign = new VariableAssign(new Variable(VARS_COLLATION));
        varAssign.getVariable().setSystem(true);
        varAssign.setDefaultValue(collDefault);
        varAssign.setValue(collExpr);
        result.add(varAssign);
      }
    }

    if (result != null) {
      if (!isDefault && valueExpr == null) {
        Tuple2<Optional<Expression>, RelOperator> expression = this.analyzeExpression(new DualTable(0), value, null, true, null);
        valueExpr = expression.getT1().orElseGet(null);
      }

      VariableAssign varAssign;
      for (String var : VARS_CHARSET) {
        varAssign = new VariableAssign(new Variable(var));
        varAssign.getVariable().setSystem(true);
        varAssign.setDefaultValue(isDefault);
        varAssign.setValue(valueExpr);
        result.add(varAssign);
      }
    }

    return result;
  }

  private VariableAssign resolveVariableAssign(SQLAssignItem assign) {
    Variable variable = Variables.resolveVariable(assign.getTarget());
    if (variable == null) {
      return null;
    }

    SQLExpr value = assign.getValue();
    VariableAssign result = new VariableAssign(variable);
    if (value instanceof SQLDefaultExpr) {
      result.setDefaultValue(true);
    } else {
      Tuple2<Optional<Expression>, RelOperator> expression = this.analyzeExpression(new DualTable(0), value, null, true, null);
      result.setValue(expression.getT1().orElseGet(null));
    }
    return result;
  }

  @Override
  public boolean visit(MySqlShowVariantsStatement statement) {
    ShowVariables showVariables = new ShowVariables();
    showVariables.setGlobal(statement.isGlobal());
    analyzeShow(showVariables, SHOW_VARIABLE_COLUMNS, null, null, statement.getLike(), statement.getWhere());
    return false;
  }

  @Override
  public boolean visit(MySqlShowCollationStatement statement) {
    analyzeShow(new ShowCollation(), SHOW_COLLATION_COLUMNS, null, SHOW_COLLATION_TYPES, null, statement.getWhere());
    return false;
  }

  @Override
  public boolean visit(MySqlShowCharacterSetStatement statement) {
    analyzeShow(new ShowCharacter(), SHOW_CHARSET_COLUMNS, null, SHOW_CHARSET_TYPES, statement.getPattern(), statement.getWhere());
    return false;
  }

  private void analyzeShow(RelOperator operator, String[] names, String[] oriNames, DataType[] types, SQLExpr like, SQLExpr where) {
    List<ColumnExpr> columnExprs = new ArrayList<>(names.length);
    for (int i = 0; i < names.length; i++) {
      ColumnExpr columnExpr = new ColumnExpr(session.allocColumnID());
      columnExpr.setAliasCol(names[i]);
      if (oriNames != null) {
        columnExpr.setOriCol(oriNames[i]);
      } else {
        columnExpr.setOriCol(names[i]);
      }
      Metapb.SQLType sqlType;
      if (types != null) {
        sqlType = Types.buildSQLType(types[i]);
      } else {
        sqlType = Types.buildSQLType(Basepb.DataType.Varchar);
      }
      columnExpr.setResultType(sqlType);
      columnExprs.add(columnExpr);
    }

    operator.setSchema(new Schema(columnExprs));
    DualTable mockTablePlan = new DualTable(1);
    mockTablePlan.setPlaceHolder(true);
    mockTablePlan.setSchema(operator.getSchema());
    RelOperator newPlan = mockTablePlan;

    if (like != null) {
      // todo: support like expression
    }
    if (where != null) {
      newPlan = analyzeFilter(mockTablePlan, where, null);
    }
    if (newPlan == mockTablePlan) {
      this.resultOperator = new Show(operator);
      return;
    }

    List<Expression> expressions = new ArrayList<>();
    List<ColumnExpr> newColumns = new ArrayList<>();
    mockTablePlan.getSchema().getColumns().forEach(e -> {
      expressions.add(e);
      ColumnExpr clone = e.clone();
      clone.setUid(session.allocColumnID());
      newColumns.add(clone);
    });

    Projection projection = new Projection(expressions.toArray(new Expression[expressions.size()]), new Schema(newColumns), newPlan);

    this.optimizationFlag |= OptimizeFlag.ELIMINATEPROJECTION;
    this.resultOperator = new Show(operator, projection);
  }

  @Override
  public boolean visit(MySqlExplainStatement stmt) {
    if (!stmt.isDescribe()) {
      // explain
      return true;
    }

    // desc
    String dbName = null;
    String tableName = null;
    SQLName table = stmt.getTableName();
    if (table instanceof SQLIdentifierExpr) {
      tableName = ((SQLIdentifierExpr) table).getName();
    } else if (table instanceof SQLPropertyExpr) {
      tableName = ((SQLPropertyExpr) table).getName();
      SQLExpr owner = ((SQLPropertyExpr) table).getOwner();
      if (owner instanceof SQLIdentifierExpr) {
        dbName = ((SQLIdentifierExpr) owner).getName();
      }
    }
    if (StringUtils.isBlank(dbName)) {
      dbName = session.getVarContext().getDefaultCatalog();
    }

    analyzeShow(new ShowColumns(dbName, tableName, false), SHOW_COLUMNS_COLUMNS, null, null, null, null);
    return false;
  }

  @Override
  public void endVisit(MySqlExplainStatement stmt) {
    if (stmt.isDescribe()) {
      return;
    }
    Operator child = getResultOperator();
    resultOperator = new Explain(child);
    List<ColumnExpr> columnExprs = new ArrayList<>(EXPLAIN_COLUMNS.length);
    for (int i = 0; i < EXPLAIN_COLUMNS.length; i++) {
      ColumnExpr columnExpr = new ColumnExpr(session.allocColumnID());
      columnExpr.setOriCol(EXPLAIN_COLUMNS[i]);
      columnExpr.setAliasCol(EXPLAIN_COLUMNS[i]);
      Metapb.SQLType sqlType;
      if (EXPLAIN_TYPES != null) {
        sqlType = Metapb.SQLType.newBuilder().setType(EXPLAIN_TYPES[i]).build();
      } else {
        sqlType = Metapb.SQLType.newBuilder().setType(Basepb.DataType.Varchar).build();
      }
      columnExpr.setResultType(sqlType);
      columnExprs.add(columnExpr);
    }
    resultOperator.setSchema(new Schema(columnExprs));
  }

  @Override
  public boolean visit(MySqlShowDatabasesStatement x) {
    analyzeShow(new ShowDatabases(), SHOW_DATABASE_COLUMNS, SHOW_DATABASE_ORC_COLUMNS, SHOW_DATABASE_TYPES, x.getLike(), x.getWhere());
    return false;
  }

  @Override
  public boolean visit(MySqlShowDatabasesInfoStatement x) {
    analyzeShow(new ShowDatabasesInfo(), SHOW_DATABASE_INFO_COLUMNS, null, SHOW_DATABASE_INFO_TYPES, x.getLike(), x.getWhere());
    return false;
  }

  @Override
  public boolean visit(MySqlShowStatusStatement x) {
    analyzeShow(new ShowStatus(), SHOW_STATUS_COLUMNS, null, SHOW_STATUS_TYPES, null, null);
    return false;
  }

  @Override
  public boolean visit(SQLShowTablesStatement stmt) {
    String dbName = null;
    if (stmt.getDatabase() != null) {
      dbName = stmt.getDatabase().getSimpleName();
    }

    if (StringUtil.isBlank(dbName)) {
      dbName = session.getVarContext().getDefaultCatalog();
    }
    analyzeShow(new ShowTables(dbName), new String[]{ SHOW_TABLE_COLUMNS[0] + dbName }, null, SHOW_TABLE_TYPES, null, null);
    return false;
  }

  @Override
  public boolean visit(SQLShowTablesInfoStatement stmt) {
    String dbName = null;
    if (stmt.getDatabase() != null) {
      dbName = stmt.getDatabase().getSimpleName();
    }

    if (StringUtil.isBlank(dbName)) {
      dbName = session.getVarContext().getDefaultCatalog();
    }
    analyzeShow(new ShowTablesInfo(dbName), new String[]{ SHOW_TABLE_INFO_COLUMNS[0], SHOW_TABLE_INFO_COLUMNS[1], SHOW_TABLE_INFO_COLUMNS[2] + dbName }, null, SHOW_TABLE_INFO_TYPES, null, null);
    return false;
  }

  @Override
  public boolean visit(MySqlAnalyzeStatement stmt) {
    List<TableSource> tableSourceList = new ArrayList<>(stmt.getTableSources().size());
    for (SQLExprTableSource tableSource : stmt.getTableSources()) {
      tableSourceList.add(analyzeExprTableSource(tableSource, AnalyzerType.SELECT));
    }
    this.resultOperator = new Analyze(tableSourceList);
    return false;
  }

  @Override
  public boolean visit(SQLCreateDatabaseStatement stmt) {
    this.resultOperator = DDLAnalyzer.analyzeCreateDatabase(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLDropDatabaseStatement stmt) {
    this.resultOperator = DDLAnalyzer.analyzeDropDatabase(session, stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlCreateTableStatement stmt) {
    SQLExpr table = stmt.getTableSource().getExpr();
    if (!(table instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
      table = new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), table.toString());
      stmt.getTableSource().setExpr(table);
    }

    this.resultOperator = DDLAnalyzer.analyzeCreateTable(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLDropTableStatement stmt) {
    List<SQLExprTableSource> tableSources = stmt.getTableSources();
    for (SQLExprTableSource tableSource : tableSources) {
      SQLExpr table = tableSource.getExpr();
      if (!(table instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
        table = new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), table.toString());
        tableSource.setExpr(table);
      }
    }
    this.resultOperator = DDLAnalyzer.analyzeDropTable(session, stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlRenameTableStatement stmt) {
    for (MySqlRenameTableStatement.Item item : stmt.getItems()) {
      SQLExpr from = item.getName();
      SQLExpr to = item.getTo();
      if (!(from instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
        item.setName(new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), from.toString()));
      }
      if (!(to instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
        item.setTo(new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), to.toString()));
      }
    }

    this.resultOperator = DDLAnalyzer.analyzeRenameTable(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLCreateIndexStatement stmt) {
    SQLExpr table = ((SQLExprTableSource) stmt.getTable()).getExpr();
    if (!(table instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
      table = new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), table.toString());
      stmt.setTable((SQLPropertyExpr) table);
    }

    this.resultOperator = DDLAnalyzer.analyzeCreateIndex(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLDropIndexStatement stmt) {
    SQLExpr table = stmt.getTableName().getExpr();
    if (!(table instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
      table = new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), table.toString());
      stmt.getTableName().setExpr(table);
    }
    this.resultOperator = DDLAnalyzer.analyzeDropIndex(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLAlterTableStatement stmt) {
    SQLExpr table = stmt.getTableSource().getExpr();
    if (!(table instanceof SQLPropertyExpr) && StringUtils.isNotBlank(session.getVarContext().getDefaultCatalog())) {
      table = new SQLPropertyExpr(new SQLIdentifierExpr(session.getVarContext().getDefaultCatalog()), table.toString());
      stmt.getTableSource().setExpr(table);
    }

    this.resultOperator = DDLAnalyzer.analyzeAlterTable(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLGrantStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeGrant(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLRevokeStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeRevoke(session, stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlFlushStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeFlush(stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlCreateUserStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeCreateUser(session, stmt);
    return false;
  }

  @Override
  public boolean visit(SQLDropUserStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeDropUser(session, stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlAlterUserStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeAlterUser(session, stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlCreateUserStatement.UserSpecification userSpecification) {
    return false;
  }

  @Override
  public boolean visit(MySqlShowGrantsStatement stmt) {
    this.resultOperator = PrivilegeAnalyzer.analyzeShowGrants(stmt);
    return false;
  }

  @Override
  public boolean visit(MySqlShowIndexesStatement stmt) {
    SQLName database = stmt.getDatabase();
    String dbName;
    if (database == null) {
      dbName = session.getVarContext().getDefaultCatalog();
    } else {
      dbName = database.getSimpleName();
      if (StringUtil.isBlank(dbName)) {
        dbName = session.getVarContext().getDefaultCatalog();
      }
    }
    analyzeShow(new ShowIndex(dbName, stmt.getTable().getSimpleName()), SHOW_INDEX_COLUMNS, null, SHOW_INDEX_TYPES,
            null, null);
    return false;
  }

  @Override
  public boolean visit(MySqlShowColumnsStatement stmt) {
    String dbName = null;
    String tableName = null;
    if (stmt.getDatabase() != null) {
      dbName = stmt.getDatabase().getSimpleName();
    }
    if (StringUtils.isBlank(dbName)) {
      dbName = session.getVarContext().getDefaultCatalog();
    }
    if (stmt.getTable() != null) {
      tableName = stmt.getTable().getSimpleName();
    }

    analyzeShow(new ShowColumns(dbName, tableName, stmt.isFull()), stmt.isFull() ? SHOW_FULL_COLUMNS_COLUMNS : SHOW_COLUMNS_COLUMNS,

            null, null, stmt.getLike(), stmt.getWhere());

    return false;
  }

  @Override
  public boolean visit(MySqlShowCreateTableStatement stmt) {
    String dbName = null;
    String tableName = null;
    SQLName table = stmt.getName();
    if (table instanceof SQLIdentifierExpr) {
      tableName = ((SQLIdentifierExpr) table).getName();
    } else if (table instanceof SQLPropertyExpr) {
      tableName = ((SQLPropertyExpr) table).getName();
      SQLExpr owner = ((SQLPropertyExpr) table).getOwner();
      if (owner instanceof SQLIdentifierExpr) {
        dbName = ((SQLIdentifierExpr) owner).getName();
      }
    }
    if (StringUtils.isBlank(dbName)) {
      dbName = session.getVarContext().getDefaultCatalog();
    }

    analyzeShow(new ShowCreateTable(dbName, tableName), SHOW_CREATE_TABLE_COLUMNS, null, null, null, null);
    return false;
  }

  @Override
  public boolean visit(SQLCommitStatement x) {
    this.resultOperator = Commit.getInstance();
    return true;
  }

  @Override
  public boolean visit(SQLValuesQuery x) {
    return false;
  }

  @Override
  public void endVisit(SQLValuesQuery x) {

  }

  @Override
  public boolean visit(SQLDataTypeRefExpr x) {
    return false;
  }

  @Override
  public void endVisit(SQLDataTypeRefExpr x) {

  }

  @Override
  public boolean visit(SQLTableSampling x) {
    return false;
  }

  @Override
  public void endVisit(SQLTableSampling x) {

  }

  @Override
  public boolean visit(SQLSizeExpr x) {
    return false;
  }

  @Override
  public void endVisit(SQLSizeExpr x) {

  }

  @Override
  public boolean visit(SQLUnnestTableSource x) {
    return false;
  }

  @Override
  public void endVisit(SQLUnnestTableSource x) {

  }

  @Override
  public boolean visit(SQLAdhocTableSource x) {
    return false;
  }

  @Override
  public void endVisit(SQLAdhocTableSource x) {

  }

  @Override
  public boolean visit(SQLCurrentTimeExpr x) {
    return false;
  }

  @Override
  public void endVisit(SQLCurrentTimeExpr x) {

  }

  @Override
  public boolean visit(SQLDecimalExpr x) {
    return false;
  }

  @Override
  public void endVisit(SQLDecimalExpr x) {

  }

  @Override
  public boolean visit(SQLCurrentUserExpr x) {
    return false;
  }

  @Override
  public void endVisit(SQLCurrentUserExpr x) {

  }

  @Override
  public boolean visit(SQLRollbackStatement x) {
    this.resultOperator = Rollback.getInstance();
    return true;
  }


}
