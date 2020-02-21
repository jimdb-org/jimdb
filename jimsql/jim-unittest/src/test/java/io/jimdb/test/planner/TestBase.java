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

package io.jimdb.test.planner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.Bootstraps;
import io.jimdb.core.Session;
import io.jimdb.core.config.JimConfig;
import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.KeyColumn;
import io.jimdb.core.expression.KeyValueRange;
import io.jimdb.core.expression.TableAccessPath;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.expression.ValueRange;
import io.jimdb.core.model.meta.Column;
import io.jimdb.core.model.meta.Index;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.core.model.result.impl.DMLExecResult;
import io.jimdb.core.model.result.impl.QueryExecResult;
import io.jimdb.core.plugin.PluginFactory;
import io.jimdb.core.plugin.SQLEngine;
import io.jimdb.core.plugin.SQLExecutor;
import io.jimdb.core.plugin.store.Engine;
import io.jimdb.core.plugin.store.Transaction;
import io.jimdb.core.values.Value;
import io.jimdb.pb.Processorpb;
import io.jimdb.sql.Planner;
import io.jimdb.sql.operator.Delete;
import io.jimdb.sql.operator.IndexLookup;
import io.jimdb.sql.operator.IndexSource;
import io.jimdb.sql.operator.Insert;
import io.jimdb.sql.operator.KeyGet;
import io.jimdb.sql.operator.Operator;
import io.jimdb.sql.operator.OperatorUtil;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.sql.operator.TableSource;
import io.jimdb.sql.operator.Update;
import io.jimdb.sql.optimizer.OptimizeFlag;
import io.jimdb.sql.optimizer.logical.LogicalOptimizer;
import io.jimdb.sql.optimizer.statistics.TableStatsManager;
import io.jimdb.test.TestUtil;
import io.jimdb.test.mock.meta.MockMeta;
import io.jimdb.test.mock.meta.MockMetaStore;
import io.jimdb.test.mock.store.MockStoreEngine;
import io.jimdb.test.mock.store.MockTableData;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.fastjson.JSON;
import com.zaxxer.hikari.HikariDataSource;

import reactor.core.publisher.Flux;

public class TestBase {
  private static final int optimizerFlag = OptimizeFlag.PRUNCOLUMNS |
          OptimizeFlag.BUILDKEYINFO | OptimizeFlag.ELIMINATEPROJECTION |
          OptimizeFlag.PREDICATEPUSHDOWN | OptimizeFlag.PUSHDOWNTOPN;
  protected static final Logger LOG = LoggerFactory.getLogger(TestBase.class);
  protected static Planner planner;
  protected static Session session;
  protected static final String CATALOG = "test";
  protected static final String USER_TABLE = "user";
  protected static JimConfig config;
  protected static MockMeta mockMeta = new MockMetaStore();
  protected static HikariDataSource dataSource;
  private static MockStoreEngine storeEngine;
  private static CountDownLatch latch = new CountDownLatch(1);
  private static SQLExecutor sqlExecutor;

  @BeforeClass
  public static void setup() throws Exception {
    if (config == null) {
      config = Bootstraps.init("jim_test.properties");
      mockMeta.init();
      storeEngine = new MockStoreEngine();
      sqlExecutor = PluginFactory.getSqlExecutor();
      session = new MockSession(PluginFactory.getSqlEngine(), storeEngine);
      session.getVarContext().setDefaultCatalog(CATALOG);
      session.getVarContext().setAutocommit(false);

      //final Session session = new Session(config.getSQLEngine(), storeEngine);
      TableStatsManager.init(config);
      planner = new Planner(SQLEngine.DBType.MYSQL);
    }
  }

  @AfterClass
  public static void resetMockMeta() {
    mockMeta = new MockMetaStore();
  }

  protected static void setMockMeta(MockMeta meta) {
    mockMeta = meta;
    config = null;
  }

  protected static List<String> collect(ResultSet resultSet) throws SQLException {
    final List<String> result = new ArrayList<>();
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int count = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= count; i++) {
        buf.append(sep)
                .append(resultSet.getMetaData().getColumnLabel(i))
                .append("=")
                .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(TestUtil.linuxString(buf.toString()));
    }
    return result;
  }

  protected static List<String> execQuery(String sql) {
    List<String> result = new ArrayList<>();
    execQuery(sql, resultSet -> {
      try {
        result.addAll(collect(resultSet));
        //Assert.assertEquals(expected, actual);
      } catch (Exception e) {
        TestUtil.rethrow(e);
      }
    });

    return result;
  }

  protected static void execQuery(String sql, Consumer<ResultSet> expect) {
    Connection conn = null;
    Statement stmt = null;
    ResultSet resultSet = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(true);
      stmt = conn.createStatement();
      resultSet = stmt.executeQuery(sql);
      if (expect != null) {
        expect.accept(resultSet);
      }
    } catch (Exception e) {
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, resultSet);
    }
  }

  protected static void execUpdate(String sql, int result, boolean autocommit) {
    execUpdate(new String[]{ sql }, new int[]{ result }, autocommit);
  }

  protected static void execUpdate(String[] sqls, int[] result, boolean autocommit) {
    Connection conn = null;
    Statement stmt = null;
    try {
      conn = dataSource.getConnection();
      conn.setAutoCommit(autocommit);
      stmt = conn.createStatement();
      for (String sql : sqls) {
        stmt.addBatch(sql);
      }
      int[] rs = stmt.executeBatch();
      if (!autocommit) {
        conn.commit();
      }
      Assert.assertArrayEquals(result, rs);
    } catch (Exception e) {
      if (!autocommit) {
        try {
          conn.rollback();
        } catch (Exception e1) {
          TestUtil.rethrow(e1);
        }
      }
      TestUtil.rethrow(e);
    } finally {
      closeQuit(conn, stmt, null);
    }
  }

  protected static void closeQuit(final Connection conn, final Statement stmt, final ResultSet rs) {
    try {
      if (rs != null) {
        rs.close();
      }
      if (stmt != null) {
        stmt.close();
      }
      if (conn != null) {
        conn.close();
      }
    } catch (SQLException e) {
    }
  }

  protected static String predicates2str(List<Expression> predicates, String prefix) {
    StringBuilder sb = new StringBuilder(prefix + "=[");
    for (Expression expression : predicates) {
      if (expression instanceof ValueExpr) {
        ValueExpr ve = (ValueExpr) expression;
        sb.append(ve.getExprType()).append('-').append(ve.getValue());
      } else if (expression instanceof ColumnExpr) {
        ColumnExpr ce = (ColumnExpr) expression;
        sb.append(ce.getExprType()).append('-').append(ce.getOriCol()).append(ce.getAliasCol());
      } else if (expression instanceof FuncExpr) {
        FuncExpr fe = (FuncExpr) expression;
        sb.append(fe.getExprType()).append('-').append(fe.getFunc().getName()).append('(');
        Expression[] args = fe.getArgs();
        for (Expression arg : args) {
          if (arg instanceof ValueExpr) {
            ValueExpr veArg = (ValueExpr) arg;
            sb.append(veArg.getValue().getString());
          } else if (arg instanceof ColumnExpr) {
            ColumnExpr ceArg = (ColumnExpr) arg;
            sb.append(ceArg.getOriCol());
          }
          sb.append(",");
        }
        sb.deleteCharAt(sb.length() - 1);
        sb.append(")");
      }
      sb.append(",");
    }
    if (!predicates.isEmpty()) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("]");

    return sb.toString();
  }

  protected static String columns2str(Column[] columns, String prefix) {
    StringBuilder sb = new StringBuilder(prefix + "=[");
    for (Column column : columns) {
      sb.append(column.getName());
      sb.append(",");
    }
    if (columns.length > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("]");

    return sb.toString();
  }

  protected static String keycolumns2str(List<KeyColumn> keyColumns, String prefix) {
    StringBuilder sb = new StringBuilder(prefix + "=[");
    for (KeyColumn column : keyColumns) {
      List<ColumnExpr> columnExprs = column.getColumnExprs();
      for (ColumnExpr columnExpr : columnExprs) {
        sb.append(columnExpr.getOriCol() + "-");
      }
      if (columnExprs.size() > 0) {
        sb.deleteCharAt(sb.length() - 1);
      }
      sb.append(",");
    }
    if (keyColumns.size() > 0) {
      sb.deleteCharAt(sb.length() - 1);
    }
    sb.append("]");

    return sb.toString();
  }

  public static void checkExecute(String sql) {
    sqlExecutor.executeQuery(session, sql);
  }

  protected List<Checker> builderTestCase() {
    return null;
  }

  protected RelOperator buildPlanTree(String sql) {
    return planAnalyze(sql, false);
  }

  protected RelOperator buildPlanAndOptimize(String sql) {
    return planAnalyze(sql, true);
  }

  protected RelOperator buildPlanAndLogicalOptimizeOnly(String sql) {
    RelOperator tree = planAnalyze(sql, false);
    Operator.OperatorType type = tree.getOperatorType();
    if (type == Operator.OperatorType.SIMPLE) {
      return tree;
    }
    return LogicalOptimizer.optimize(tree, optimizerFlag, session);
  }

  private RelOperator planAnalyze(String sql, boolean needOpt) {
    List<SQLStatement> parse = planner.parse(sql);
    SQLStatement stmt = parse.get(0);

    if (!needOpt) {
      return (RelOperator) planner.analyze(session, stmt);
    }

    return (RelOperator) this.planner.analyzeAndOptimize(session, stmt);
  }

  public enum CheckPoint {
    PlanTree, ColumnPrune, BuildKeyInfo, PredicatesPushDown, ProjectionEliminate, TopNPushDown, DetachConditions,
    ColumnRange, PushDownProcessors, ExecuteSql, ProcessorpbSelection, EvaluatorExp, IndexPlan,
    IndexLookup, TablePlan, PushDownTablePlan, PushDownIndexPlan
  }

  public static class Checker {
    private String sql;
    private Map<CheckPoint, String> checkPoints;
    private boolean hasException;
    private CheckMethod checkMethod;

    protected Checker() {
      checkPoints = new LinkedHashMap<>();
    }

    protected Checker(CheckMethod checkMethod) {
      this.checkPoints = new LinkedHashMap<>();
      this.checkMethod = checkMethod;
    }

    public static Checker build() {
      return new Checker();
    }

    public static Checker build(CheckMethod checkMethod) {
      return new Checker(checkMethod);
    }

    private static String parseExprList(List<io.jimdb.pb.Exprpb.Expr> exprList) {
      List<String> strings = new ArrayList<>();
      exprList.forEach(expr -> {
        strings.add(parseExpr(expr).trim());
      });
      return String.join(" AND ", strings);
    }

    private static String parseExpr(io.jimdb.pb.Exprpb.Expr expr) {

      String result = "";

      if (!expr.getChildList().isEmpty()) {
        result += parseExpr(expr.getChildList().get(0));
        result += expr.getExprType() + " ";
        result += parseExpr(expr.getChildList().get(1));
        return result;
      }

      Column[] columns = MetaData.Holder.get().getCatalog("test").getTable("user").getReadableColumns();

      switch (expr.getExprType()) {
        case Column:
          result = columns[expr.getColumn().getId() - 1].getName() + " ";
          break;
        case Const_Int:
          result = "value(" + expr.getValue().toStringUtf8() + ") ";
          break;
        case Const_Bytes:
          result = "value(" + expr.getValue().toStringUtf8() + ") ";
          break;
      }

      return result;
    }

    public Checker addCheckPoint(CheckPoint checkPoint, String expectedStr) {
      checkPoints.put(checkPoint, expectedStr);
      return this;
    }

    public Checker sql(String val) {
      sql = val;
      return this;
    }

    public Checker hasException(boolean hasException) {
      this.hasException = hasException;
      return this;
    }

    public String getSql() {
      return sql;
    }

    public void setSql(String sql) {
      this.sql = sql;
    }

    public Map<CheckPoint, String> getCheckPoints() {
      return checkPoints;
    }

    public void setCheckPoints(Map<CheckPoint, String> checkPoints) {
      this.checkPoints = checkPoints;
    }

    public boolean isHasException() {
      return hasException;
    }

    public void setHasException(boolean hasException) {
      this.hasException = hasException;
    }

    public void check() {
      checkMethod.apply(this);
    }

    public void reset() {
      this.sql = "";
      this.checkPoints.clear();
      this.hasException = false;
    }

    public void doCheck(RelOperator planTree) {
      checkPoints.forEach((checkPoint, expected) -> {
        RelOperator operator = planTree;
        switch (checkPoint) {
          case PlanTree:
            String treeAsStr = OperatorUtil.printRelOperatorTree(planTree);
//            System.out.println(this.getSql() + " : " + treeAsStr);
            Assert.assertEquals(String.format("sql[%s] optimized error", this.getSql()), expected, treeAsStr);
            break;
          case PredicatesPushDown:
            while (!(operator instanceof TableSource)) {
              operator = operator.getChildren()[0];
            }
            List<Expression> predsPushDown = ((TableSource) operator).getPushDownPredicates();
            String ppdStr = predicates2str(predsPushDown, "TableSource.pushDownPredicates");
            Assert.assertEquals(expected, ppdStr);
            break;
          case ColumnPrune:
            while (!(operator instanceof TableSource)) {
              operator = operator.getChildren()[0];
            }
            Column[] columns = ((TableSource) operator).getColumns();
            String columns2str = columns2str(columns, "TableSource.columns");
            Assert.assertEquals(expected, columns2str);
            break;
          case BuildKeyInfo:
            while (!(operator instanceof TableSource)) {
              operator = operator.getChildren()[0];
            }
            List<KeyColumn> keyColumns = operator.getSchema().getKeyColumns();
            String keycolumns2str = keycolumns2str(keyColumns, "TableSource.schema.keyColumns");
            Assert.assertEquals(expected, keycolumns2str);
            break;
          case ColumnRange:
            String str = buildRange(operator);
            Assert.assertEquals(String.format("ColumnRange error . sql : %s ", this.getSql()), expected, str);
            break;
          case DetachConditions:
            if (planTree instanceof KeyGet) {
              final StringBuilder actual = new StringBuilder("{");
              KeyGet keyGet = (KeyGet) planTree;
              List<Index> indices = keyGet.getIndices();
              List<Value[]> values = keyGet.getValues();
              for (int i = 0; i < indices.size(); i++) {
                actual.append(indices.get(i).getName() + ":[");
                Arrays.asList(values.get(i)).stream().forEach(x -> actual.append(x.getString()));
                actual.append("]");
              }
              actual.append("}");
              Assert.assertEquals(expected, actual.toString());
              return;
            }

            while (true) {
              if (operator instanceof IndexLookup) {
                // Indexlookup find indexSource
                operator = ((IndexLookup) operator).getPushedDownIndexPlan();
                break;
              } else if (operator instanceof IndexSource || operator instanceof TableSource) {
                break;
              } else {
                if (!operator.hasChildren()) {
                  Assert.assertEquals(String.format("DetachConditions error . sql : %s ", this.getSql()), expected,
                          "null");
                  return;
                }
                operator = operator.getChildren()[0];
              }
            }

            if (operator instanceof TableSource) {
              List<TableAccessPath> tableAccessPaths = ((TableSource) operator).getTableAccessPaths();
              Map<String, String> actualMap = new LinkedHashMap<>();
              for (TableAccessPath path : tableAccessPaths) {
                if (path.getAccessConditions() == null || path.getAccessConditions().isEmpty()) {
                  continue;
                }

                actualMap.put(path.getIndex().getName(), String.valueOf(path.getAccessConditions()));
              }

              Assert.assertEquals(String.format("DetachConditions error . sql : %s ", this.getSql()), expected,
                      JSON.toJSONString(actualMap));
            }

            if (operator instanceof IndexSource) {
              IndexSource indexSource = ((IndexSource) operator);
              Map<String, String> actualMap = new LinkedHashMap<>();
              actualMap.put(indexSource.getKeyValueRange().getIndex().getName(), String.valueOf(indexSource.getAccessConditions()));
              Assert.assertEquals(String.format("DetachConditions error . sql : %s ", this.getSql()), expected,
                      JSON.toJSONString(actualMap));
            }

            break;
          case PushDownProcessors:
            while (operator.hasChildren()) {
              operator = operator.getChildren()[0];
            }

            List<String> actualList = new ArrayList<>();
            if (operator instanceof IndexLookup) {
              IndexLookup indexLookup = (IndexLookup) operator;
              List<Processorpb.Processor.Builder> tablePlanProcessors = indexLookup.getTablePlanProcessors();
              List<Processorpb.Processor.Builder> indexPlanProcessors = indexLookup.getIndexPlanProcessors();

              List<String> indexPlanProcessorsStrList = new ArrayList<>();
              indexPlanProcessors.forEach(x -> indexPlanProcessorsStrList.add(x.getType().name()));
              String indexPlanProcessorsStr = "indexPlanProcessors : [" + String.join(" -> ",
                      indexPlanProcessorsStrList) + "]";
              actualList.add(indexPlanProcessorsStr);

              List<String> tablePlanProcessorsStrList = new ArrayList<>();
              tablePlanProcessors.forEach(x -> tablePlanProcessorsStrList.add(x.getType().name()));
              String tablePlanProcessorsStr = "tablePlanProcessors : [" + String.join(" -> ",
                      tablePlanProcessorsStrList) + "]";
              actualList.add(tablePlanProcessorsStr);
            } else if (operator instanceof TableSource) {
              TableSource tableSource = (TableSource) operator;
              List<Processorpb.Processor.Builder> processors = tableSource.getProcessors();
              processors.forEach(x -> actualList.add(x.getType().name()));
            } else if (operator instanceof IndexSource) {
              IndexSource indexSource = (IndexSource) operator;
              List<Processorpb.Processor.Builder> processors = indexSource.getProcessors();
              processors.forEach(x -> actualList.add(x.getType().name()));
            }
            String actualStr = String.join(" -> ", actualList);
            Assert.assertEquals(String.format("PushDownProcessors error . sql : %s ", this.getSql()), expected,
                    actualStr);
            break;
          case ExecuteSql:
            QueryExecResult rs = MockSession.getQueryResult();
            Object obj = getRows(rs);
            Assert.assertEquals(expected, obj);
            break;
          case IndexLookup:
            while (!(operator instanceof IndexLookup)) {
              operator = operator.getChildren()[0];
            }
            IndexLookup indexLookup = (IndexLookup) operator;
            Assert.assertEquals(expected, OperatorUtil.printRelOperatorTree(indexLookup));
            break;
          case IndexPlan:
            while (!(operator instanceof IndexSource)) {
              operator = operator.getChildren()[0];
            }
            IndexSource indexSource = (IndexSource) operator;
            String indexSourceStr = OperatorUtil.printRelOperatorTree(indexSource);
            indexSourceStr = indexSourceStr.substring(0, indexSourceStr.length() - 1)
                    + "." + indexSource.getExplainInfo() + ")";
            Assert.assertEquals(expected, indexSourceStr);
            break;
          case TablePlan:
            while (!(operator instanceof TableSource)) {
              operator = operator.getChildren()[0];
            }
            TableSource tableSource = (TableSource) operator;
            Assert.assertEquals(expected, tableSource.toString());
            break;
          case PushDownTablePlan:
            while (!(operator instanceof TableSource)) {
              operator = operator.getChildren()[0];
            }
            RelOperator pushDownTablePlan = ((TableSource) operator).getPushDownTablePlan();
            String pdtp = OperatorUtil.printRelOperatorTree(pushDownTablePlan);
            Assert.assertEquals(expected, pdtp);
            break;
          case PushDownIndexPlan:
            while (!(operator instanceof IndexLookup)) {
              operator = operator.getChildren()[0];
            }
            IndexLookup indexLookup1 = (IndexLookup) operator;
            IndexSource indexSource1 = (IndexSource) indexLookup1.getPushedDownIndexPlan();
            String indexSourceStr1 = OperatorUtil.printRelOperatorTree(indexSource1);
            indexSourceStr = indexSourceStr1.substring(0, indexSourceStr1.length() - 1)
                    + "." + indexSource1.getExplainInfo() + ")";
            Assert.assertEquals(expected, indexSourceStr);
            break;
        }
      });
    }

    protected String buildRange(Operator operator) {
      //pk range
      if (operator instanceof KeyGet) {
        // don't check
        return "KeyGet";
      }

      KeyValueRange keyValueRange = null;

      while (true) {
        if (operator instanceof IndexSource) {
          keyValueRange = ((IndexSource) operator).getKeyValueRange();
          break;
        }

        if (operator instanceof TableSource) {
          keyValueRange = ((TableSource) operator).getKeyValueRange();
          break;
        }

        if (operator instanceof IndexLookup) {
          operator = ((IndexLookup) operator).getIndexSource();
          keyValueRange = ((IndexSource) operator).getKeyValueRange();
          break;
        }

        if (operator instanceof Update) {
          operator = ((Update) operator).getSelect();
          continue;
        }

        if (operator instanceof Delete) {
          operator = ((Delete) operator).getSelect();
          continue;
        }

        if (operator instanceof Insert) {
          operator = ((Insert) operator).getSelect();
          continue;
        }

        if (operator instanceof RelOperator) {

          if (!((RelOperator) operator).hasChildren()) {
            return "";
          }

          operator = ((RelOperator) operator).getChildren()[0];
        }
      }
      return getRange(keyValueRange);
    }

    private String getProcessorsSelectionFiltersString(List<Processorpb.Processor> processors) {
      StringBuilder result = new StringBuilder();
      processors.forEach(processor -> {
        if (processor.getSelection() != null) {
          Processorpb.Selection selection = processor.getSelection();
          java.util.List<io.jimdb.pb.Exprpb.Expr> exprList = selection.getFilterList();
          if (exprList != null && !exprList.isEmpty()) {
            result.append(parseExprList(exprList));
          }
        }
      });
      return result.toString();
    }

    private String getRows(QueryExecResult rs) {
      ColumnExpr[] columnExprs = rs.getColumns();
      List<String> cols = new ArrayList<>();
      for (ColumnExpr columnExpr : columnExprs) {
        cols.add(columnExpr.getOriCol());
      }
      List<String> list = new ArrayList<>();
      rs.forEach(row -> {
        MockTableData.User user = new MockTableData.User();
        if (cols.contains("name")) {
          user.setName(row.get(0).getString());
        }
        if (cols.contains("age")) {
          user.setAge(Integer.valueOf(row.get(1).getString()));
        }
        if (cols.contains("phone")) {
          user.setPhone(row.get(2).getString());
        }
        if (cols.contains("score")) {
          user.setScore(Integer.valueOf(row.get(3).getString()));
        }
        if (cols.contains("salary")) {
          user.setSalary(Integer.valueOf(row.get(4).getString()));
        }
        list.add(user.toString());
      });
      return list.toString();
    }

    private String getRange(KeyValueRange keyValueRange) {
      if (null == keyValueRange) {
        return "null";
      }
      List<ValueRange> valueRanges = keyValueRange.getValueRanges();
      String actualStr = "[";
      actualStr += valueRanges.stream().map(valueRange -> {
        List<Value> starts = valueRange.getStarts();
        List<Value> ends = valueRange.getEnds();

        //todo
        StringBuilder sb = new StringBuilder("{[");
        for (int i = 0; i < starts.size(); i++) {
          String start = starts.get(i) == Value.MIN_VALUE ? "MIN_VALUE" : starts.get(i).getString();
          String end = ends.get(i) == Value.MAX_VALUE ? "MAX_VALUE" : ends.get(i).getString();
          sb.append("{start:" + start + ",end:" + end + "},");
        }
        sb.setLength(sb.length() - 1);
        sb.append("]");
        sb.append(",startInclusive:" + valueRange.isStartInclusive() + ",endInclusive:" + valueRange.isEndInclusive() + "}");
        return sb.toString();
      }).collect(Collectors.joining(",")) + "]";

      return actualStr;
    }

    @FunctionalInterface
    public interface CheckMethod {
      void apply(Checker checker);
    }
  }

  /**
   * mock sql session
   */
  public static class MockSession extends Session {

    private static QueryExecResult queryExecResult;

    public MockSession(final SQLEngine sqlEngine, final Engine storeEngine) {
      super(sqlEngine, storeEngine);
    }

    public static QueryExecResult getQueryResult() {
      return queryExecResult;
    }

    @Override
    public void write(ExecResult rs, boolean isEof) throws JimException {
      this.queryExecResult = null;
      if (rs != null) {
        if (rs instanceof DMLExecResult) {
          System.out.println("dml result" + rs.getAffectedRows());
          //select no need commit
          Transaction txn = getTxn();
          Flux<ExecResult> flux = txn.commit();
          flux.subscribe(m -> {
            System.out.println("txn commit result ok");
            latch.countDown();
          }, e -> {
            System.out.println("error" + e.getMessage());
            latch.countDown();
          });
        } else if (rs instanceof QueryExecResult) {

          try {
            this.queryExecResult = (QueryExecResult) rs;
          } catch (Exception e) {
            e.printStackTrace();
          } finally {
            latch.countDown();
          }
        }
      }
    }

    @Override
    public void writeError(JimException ex) throws JimException {
      System.out.println("error:" + ex.getMessage());
      latch.countDown();
    }
  }
}
