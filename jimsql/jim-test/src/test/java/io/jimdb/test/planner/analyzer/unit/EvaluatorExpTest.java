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
package io.jimdb.test.planner.analyzer.unit;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import io.jimdb.common.exception.JimException;
import io.jimdb.core.expression.Expression;
import io.jimdb.core.expression.FuncExpr;
import io.jimdb.core.expression.ValueExpr;
import io.jimdb.core.values.DecimalValue;
import io.jimdb.core.values.DoubleValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.sql.operator.Projection;
import io.jimdb.sql.operator.RelOperator;
import io.jimdb.test.planner.TestBase;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class EvaluatorExpTest extends TestBase {

  @Test
  public void testEval() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.addAll(builderTestCase());
    checkerList.addAll(varPlanTestCase());
    checkerList.addAll(varExprTestCase());
    checkerList.addAll(arithmeticConTestCase());
    checkerList.addAll(arithmeticVarTestCase());

    checkerList.forEach(checker -> {
      try {
        RelOperator relOperator = buildPlanTree(checker.getSql());
        Assert.assertNotNull(relOperator);
        LOG.debug("The original SQL is ({})", checker.getSql());
        checker.doCheck(relOperator);
      } catch (JimException e) {
        LOG.error("cause error ,checker sql {}", checker.getSql(), e);
        Assert.assertTrue(checker.isHasException());
      }
    });
  }

  @Test
  public void testDebugEval() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age !='1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(NotEqualReal(CastIntToReal(test.user"
                    + ".age),1.1)) -> Projection"));
    checkerList.forEach(checker -> {
      try {
        RelOperator relOperator = buildPlanTree(checker.getSql());
        Assert.assertNotNull(relOperator);
        LOG.info("The original SQL is ({})", checker.getSql());
        checker.doCheck(relOperator);
      } catch (JimException e) {
        LOG.error("cause error", e);
        Assert.assertTrue(checker.isHasException());
      }
    });
  }

  public List<Checker> arithmeticVarTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age+1 from user ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "PlusInt(test.user.age,1)"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age+1.0 from user ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "PlusDecimal(CastIntToDecimal(test.user.age),1.0)"));
    return checkerList;
  }

  public List<Checker> arithmeticConTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 + 1 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 - 1 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 2 * 3 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "6"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 3.5 / 0.5 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.0000"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 4 / 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.0000"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 2.0 / 1.0 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.00000"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '100' / 3 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "33.333333333333336"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1' / 3 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0.3333333333333333"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '8' / 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "4"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 4 div 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 4 mod 3 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1.2 - 1 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0.2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 2.2 * 3 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "6.6"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 4.2 / 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 5 div 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 5.0 / 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.5"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 5.0 div 2 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 5.0 div 1.5 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "3"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '5.0' div 1.5 ")
            .addCheckPoint(CheckPoint.EvaluatorExp, "3"));
    return checkerList;
  }

  public List<Checker> varExprTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age !=1.1 from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "NotEqualDecimal(CastIntToDecimal(test.user.age),1.1)"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age <=1.1 from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "LessOrEqualInt(test.user.age,1)"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 123456789123456789123456789.12345 > 11 from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 123456789123456789.12345 = age from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 123456789123456789123456789.12345 > 11 from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select -123456789123456789123456789.12345 > age from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 123456789123456789123456789.12345 < age from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));

    checkerList.add(EvaluatorExpChecker.build()
            .sql("select -123456789123456789123456789.12345 < age from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    return checkerList;
  }

  public List<Checker> varPlanTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age = '123456789123456711111189'")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age = 123456789123456711111189")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age > 123456789123456711111189")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age > '123456789123456711111189'")
            .addCheckPoint(CheckPoint.PlanTree, "Dual -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age !=1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(NotEqualDecimal(CastIntToDecimal(test"
                    + ".user.age),1.1)) -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age !='1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(NotEqualReal(CastIntToReal(test.user"
                    + ".age),1.1)) -> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age <'1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(LessInt(test.user.age,2)) -> "
                    + "Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age <1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(LessInt(test.user.age,2)) -> "
                    + "Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age >=1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterOrEqualInt(test.user.age,2)) "
                    + "-> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age >='1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterOrEqualInt(test.user.age,2)) "
                    + "-> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age >'1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age >1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age <='1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(LessOrEqualInt(test.user.age,1)) -> "
                    + "Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where age <=1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(LessOrEqualInt(test.user.age,1)) -> "
                    + "Projection"));

    //decimal
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where scale > 1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterDecimal(test.user.scale,1)) ->"
                    + " Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where scale > 1.1")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterDecimal(test.user.scale,1.1)) "
                    + "-> Projection"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select age from user where scale > '1.1'")
            .addCheckPoint(CheckPoint.PlanTree, "TableSource(user) -> Selection(GreaterDecimal(test.user.scale,1.1)) "
                    + "-> Projection"));
    return checkerList;
  }

  @Override
  protected List<Checker> builderTestCase() {
    List<Checker> checkerList = new ArrayList<>();
    // add
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1+1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'+1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1.1 +1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1.1' +1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2.1"));
    //eq or not
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1=1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1!=1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    // XOR
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 3 ^ 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "2"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 3 ^ 1 ^ 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "3"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 ^ 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 ^ 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    // greater or less
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 > 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 0 > 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 >= 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 0 >= 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 0 >= 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 0 < 1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 < 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 <= 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 0 <= 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 1 <= 0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));

    //cast
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'=1")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'>0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'>2")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'>=2")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'<0")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'<2")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1'<=2")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select '1.1'<=2")
            .addCheckPoint(CheckPoint.EvaluatorExp, "1"));
    checkerList.add(EvaluatorExpChecker.build()
            .sql("select 'a' !='a' from user")
            .addCheckPoint(CheckPoint.EvaluatorExp, "0"));

    return checkerList;
  }

  private static class EvaluatorExpChecker extends Checker {

    public EvaluatorExpChecker() {
    }

    public static Checker build() {
      return new EvaluatorExpChecker();
    }

    @Override
    public void doCheck(RelOperator planTree) {
      getCheckPoints().forEach(((checkPoint, expected) -> {
        RelOperator operator = planTree;
        switch (checkPoint) {
          case EvaluatorExp:
            Expression exp = ((Projection) operator).getExpressions()[0];
            if (exp instanceof ValueExpr) {
              ValueExpr valueExpr = (ValueExpr) exp;
              if (valueExpr.getValue() instanceof LongValue) {
                LongValue value = (LongValue) valueExpr.getValue();
                System.out.println((value.getValue()));
                Assert.assertTrue(value.getValue() == Long.valueOf(expected));
                break;
              }
              if (valueExpr.getValue() instanceof DoubleValue) {
                DoubleValue value = (DoubleValue) valueExpr.getValue();
                System.out.println((value.getValue()));
                Assert.assertTrue(value.getValue() == Double.valueOf(expected));
                break;
              }
              if (valueExpr.getValue() instanceof DecimalValue) {
                DecimalValue value = (DecimalValue) valueExpr.getValue();
                System.out.println((value.getValue()));
                Assert.assertTrue(value.getValue().compareTo(BigDecimal.valueOf(Double.valueOf(expected))) == 0);
                break;
              }
            }
            if (exp instanceof FuncExpr) {
              LOG.info("expected: " + expected + " result:" + exp.toString());
              Assert.assertTrue(expected.equals(exp.toString()));
              break;
            }
            Assert.assertTrue("not compare exp", false);
            break;
          case PlanTree:
            super.doCheck(planTree);
            break;
          default:
            throw new RuntimeException("no checker point");
        }
      }));
    }
  }

  /**
   * ExprMockMeta
   */
//  public static class MockMetaEngine4Expr implements MetaEngine {
//    private MetaData metaData;
//
//    @Override
//    public MetaData getMetaData() {
//      return this.metaData;
//    }
//
//    @Override
//    public CompletableFuture<MetaData> sync(int version) {
//      final CompletableFuture<MetaData> completableFuture = new CompletableFuture<>();
//      completableFuture.complete(this.metaData);
//      return completableFuture;
//    }
//
//    @Override
//    public String getName() {
//      return "mockMeta";
//    }
//
//    @Override
//    public void init(JimConfig c) {
//      this.metaData = new MockMetaEngine4Expr.MockMetaData();
//    }
//
//    @Override
//    public void close() {
//    }
//
//    static final class MockMetaData implements MetaData {
//      private final Map<String, JimDatabase> catalogs = new HashMap<>();
//
//      MockMetaData() {
//        initCatalogs();
//      }
//
//      @Override
//      public int getVersion() {
//        return 1;
//      }
//
//      @Override
//      public JimDatabase getCatalog(String name) {
//        return catalogs.get(name);
//      }
//
//      @Override
//      public Table getTable(String catalog, String table) {
//        return catalogs.get(catalog).getTable(table);
//      }
//
//      @Override
//      public Collection<? extends Catalog> getAllCatalogs() {
//        return catalogs.values();
//      }
//
//      private void initCatalogs() {
//        JimDatabase database = new JimDatabase();
//        database.setId(1);
//        database.setName("test");
//        database.setVersion(1);
//        database.setJimTables(initTables());
//        catalogs.put("test", database);
//      }
//
//      private JimTable[] initTables() {
//        JimTable table = new JimTable();
//        table.setId(1);
//        table.setDbId(1);
//        table.setName("user");
//
//        // table columns: name(Varchar), age(Int), phone(Varchar), score(Int), salary(Int)
//        JimColumn[] columns = new JimColumn[7];
//        JimColumn column1 = new JimColumn();
//        column1.setId(1L);
//        column1.setName("name");
//        // key != 0 means primaryKey
//        column1.setPrimaryKey(1);
//        column1.setDataType(Basepb.DataType.Varchar);
//        columns[0] = column1;
//        table.setColumns(columns);
//
//        JimColumn column2 = new JimColumn();
//        column2.setId(2L);
//        column2.setName("age");
//        column2.setDataType(Basepb.DataType.Int);
//        column2.setNullable(false);
//        columns[1] = column2;
//
//        JimColumn column3 = new JimColumn();
//        column3.setId(3L);
//        column3.setName("phone");
//        column3.setDataType(Basepb.DataType.Varchar);
//        column3.setNullable(false);
//        columns[2] = column3;
//
//        JimColumn column4 = new JimColumn();
//        column4.setId(4L);
//        column4.setName("score");
//        column4.setDataType(Basepb.DataType.Int);
//        columns[3] = column4;
//
//        JimColumn column5 = new JimColumn();
//        column5.setId(5L);
//        column5.setName("salary");
//        column5.setDataType(Basepb.DataType.Int);
//        columns[4] = column5;
//        table.setColumns(columns);
//
//        JimColumn column6 = new JimColumn();
//        column6.setId(6L);
//        column6.setName("scale");
//        column6.setDataType(Basepb.DataType.Decimal);
//        columns[5] = column6;
//
//        JimColumn column7 = new JimColumn();
//        column7.setId(7L);
//        column7.setName("pas");
//        column7.setDataType(Basepb.DataType.Double);
//        columns[6] = column7;
//        table.setColumns(columns);
//
//        // table indices: idx_age, idx_phone, idx_age_phone
//        JimIndex[] indices = new JimIndex[3];
//        JimIndex jimIndex = new JimIndex();
//        jimIndex.setId(1);
//        jimIndex.setName("idx_age");
//        jimIndex.setColNames(new String[]{ "age" });
//        //  jimIndex.setUnique(true);
//        jimIndex.setIndexType(Index.IndexType.Btree);
//        indices[0] = jimIndex;
//        table.setIndexes(indices);
//
//        JimIndex jimIndex1 = new JimIndex();
//        jimIndex1.setId(2);
//        jimIndex1.setName("idx_phone");
//        jimIndex1.setColNames(new String[]{ "phone" });
//        jimIndex1.setUnique(true);
//        jimIndex1.setIndexType(Index.IndexType.Btree);
//        indices[1] = jimIndex1;
//        table.setIndexes(indices);
//
//        JimIndex jimIndex2 = new JimIndex();
//        jimIndex2.setId(3);
//        jimIndex2.setName("idx_age_phone");
//        jimIndex2.setColNames(new String[]{ "age", "phone" });
//        jimIndex2.setUnique(true);
//        jimIndex2.setIndexType(Index.IndexType.Btree);
//        indices[2] = jimIndex2;
//        table.setIndexes(indices);
//
//        table.initColumns();
//
//        return new JimTable[]{ table };
//      }
//    }
//  }
}
