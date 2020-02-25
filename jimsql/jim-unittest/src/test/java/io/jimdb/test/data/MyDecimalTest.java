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
package io.jimdb.test.data;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.values.MySqlDecimalConverter;
import io.jimdb.common.utils.lang.ByteUtil;

import org.junit.Assert;
import org.junit.Test;

public class MyDecimalTest {
  @Test
  public void testCase() {
    List<BigDecimal> dataList = builderTestcase();
    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 22);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 23);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 24);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 25);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 26);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 27);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });

    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 28);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });
    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 29);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });
    dataList.forEach(orgData -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(orgData, 65, 30);
      BigDecimal decimal = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(decimal.toPlainString());
      Assert.assertTrue(orgData.compareTo(decimal) == 0);
    });
  }

  List<BigDecimal> builderTestcase() {
    List<BigDecimal> dataList = new ArrayList<>();

    dataList.add(new BigDecimal("0"));
    dataList.add(new BigDecimal("1"));
    dataList.add(new BigDecimal("12345678"));
    dataList.add(new BigDecimal("123456789"));
    dataList.add(new BigDecimal("1234567890"));
    dataList.add(new BigDecimal("12345678.1"));
    dataList.add(new BigDecimal("0.01234"));
    dataList.add(new BigDecimal("1.01234"));
    dataList.add(new BigDecimal("12345678.0001"));
    dataList.add(new BigDecimal("1000000001"));

    dataList.add(new BigDecimal("-1"));
    dataList.add(new BigDecimal("-12345678"));
    dataList.add(new BigDecimal("-123456789"));
    dataList.add(new BigDecimal("-1234567890"));
    dataList.add(new BigDecimal("-12345678.1"));
    dataList.add(new BigDecimal("-0.01234"));
    dataList.add(new BigDecimal("-1.01234"));
    dataList.add(new BigDecimal("-12345678.0001"));
    dataList.add(new BigDecimal("-1000000001"));
    dataList.add(new BigDecimal("12345678.0100000001"));
    dataList.add(new BigDecimal("12345678.1000000001"));
    dataList.add(new BigDecimal("12345678.1100000001"));

    dataList.add(new BigDecimal("1.0123456789012345678912"));
    dataList.add(new BigDecimal("0.0123456789012345678912"));
    dataList.add(new BigDecimal("123456789012345678901234567890"));
//
    dataList.add(new BigDecimal("-1"));
    dataList.add(new BigDecimal("-111111111"));
    dataList.add(new BigDecimal("-11111111111111111"));
    dataList.add(new BigDecimal("-111111111111111111"));
    dataList.add(new BigDecimal("-1.01"));
    dataList.add(new BigDecimal("-1.0100000001"));
    dataList.add(new BigDecimal("-1.1100000001"));
    dataList.add(new BigDecimal("-12345678"));
    dataList.add(new BigDecimal("-123456789"));
    dataList.add(new BigDecimal("-1234567890"));
    dataList.add(new BigDecimal("-123456789012345678901234567890"));
    dataList.add(new BigDecimal("-12345678.1"));
    dataList.add(new BigDecimal("-0.01234"));
    dataList.add(new BigDecimal("-1.01234"));
    dataList.add(new BigDecimal("-0.0123456789012345678912"));
    dataList.add(new BigDecimal("-1.0123456789012345678912"));
    dataList.add(new BigDecimal("-12345678.0001"));
    dataList.add(new BigDecimal("-12345678.1100000001"));
    dataList.add(new BigDecimal("-12345678.1000000001"));
    dataList.add(new BigDecimal("-1000000001"));
//
//    dataList.add(new BigDecimal("0.01234567890123456789123450123456789012345678912345"));
//    dataList.add(new BigDecimal("-0.01234567890123456789123450123456789012345678912345"));
//    dataList.add(new BigDecimal("1.01234567890123456789123450123456789012345678912345"));
//    dataList.add(new BigDecimal("-1.01234567890123456789123450123456789012345678912345"));
//    dataList.add(new BigDecimal("1234567890123456789123450123456789012345678912345"));
//    dataList.add(new BigDecimal("-1234567890123456789123450123456789012345678912345"));

    return dataList;
  }

  @Test
  public void testByteToStr() {
    byte a = 2;
    System.out.println(Long.toString(a, 2));
  }

  @Test
  public void testBigDecimalUp() {
    BigDecimal decimal = new BigDecimal("33.33");
    System.out.println(decimal.toPlainString());
    System.out.println(decimal.setScale(3).toPlainString());
    System.out.println(decimal.setScale(1, RoundingMode.HALF_UP).toPlainString());
  }

  @Test
  public void testMyDecimal() {
    List<Checker> test = buildtestcase();
    test.forEach(item -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(item.getValue(), item.getPrecision(), item.getScale());
      BigDecimal target = MySqlDecimalConverter.decodeFromMySqlDecimal(bytes);
      System.out.println(target.toPlainString());
      Assert.assertTrue(target.compareTo(item.getValue()) == 0);
    });
  }

  @Test
  public void testMyDecimalBytes() {
    List<Checker> checkers = buildTestBinaryStrCompare();
    checkers.forEach(item -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(item.getValue(), item.getPrecision(), item.getScale());
      String str1 = ByteUtil.bytes2StrWithFillTrailing0(bytes);
      System.out.println(item.getValue().toPlainString() + "--" + str1);
      Assert.assertTrue(str1.equals(item.getExpected()) == true);
    });
  }

  @Test
  public void testMyDecimalBytes1() {
    List<Checker> checkers = buildTestBinaryStrCompare1();
    checkers.forEach(item -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(item.getValue(), item.getPrecision(), item.getScale());
      String str1 = ByteUtil.bytes2StrWithFillTrailing0(bytes);
      String str16 = ByteUtil.bytes2hex01(bytes);
      System.out.println(item.getValue().toPlainString() + "--" + str1);
      System.out.println(item.getValue().toPlainString() + "--" + str16);
      Assert.assertTrue(str1.equals(item.getExpected()) == true);
    });
  }

  public List<Checker> buildTestBinaryStrCompare() {
    List<Checker> list = new ArrayList<>();
    list.add(Checker.build().value(new BigDecimal("123400")).precision(6).scale(0).expected(
            "0000011000000000100000011110001000001000"));

    return list;
  }

  public List<Checker> buildTestBinaryStrCompare1() {
    List<Checker> list = new ArrayList<>();
    list.add(Checker.build().value(new BigDecimal("12.34")).precision(10).scale(2).expected(
            "00001010000000101000000000000000000000000000110000100010"));
    list.add(Checker.build().value(new BigDecimal("13.34")).precision(10).scale(2).expected(
            "00001010000000101000000000000000000000000000110100100010"));

    list.add(Checker.build().value(new BigDecimal("1.3")).precision(10).scale(2).expected(
            "00001010000000101000000000000000000000000000000100011110"));
    return list;
  }

  private List<Checker> buildtestcase() {
    List<Checker> list = new ArrayList<>();
    list.add(Checker.build().value(new BigDecimal("0")).precision(10).scale(0).expected("0"));
    list.add(Checker.build().value(new BigDecimal("1")).precision(1).scale(0).expected("1"));
    list.add(Checker.build().value(new BigDecimal("12345678")).precision(8).scale(0).expected("12345678"));
    list.add(Checker.build().value(new BigDecimal("123456789")).precision(9).scale(0).expected("123456789"));
    list.add(Checker.build().value(new BigDecimal("12345678.1")).precision(9).scale(1).expected("12345678.1"));
    list.add(Checker.build().value(new BigDecimal("0.01234")).precision(6).scale(5).expected("0.01234"));
    list.add(Checker.build().value(new BigDecimal("1.01234")).precision(6).scale(5).expected("1.01234"));
    list.add(Checker.build().value(new BigDecimal("12345678.0001")).precision(12).scale(4).expected("12345678.0001"));
    // leadeing zero do not ignore
    list.add(Checker.build().value(new BigDecimal("1000000001")).precision(10).scale(0).expected("1000000001"));

    list.add(Checker.build().value(new BigDecimal("-1")).precision(1).scale(0).expected("-1"));
    list.add(Checker.build().value(new BigDecimal("-12345678")).precision(8).scale(0).expected("-12345678"));
    list.add(Checker.build().value(new BigDecimal("-123456789")).precision(9).scale(0).expected("-123456789"));
    list.add(Checker.build().value(new BigDecimal("-12345678.1")).precision(9).scale(1).expected("-12345678.1"));
    list.add(Checker.build().value(new BigDecimal("-0.01234")).precision(6).scale(5).expected("-0.01234"));
    list.add(Checker.build().value(new BigDecimal("-1.01234")).precision(6).scale(5).expected("-1.01234"));
    list.add(Checker.build().value(new BigDecimal("-12345678.0001")).precision(12).scale(4).expected("-12345678.0001"));
    // leadeing zero do not ignore
    list.add(Checker.build().value(new BigDecimal("-1000000001")).precision(10).scale(0).expected("-1000000001"));
    return list;
  }

  /**
   * BigDecimal precision : the number of digits in the unscaled value(zero is one) {@link BigDecimal#precision()}
   * BigDecimal unscaledValue: the unscaled value of this {@code BigDecimal} {@link BigDecimal#unscaledValue()}
   * mysql decimal: The precision represents the number of significant digits that are stored for values,
   * and the scale represents the number of digits that can be stored following the decimal point
   *
   * so convert BigDecimal to Mysql decimal to see {@link }
   */
  @Test
  public void testPrecision() {
    BigDecimal bigDecimal = new BigDecimal("0.00");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "--" + bigDecimal.scale());
    bigDecimal = new BigDecimal("0.11");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "--" + bigDecimal.scale());
    bigDecimal = new BigDecimal("1.11");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "--" + bigDecimal.scale());
    bigDecimal = new BigDecimal("100");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("100.00");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("100.01");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("0.00001");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("-0.00001");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("1.00001");
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    bigDecimal = new BigDecimal("1.0001");
    BigDecimal round = bigDecimal.round(new MathContext(2, RoundingMode.HALF_EVEN));
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    System.out.println(round.toPlainString() + "--" + round.precision() + "-" + round.scale());
    bigDecimal = new BigDecimal("0.0001");
    round = bigDecimal.round(new MathContext(3, RoundingMode.HALF_EVEN));
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    System.out.println(round.toPlainString() + "--" + round.precision() + "-" + round.scale());
    bigDecimal = new BigDecimal("0.0001");
    round = bigDecimal.setScale(2, RoundingMode.HALF_EVEN);
    System.out.println(bigDecimal.toPlainString() + "--" + bigDecimal.precision() + "-" + bigDecimal.scale());
    System.out.println(round.toPlainString() + "--" + round.precision() + "-" + round.scale());
  }

  @Test
  public void testGetLength() {
    List<Checker> checkers = buildTestBinaryStrCompare1();
    checkers.forEach(item -> {
      byte[] bytes = MySqlDecimalConverter.encodeToMySqlDecimal(item.getValue(), item.getPrecision(), item.getScale());
      String str1 = ByteUtil.bytes2StrWithFillTrailing0(bytes);
      System.out.println(item.getValue().toPlainString() + "--" + str1);
      Assert.assertTrue(MySqlDecimalConverter.getBytesLength(10, 2) + 2 == bytes.length);
    });
  }

  private static class Checker {
    private BigDecimal value;
    private int precision;
    private int scale;
    private String expected;

    public static Checker build() {
      return new Checker();
    }

    public BigDecimal getValue() {
      return value;
    }

    public Checker value(BigDecimal value) {
      this.value = value;
      return this;
    }

    public int getPrecision() {
      return precision;
    }

    public Checker precision(int precision) {
      this.precision = precision;
      return this;
    }

    public int getScale() {
      return scale;
    }

    public Checker scale(int scale) {
      this.scale = scale;
      return this;
    }

    public String getExpected() {
      return expected;
    }

    public Checker expected(String expected) {
      this.expected = expected;
      return this;
    }
  }
}
