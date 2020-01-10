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
package io.jimdb.test.mock.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.jimdb.core.expression.ColumnExpr;
import io.jimdb.core.expression.RowValueAccessor;
import io.jimdb.core.expression.ValueAccessor;
import io.jimdb.core.model.meta.Table;
import io.jimdb.core.values.BinaryValue;
import io.jimdb.core.values.LongValue;
import io.jimdb.core.values.Value;

import com.google.common.collect.Lists;

/**
 * Mock data for database tables.
 */
public class MockTableData {

  public static class User {
    public String name;
    public int age;
    public String phone;
    public int score;
    public int salary;

    private Map<String, Value> fieldValus = new HashMap<>();

    public User(String name, int age, String phone, int score, int salary) {
      this.name = name;
      this.age = age;
      this.phone = phone;
      this.score = score;
      this.salary = salary;

      fieldValus.put("name", BinaryValue.getInstance(this.name.getBytes()));
      fieldValus.put("age", LongValue.getInstance(this.age));
      fieldValus.put("phone", BinaryValue.getInstance(this.phone.getBytes()));
      fieldValus.put("score", LongValue.getInstance(this.score));
      fieldValus.put("salary", LongValue.getInstance(this.salary));
    }

    public User() {

    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public int getAge() {
      return age;
    }

    public void setAge(int age) {
      this.age = age;
    }

    public String getPhone() {
      return phone;
    }

    public void setPhone(String phone) {
      this.phone = phone;
    }

    public int getScore() {
      return score;
    }

    public void setScore(int score) {
      this.score = score;
    }

    public int getSalary() {
      return salary;
    }

    public void setSalary(int salary) {
      this.salary = salary;
    }

    @Override
    public String toString() {
      return "User{" +
              "name='" + name + '\'' +
              ", age=" + age +
              ", phone='" + phone + '\'' +
              ", score=" + score +
              ", salary=" + salary +
              '}';
    }

    private boolean filtered(Map<Object, Object> filter) {
      if (filter == null) {
        return false;
      }

      boolean filtered = false;
      Iterator<Object> it = filter.keySet().iterator();
      while (it.hasNext()) {
        String k = (String)it.next();
        Object v = filter.get(k);
        if (k.equals("name") && v.equals(name)
                || k.equals("age") && (Integer)v == age
                || k.equals("phone") && v.equals(phone)
                || k.equals("score") && (Integer)v == score
                || k.equals("salary") && (Integer)v == salary) {
          filtered = true;
        }
      }
      return filtered;
    }

    private static List<ValueAccessor> filteData(ColumnExpr[] resultColumns, List<MockTableData.User> users, Map<Object, Object> filter) {
      final int size = resultColumns.length;

      List<ValueAccessor> newRows = Lists.newArrayListWithCapacity(users.size());
      for (MockTableData.User user : users) {
        if (user.filtered(filter)) {
          continue;
        }

        Value[] colValues = new Value[size];
        for (int i = 0; i < size; i++) {
          ColumnExpr expr = resultColumns[i];
          String columnName = expr.getOriCol();
          if (user.fieldValus.containsKey(columnName)) {
            colValues[i] = user.fieldValus.get(columnName);
          }
        }
        newRows.add(new RowValueAccessor(colValues));
      }

      return newRows;
    }
  }

  static final List<User> userDataList = new ArrayList<>();
  static {
    userDataList.add(new User("Tom", 30, "13010010000", 90, 5000));
    userDataList.add(new User("Jack", 20, "13110011111", 100, 6000));
    userDataList.add(new User("Mary", 50, "13210012222", 60, 7000));
    userDataList.add(new User("Suzy", 40, "13310013333", 90, 8000));
    userDataList.add(new User("Kate", 30, "13410014444", 80, 9000));
    userDataList.add(new User("Luke", 20, "13510015555", 100, 5000));
    userDataList.add(new User("Sophia", 10, "13610016666", 80, 4000));
    userDataList.add(new User("Steven", 60, "13710017777", 50, 3000));
    userDataList.add(new User("Bob", 30, "13810018888", 40, 2000));
    userDataList.add(new User("Daniel", 40, "13910019999", 70, 5000));
  }

  public static List<User> getUserDataList() {
    return userDataList;
  }

  public static ValueAccessor[] provideDatas(Table table, ColumnExpr[] resultColumns, Map<Object, Object> filter) {
    List<ValueAccessor> result = null;
    if (table.getName().equalsIgnoreCase("user")) {
      result = User.filteData(resultColumns, userDataList, filter);
    }
    return result == null ? null : result.toArray(new ValueAccessor[result.size()]);
  }

  public static List<Value> getColumnValueList() {
    return userDataList.stream()
                   .map(user -> BinaryValue.getInstance(user.name.getBytes()))
                   .collect(Collectors.toList());
  }

}
