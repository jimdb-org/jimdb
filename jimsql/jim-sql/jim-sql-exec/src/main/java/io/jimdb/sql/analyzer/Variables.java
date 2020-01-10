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
package io.jimdb.sql.analyzer;

import io.jimdb.core.expression.Expression;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;

/**
 * @version V1.0
 */
public final class Variables {

  public static Variable resolveVariable(SQLExpr expr) {
    boolean global = false;
    boolean system = true;
    String name = null;
    if (expr instanceof SQLVariantRefExpr) {
      SQLVariantRefExpr targetVar = (SQLVariantRefExpr) expr;
      name = targetVar.getName();
      if (targetVar.isGlobal()) {
        global = true;
        system = true;
      }

      if (name.charAt(0) == '@') {
        if (name.charAt(1) == '@') {
          system = true;
          name = name.substring(2);
        } else {
          system = false;
          name = name.substring(1);
        }
      }
    } else if (expr instanceof SQLPropertyExpr) {
      SQLPropertyExpr targetVar = (SQLPropertyExpr) expr;
      SQLExpr owner = targetVar.getOwner();
      if (owner instanceof SQLVariantRefExpr) {
        String ownerName = ((SQLVariantRefExpr) owner).getName();
        if (ownerName.charAt(0) == '@') {
          if (ownerName.charAt(1) == '@') {
            system = true;
            global = "@@GLOBAL".equalsIgnoreCase(ownerName);
          }
          name = targetVar.getSimpleName();
        } else {
          system = false;
          name = ownerName.substring(1) + "." + targetVar.getSimpleName();
        }
      } else {
        system = true;
        name = targetVar.toString();
      }
    }

    if (name == null) {
      return null;
    }

    Variable variable = new Variable(name);
    variable.setSystem(system);
    variable.setGlobal(global);
    return variable;
  }

  /**
   * Variable assignment in Set statement.
   */
  public static final class VariableAssign {
    private Variable variable;
    private Expression value;
    private boolean defaultValue;

    public VariableAssign(Variable variable) {
      this.variable = variable;
    }

    public Variable getVariable() {
      return variable;
    }

    public void setVariable(Variable variable) {
      this.variable = variable;
    }

    public void setValue(Expression value) {
      this.value = value;
    }

    public Expression getValue() {
      return value;
    }

    public boolean isDefaultValue() {
      return defaultValue;
    }

    public void setDefaultValue(boolean defaultValue) {
      this.defaultValue = defaultValue;
    }
  }

  /**
   * Variable information.
   */
  public static final class Variable {
    private final String name;
    private boolean global;
    private boolean system;

    public Variable(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }

    public boolean isGlobal() {
      return global;
    }

    public void setGlobal(boolean global) {
      this.global = global;
    }

    public boolean isSystem() {
      return system;
    }

    public void setSystem(boolean system) {
      this.system = system;
    }
  }
}
