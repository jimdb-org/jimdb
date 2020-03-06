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
package io.jimdb.fi;

import java.util.Objects;
import java.util.Optional;

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

/**
 * The definition of action execution that controls when action is executed.
 * The definition format is: [<pct>%][<cnt>*]<type>[(args...)]
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "CC_CYCLOMATIC_COMPLEXITY", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS", "LEST_LOST_EXCEPTION_STACK_TRACE" })
final class Action extends ProbabilityModel {
  private final ActionType type;
  private final String arg;

  Action(ActionType type, String arg, float prob, int count) {
    super(prob, count);
    this.type = type;
    this.arg = arg == null ? "" : arg;
  }

  /**
   * Parse an action from string.
   *
   * @param s [p%][cnt*]<type>[(args)]:`p%` is probability, `cnt*` is count.
   * @return
   */
  static Action parseAction(final String s) {
    String args = "";
    String str = s.trim();
    Tuple2<String, Optional<String>> splits = split(str, "\\(");
    if (splits.getT2().isPresent()) {
      args = splits.getT2().get();
      if (!args.endsWith(")")) {
        throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
                String.format("parentheses do not match for '%s'", s));
      }

      str = splits.getT1();
      args = args.substring(0, args.length() - 1);
    }

    float prob = MAX_PROB;
    splits = split(str, "\\%");
    if (splits.getT2().isPresent()) {
      try {
        float p = Float.parseFloat(splits.getT1());
        prob = p / 100.0f;
        if (prob < MIN_PROB) {
          prob = MIN_PROB;
        } else if (prob > MAX_PROB) {
          prob = MAX_PROB;
        }
      } catch (Exception ex) {
        throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
                String.format("parse probability error for '%s': %s", s, ex.getMessage()));
      }

      str = splits.getT2().get();
    }

    int count = -1;
    splits = split(str, "\\*");
    if (splits.getT2().isPresent()) {
      try {
        count = Integer.parseInt(splits.getT1());
        if (count < 0) {
          count = 0;
        }
      } catch (Exception ex) {
        throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
                String.format("parse count error for '%s': %s", s, ex.getMessage()));
      }
      str = splits.getT2().get();
    }

    final ActionType type;
    switch (str.toLowerCase()) {
      case "off":
        type = ActionType.OFF;
        break;
      case "return":
        type = ActionType.RETURN;
        break;
      case "sleep":
        verifyTimeout("sleep", s, args);
        type = ActionType.SLEEP;
        break;
      case "delay":
        verifyTimeout("delay", s, args);
        type = ActionType.DELAY;
        break;
      case "pause":
        if (StringUtils.isNotBlank(args)) {
          verifyTimeout("pause", s, args);
        }
        type = ActionType.PAUSE;
        break;
      case "panic":
        type = ActionType.PANIC;
        break;
      case "print":
        type = ActionType.PRINT;
        break;
      case "yield":
        type = ActionType.YIELD;
        break;
      default:
        throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
                String.format("unsupport fault type for '%s'", s));
    }

    return new Action(type, args, prob, count);
  }

  static Tuple2<String, Optional<String>> split(String s, String regex) {
    final String[] splits = s.split(regex, 2);
    return Tuples.of(splits[0], Optional.ofNullable(splits.length == 1 ? null : splits[1]));
  }

  static void verifyTimeout(String name, String src, String args) {
    if (StringUtils.isBlank(args)) {
      throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
              String.format("%s require timeout args for '%s'", name, src));
    }

    try {
      long timeout = Long.parseLong(args);
      if (timeout <= 0) {
        throw new RuntimeException("timeout must be greater than 0");
      }
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.SYSTEM, ErrorCode.ER_INTERNAL_ERROR, false,
              String.format("parse %s timeout error for '%s': %s", name, src, ex.getMessage()));
    }
  }

  ActionType getType() {
    return type;
  }

  String getArg() {
    return arg;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }

    Action that = (Action) o;
    return type == that.type
            && Objects.equals(arg, that.arg);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), type, arg);
  }
}
