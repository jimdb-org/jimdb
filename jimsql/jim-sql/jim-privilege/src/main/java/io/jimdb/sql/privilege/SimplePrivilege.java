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
package io.jimdb.sql.privilege;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import io.jimdb.core.config.JimConfig;
import io.jimdb.core.model.meta.MetaData;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.pb.Ddlpb;
import io.jimdb.core.plugin.PrivilegeEngine;
import io.jimdb.common.utils.lang.StringUtil;

import com.google.common.base.Strings;

import reactor.core.publisher.Flux;

/**
 * For Test Mock.
 *
 * @version V1.0
 */
public final class SimplePrivilege implements PrivilegeEngine {
  private final String userName;
  private final String password;

  public SimplePrivilege() {
    this.userName = "root";
    this.password = "root";
  }

  @Override
  public void init(JimConfig c) {
  }

  @Override
  public boolean verify(UserInfo userInfo, PrivilegeInfo... privilegeInfo) {
    return true;
  }

  @Override
  public List<String> showGrant(UserInfo userInfo, String user, String host) {
    return Collections.EMPTY_LIST;
  }

  @Override
  public boolean auth(UserInfo userInfo, String catalog, byte[] auth, byte[] salt) {
    if (!StringUtil.isBlank(catalog)) {
      if (null == MetaData.Holder.getMetaData().getCatalog(catalog)) {
        return false;
      }
    }

    String password = matchUser(userInfo.getUser());
    if (Strings.isNullOrEmpty(password)) {
      return false;
    }
    byte[] localSha = ScrambleUtil.scramble411(password.getBytes(StandardCharsets.UTF_8), salt);
    return MessageDigest.isEqual(localSha, auth);
  }

  @Override
  public String getPassword(String user, String host) {
    return "";
  }

  @Override
  public Flux<Boolean> grant(Ddlpb.OpType type, Ddlpb.PrivilegeOp op) {
    return Flux.just(Boolean.TRUE);
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }

  private String matchUser(String userName) {
    if (Strings.isNullOrEmpty(userName) || !this.userName.equalsIgnoreCase(userName)) {
      return "";
    }
    return this.password;
  }
}
