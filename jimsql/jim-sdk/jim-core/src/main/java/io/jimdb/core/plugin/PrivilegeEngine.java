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
package io.jimdb.core.plugin;

import java.util.List;

import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.pb.Ddlpb.OpType;
import io.jimdb.pb.Ddlpb.PrivilegeOp;

import reactor.core.publisher.Flux;

/**
 * @version V1.0
 */
public interface PrivilegeEngine extends Plugin {
  /**
   * User connection authentication
   *
   * @return
   */
  boolean auth(UserInfo userInfo, String catalog, byte[] auth, byte[] salt);

  /**
   * Get Password
   *
   * @return
   */
  String getPassword(String user, String host);

  /**
   * Execute Privilege Command
   *
   * @return
   */
  Flux<Boolean> grant(OpType type, PrivilegeOp op);

  /**
   * Authority validation
   *
   * @param privilegeInfo
   * @return
   */
  boolean verify(UserInfo userInfo, PrivilegeInfo... privilegeInfo);

  /**
   * Flush Privilege.
   */
  void flush();

  /**
   * Show Grants
   *
   * @return
   */
  List<String> showGrant(UserInfo userInfo, String user, String host);

  /**
   * Catalog Is Visible
   */
  boolean catalogIsVisible(String user, String host, String db);
}
