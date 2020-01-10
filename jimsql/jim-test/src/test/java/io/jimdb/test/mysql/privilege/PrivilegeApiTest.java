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
package io.jimdb.test.mysql.privilege;

import java.util.List;

import io.jimdb.core.config.JimConfig;
import io.jimdb.core.model.privilege.PrivilegeInfo;
import io.jimdb.core.model.privilege.PrivilegeType;
import io.jimdb.core.model.privilege.UserInfo;
import io.jimdb.core.model.result.ExecResult;
import io.jimdb.sql.privilege.CacheablePrivilege;
import io.jimdb.sql.privilege.PrivilegeStore;
import io.jimdb.test.mysql.SqlTestBase;
import io.jimdb.common.utils.lang.IOUtil;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @version V1.0
 */
public class PrivilegeApiTest extends SqlTestBase {

  CacheablePrivilege privilegeEngine;

  @Before
  public void init() {
    JimConfig jimConfig = new JimConfig(IOUtil.loadResource("jim_test.properties"));
//    PluginFactory.init(jimConfig);
    privilegeEngine = new CacheablePrivilege();
    privilegeEngine.init(jimConfig);
  }

  @After
  public void clear() {
    String userDeleteSQL1 = "delete from mysql.user where host = '10.%' and user = 'root';";
    String userDeleteSQL2 = "delete from mysql.user where host = 'localhost' and user = 'root';";
    PrivilegeStore.execute(userDeleteSQL1);
    PrivilegeStore.execute(userDeleteSQL2);
  }

  @Test
  public void syncQueryTest() {
    ExecResult execResult = PrivilegeStore.execute("select * from mysql.db");
  }

  @Test
  public void verifyPrivilegeTest() {
    String userInsertSQL = "insert mysql.user(Host,User,Password,Insert_priv) values(" +
            "'localhost','root','1234565','Y');";
    String userDeleteSQL = "delete from mysql.user where host = 'localhost' and user = 'root';";
    UserInfo userInfo = new UserInfo("root", "localhost");
    PrivilegeInfo privilegeInfo = new PrivilegeInfo("maggie", "codec_test", PrivilegeType.INSERT_PRIV);
    PrivilegeStore.execute(userDeleteSQL);
    privilegeEngine.flush();
    boolean a = privilegeEngine.verify(userInfo, privilegeInfo);
    Assert.assertEquals(a, false);
    PrivilegeStore.execute(userDeleteSQL);
    PrivilegeStore.execute(userInsertSQL);
    privilegeEngine.flush();
    boolean b = privilegeEngine.verify(userInfo, privilegeInfo);
    Assert.assertEquals(b, true);
    PrivilegeStore.execute(userDeleteSQL);
  }

  @Test
  public void verifymatchTest() {
    String userInsertSQL = "insert mysql.user(Host,User,Password,Insert_priv) values(" +
            "'10.%','root','123456','Y');";
    String userDeleteSQL = "delete from mysql.user where host = '10.%' and user = 'root';";
    UserInfo userInfo1 = new UserInfo("root", "10.172.188.88");
    UserInfo userInfo2 = new UserInfo("root", "11.172.188.88");

    PrivilegeInfo privilegeInfo = new PrivilegeInfo("maggie", "codec_test", PrivilegeType.INSERT_PRIV);
    PrivilegeStore.execute(userDeleteSQL);
    privilegeEngine.flush();
    boolean a = privilegeEngine.verify(userInfo1, privilegeInfo);
    Assert.assertEquals(a, false);
    PrivilegeStore.execute(userDeleteSQL);
    PrivilegeStore.execute(userInsertSQL);
    privilegeEngine.flush();
    boolean b1 = privilegeEngine.verify(userInfo1, privilegeInfo);
    boolean b2 = privilegeEngine.verify(userInfo2, privilegeInfo);

    Assert.assertEquals(b1, true);
    Assert.assertEquals(b2, false);

    PrivilegeStore.execute(userDeleteSQL);
  }

  @Test
  public void showGrantsTest() {
    String userInsertSQL = "insert mysql.user(Host,User,Password,Insert_priv) values(" +
            "'10.%','root','123456','Y');";
    String userDeleteSQL = "delete from mysql.user where host = '10.%' and user = 'root';";
    UserInfo userInfo1 = new UserInfo("root", "10.172.188.88");
    PrivilegeStore.execute(userDeleteSQL);
    PrivilegeStore.execute(userInsertSQL);
    privilegeEngine.flush();
    List<String> list = privilegeEngine.showGrant(userInfo1, "root", "10.172.188.88");
    Assert.assertEquals("GRANT INSERT_PRIV ON *.* TO 'root'@'10.172.188.88'", list.get(0));
  }
}
