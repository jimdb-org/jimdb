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
package io.jimdb.mysql.util;

import java.util.HashMap;
import java.util.Map;

import io.jimdb.mysql.constant.MySQLVersion;

/**
 * charset util
 *
 * @version V1.0
 * @see <a href="https://dev.mysql.com/doc/internals/en/character-set.html#packet-Protocol::CharacterSet">CharacterSet</a>
 */
public final class CharsetUtil {

  private static final Map<String, Integer> CHARSET_MAP = new HashMap<String, Integer>();

  static {
    CHARSET_MAP.put("pclatin2", 40);
    CHARSET_MAP.put("latin1_de", 31);
    CHARSET_MAP.put("macroman", 39);
    CHARSET_MAP.put("euc_kr", 19);
    CHARSET_MAP.put("macromanciai", 55);
    CHARSET_MAP.put("geostd8", 92);
    CHARSET_MAP.put("win1250", 26);
    CHARSET_MAP.put("latvian1", 42);
    CHARSET_MAP.put("estonia", 20);
    CHARSET_MAP.put("keybcs2", 37);
    CHARSET_MAP.put("big5", 1);
    CHARSET_MAP.put("ucs2", 35);
    CHARSET_MAP.put("czech", 2);
    CHARSET_MAP.put("german1", 5);
    CHARSET_MAP.put("euckr", 85);
    CHARSET_MAP.put("usa7", 11);
    CHARSET_MAP.put("maccecias", 45);
    CHARSET_MAP.put("macromancsas", 56);
    CHARSET_MAP.put("koi8_ru", 7);
    CHARSET_MAP.put("hp8", 6);
    CHARSET_MAP.put("macromancias", 54);
    CHARSET_MAP.put("win1250ch", 34);
    CHARSET_MAP.put("latin1cias", 48);
    CHARSET_MAP.put("cp1251csas", 52);
    CHARSET_MAP.put("armscii", 64);
    CHARSET_MAP.put("armscii8", 32);
    CHARSET_MAP.put("binary", 63);
    CHARSET_MAP.put("maccecsas", 46);
    CHARSET_MAP.put("cp1251cias", 51);
    CHARSET_MAP.put("latin1csas", 49);
    CHARSET_MAP.put("croat", 27);
    CHARSET_MAP.put("cp1251bin", 50);
    CHARSET_MAP.put("eucjpms", 97);
    CHARSET_MAP.put("cp1257", 29);
    CHARSET_MAP.put("gb2312", 24);
    CHARSET_MAP.put("utf8", 33);
    CHARSET_MAP.put("utf-8", 33);
    CHARSET_MAP.put("UTF-8", 33);
    CHARSET_MAP.put("dos", 4);
    CHARSET_MAP.put("ujis", 12);
    CHARSET_MAP.put("hebrew", 16);
    CHARSET_MAP.put("hungarian", 21);
    CHARSET_MAP.put("cp866", 36);
    CHARSET_MAP.put("koi8_ukr", 22);
    CHARSET_MAP.put("macceciai", 44);
    CHARSET_MAP.put("dec8", 3);
    CHARSET_MAP.put("maccebin", 43);
    CHARSET_MAP.put("cp1250", 66);
    CHARSET_MAP.put("cp1251", 14);
    CHARSET_MAP.put("macce", 38);
    CHARSET_MAP.put("ascii", 65);
    CHARSET_MAP.put("cp1256", 57);
    CHARSET_MAP.put("swe7", 10);
    CHARSET_MAP.put("greek", 25);
    CHARSET_MAP.put("cp850", 80);
    CHARSET_MAP.put("danish", 15);
    CHARSET_MAP.put("koi8r", 74);
    CHARSET_MAP.put("latin5", 30);
    CHARSET_MAP.put("latin7", 79);
    CHARSET_MAP.put("latin1bin", 47);
    CHARSET_MAP.put("cp932", 95);
    CHARSET_MAP.put("koi8ukr", 75);
    CHARSET_MAP.put("cp852", 81);
    CHARSET_MAP.put("gbk", 28);
    CHARSET_MAP.put("sjis", 13);
    CHARSET_MAP.put("latin1", 8);
    CHARSET_MAP.put("latin2", 9);
    CHARSET_MAP.put("macromanbin", 53);
    CHARSET_MAP.put("tis620", 18);
    CHARSET_MAP.put("win1251ukr", 23);
    CHARSET_MAP.put("latvian", 41);
  }

  public static int getCharset(String charset) {
    if (charset == null || charset.length() == 0) {
      return MySQLVersion.CHARSET;
    } else {
      Integer i = CHARSET_MAP.get(charset);
      return (i == null) ? MySQLVersion.CHARSET : i.intValue();
    }
  }
}
