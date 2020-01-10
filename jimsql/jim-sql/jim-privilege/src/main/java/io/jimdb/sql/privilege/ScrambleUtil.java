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

import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import org.apache.commons.lang3.StringUtils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Encryption and decryption tool
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "WEAK_MESSAGE_DIGEST_SHA1", "EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS" })
public final class ScrambleUtil {
  private static final String SHA_1 = "SHA-1";

  private ScrambleUtil() {
  }

  public static String encodePassword(String password) {
    if (StringUtils.isBlank(password)) {
      return "";
    }

    byte[] t1 = hash(SHA_1, password.getBytes(StandardCharsets.UTF_8));
    byte[] t2 = hash(SHA_1, t1);

    return String.format("*%s", ScrambleUtil.byte2HexString(t2));
  }

  public static boolean checkPassword(String passwd, byte[] seed, byte[] authData) {
    try {
      MessageDigest md;
      byte[] pwd = ScrambleUtil.hexStringToByteArray(passwd.substring(1));
      md = MessageDigest.getInstance("SHA-1");
      md.update(seed);
      md.update(pwd);
      byte[] digest = md.digest();
      byte[] xor = xor(authData, digest);
      md.reset();
      byte[] digest1 = md.digest(xor);
      byte[] bytes1 = ScrambleUtil.hexStringToByteArray(passwd.substring(1));
      return MessageDigest.isEqual(digest1, bytes1);
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_UNKNOWN_ERROR, ex, "Check password error");
    }
  }

  public static byte[] xor(final byte[] input, final byte[] secret) {
    final byte[] result = new byte[input.length];
    for (int i = 0; i < input.length; ++i) {
      result[i] = (byte) (input[i] ^ secret[i]);
    }
    return result;
  }

  public static byte[] scramble411(byte[] pass, byte[] seed) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance(SHA_1);

      byte[] pass1 = md.digest(pass);
      md.reset();
      byte[] pass2 = md.digest(pass1);
      md.reset();
      md.update(seed);
      byte[] pass3 = md.digest(pass2);
      for (int i = 0; i < pass3.length; i++) {
        pass3[i] = (byte) (pass3[i] ^ pass1[i]);
      }
      return pass3;
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex);
    }
  }

  private static String byte2HexString(byte[] bytes) {
    StringBuilder hex = new StringBuilder();
    if (bytes != null) {
      for (Byte b : bytes) {
        hex.append(String.format("%02X", b.intValue() & 0xFF));
      }
    }
    return hex.toString();
  }

  private static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    try {
      for (int i = 0; i < len; i += 2) {
        data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                + Character.digit(s.charAt(i + 1), 16));
      }
    } catch (Exception ex) {
      throw DBException.get(ErrorModule.PROTO, ErrorCode.ER_UNKNOWN_ERROR, ex);
    }
    return data;
  }

  private static byte[] hash(String algorithm, byte[] src) {
    try {
      MessageDigest md = MessageDigest.getInstance(algorithm);
      return md.digest(src);
    } catch (Exception e) {
      throw DBException.get(ErrorModule.PRIVILEGE, ErrorCode.ER_UNKNOWN_ERROR, e);
    }
  }
}
