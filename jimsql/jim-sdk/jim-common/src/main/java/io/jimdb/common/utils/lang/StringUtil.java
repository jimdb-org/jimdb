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
package io.jimdb.common.utils.lang;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

import com.alibaba.druid.util.StringUtils;

/**
 * String util
 */
public final class StringUtil {
  private static final char[] HEX_CHARS = "0123456789abcdef".toCharArray();
  private static final byte[] CHARS_HEX = new byte['f' + 1];

  static {
    for (int i = 0; i < HEX_CHARS.length; i++) {
      CHARS_HEX[HEX_CHARS[i]] = (byte) i;
    }
  }

  public static final int NO_MATCH = 0;
  public static final int MATCH = 1;

  public static final char PAT_HOST_ANY = '%';
  public static final char PAT_CATALOG_ANY = '*';
  public static final char PAT_ONE = '_';
  public static final char PATTERN = '\\';

  private StringUtil() {
  }

  public static String trimTail(String str) {
    int len = str.length();
    for (; len > 0; len--) {
      if (str.charAt(len - 1) != ' ') {
        break;
      }
    }
    return str.substring(0, len);
  }

  /**
   * Return the collation name.
   *
   * @param locale TODO
   * @return TODO
   */
  public static String getCollationName(final Locale locale) {
    Locale english = Locale.ENGLISH;
    String name = locale.getDisplayLanguage(english) + ' ' + locale.getDisplayCountry(english) + ' ' + locale.getVariant();
    name = name.trim().replace(' ', '_');
    return name.toUpperCase(english);
  }

  /**
   * Match name of the locale with the given name.
   *
   * @param locale TODO
   * @param name TODO
   * @return TODO
   */
  public static boolean matchCollationName(final Locale locale, final String name) {
    return name.equalsIgnoreCase(locale.toString()) || name.equalsIgnoreCase(getCollationName(locale));
  }

  /**
   * Convert bytes to hex string.
   *
   * @param value TODO
   * @return TODO
   */
  public static String toHex(final byte[] value) {
    return toHex(value, value.length);
  }

  /**
   * Convert bytes to hex string.
   *
   * @param value TODO
   * @param len TODO
   * @return TODO
   */
  public static String toHex(byte[] value, int len) {
    final char[] buf = new char[len + len];
    for (int i = 0; i < len; i++) {
      int c = value[i] & 0xff;
      buf[i + i] = HEX_CHARS[c >> 4];
      buf[i + i + 1] = HEX_CHARS[c & 0xf];
    }
    return new String(buf);
  }

  /**
   * set hexString to string
   *
   * @param s TODO
   * @return TODO
   */
  public static String hexStringToString(String s) {
    byte[] bytes = new byte[s.length() / 2];
    int n;
    for (int i = 0; i < bytes.length; i++) {
      n = CHARS_HEX[s.charAt(i + i)] << 4;
      n += CHARS_HEX[s.charAt(i + i + 1)] & 0xf;
      bytes[i] = (byte) (n & 0xff);
    }
    return new String(bytes, StandardCharsets.UTF_8);
  }

  /**
   * Matching character suffix, anyChar match all ，'_' match one
   *
   * @param src TODO
   * @param target TODO
   * @return match
   */
  public static int doMatchString(String src, String target, char anyChar) {
    if (StringUtils.isEmpty(src) || StringUtils.isEmpty(target)) {
      return NO_MATCH;
    }
    if (target.equals(String.valueOf(anyChar))) {
      return MATCH;
    }

    src = src.toUpperCase();
    target = target.toUpperCase();
    int searchForPos = 0;
    int searchForEnd = target.length();
    int searchInPos = 0;
    int searchInEnd = src.length();

    while (searchForPos != searchForEnd) {
      while ((target.charAt(searchForPos) != anyChar) && (target.charAt(searchForPos) != PAT_ONE)) {
        if ((target.charAt(searchForPos) == PATTERN) && ((searchForPos + 1) != searchForEnd)) {
          searchForPos++;
        }

        if (searchInPos == searchInEnd || target.charAt(searchForPos++) != src.charAt(searchInPos++)) {
          return NO_MATCH;
        }

        if (searchForPos == searchForEnd) {
          if (searchInPos != searchInEnd) {
            return NO_MATCH;
          } else {
            return MATCH;
          }
        }
      }

      if (target.charAt(searchForPos) == PAT_ONE) {
        do {
          if (searchInPos == searchInEnd) {
            return NO_MATCH;
          }
          searchInPos++;
        } while ((++searchForPos < searchForEnd) && (target.charAt(searchForPos) == PAT_ONE));

        if (searchForPos == searchForEnd) {
          break;
        }
      }

      if (target.charAt(searchForPos) == anyChar) {
        searchForPos++;

        for (; searchForPos != searchForEnd; searchForPos++) {
          if (target.charAt(searchForPos) == anyChar) {
            continue;
          }

          if (target.charAt(searchForPos) == PAT_ONE) {
            if (searchInPos == searchInEnd) {
              return NO_MATCH;
            }
            searchInPos++;
            continue;
          }
          break;
        }

        if (searchForPos == searchForEnd) {
          return MATCH;
        }
        if (searchInPos == searchInEnd) {
          return NO_MATCH;
        }

        char cmp;
        if (((cmp = target.charAt(searchForPos)) == PATTERN) && ((searchForPos + 1) != searchForEnd)) {
          cmp = target.charAt(++searchForPos);
        }

        searchForPos++;
        do {
          while ((searchInPos != searchInEnd) && (src.charAt(searchInPos) != cmp)) {
            searchInPos++;
          }

          if (searchInPos++ == searchInEnd) {
            return NO_MATCH;
          }

          int tmp = doMatchString(src.substring(searchInPos), target.substring(searchForPos), anyChar);
          if (tmp <= 0) {
            return tmp;
          }
        } while (searchInPos != searchInEnd);
        return NO_MATCH;
      }
    }
    if (searchInPos != searchInEnd) {
      return NO_MATCH;
    } else {
      return MATCH;
    }
  }

  public static boolean matchString(String src, String target) {
    return doMatchString(src, target, PAT_HOST_ANY) == MATCH;
  }

  public static boolean matchCatalogString(String src, String target) {
    return doMatchString(src, target, PAT_CATALOG_ANY) == MATCH;
  }


  /**
   * @param cs TODO
   * @return TODO
   */
  public static boolean isBlank(CharSequence cs) {
    int strLen;
    if (cs == null || (strLen = cs.length()) == 0) {
      return true;
    }
    for (int i = 0; i < strLen; i++) {
      if (!Character.isWhitespace(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * @param cs TODO
   * @return TODO
   */
  public static boolean isNotBlank(CharSequence cs) {
    return !isBlank(cs);

  }
}
