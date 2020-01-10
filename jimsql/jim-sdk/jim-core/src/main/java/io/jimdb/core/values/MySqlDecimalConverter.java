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
package io.jimdb.core.values;

import static io.jimdb.common.exception.ErrorCode.ER_M_BIGGER_THAN_D;
import static io.jimdb.common.exception.ErrorCode.ER_TOO_BIG_PRECISION;
import static io.jimdb.common.exception.ErrorCode.ER_TOO_BIG_SCALE;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import io.jimdb.core.types.Types;
import io.jimdb.common.exception.DBException;
import io.jimdb.common.exception.ErrorCode;
import io.jimdb.common.exception.ErrorModule;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * mysqlDecimal converter
 *
 * @version V1.0
 */
@SuppressFBWarnings({ "PL_PARALLEL_LISTS" })
public class MySqlDecimalConverter {
  private static final int TEN0 = 1;
  private static final int TEN1 = 10;
  private static final int TEN2 = 100;
  private static final int TEN3 = 1000;
  private static final int TEN4 = 10000;
  private static final int TEN5 = 100000;
  private static final int TEN6 = 1000000;
  private static final int TEN7 = 10000000;
  private static final int TEN8 = 100000000;
  private static final int TEN9 = 1000000000;

  private static final String ZERO0 = "";
  private static final String ZERO1 = "0";
  private static final String ZERO2 = "00";
  private static final String ZERO3 = "000";
  private static final String ZERO4 = "0000";
  private static final String ZERO5 = "00000";
  private static final String ZERO6 = "000000";
  private static final String ZERO7 = "0000000";
  private static final String ZERO8 = "00000000";
  private static final String ZERO9 = "000000000";

  private static final int DIGITS_PER_WORD = 9;
  private static final int WORD_SIZE = 4;
  private static final int[] DIG_2_BYTES = { 0, 1, 1, 2, 2, 3, 3, 4, 4, 4 };
  private static final int[] POWERS_10 = { TEN0, TEN1, TEN2, TEN3, TEN4, TEN5, TEN6, TEN7, TEN8, TEN9 };
  private static final String[] ZERO_STRS = { ZERO0, ZERO1, ZERO2, ZERO3, ZERO4, ZERO5, ZERO6, ZERO7, ZERO8, ZERO9 };

  /**
   * encode myDecimal form BigDecimal
   *
   * @param data
   * @param precision
   * @param frac
   * @return
   */
  public static byte[] encodeToMySqlDecimal(BigDecimal data, int precision, int frac) {
    //
    if (precision > Types.MAX_DEC_WIDTH) {
      throw DBException.get(ErrorModule.PARSER, ER_TOO_BIG_PRECISION, String.valueOf(precision));
    }
    if (frac > Types.MAX_DEC_SCALE) {
      throw DBException.get(ErrorModule.PARSER, ER_TOO_BIG_SCALE, String.valueOf(frac));
    }
    if (precision < frac) {
      throw DBException.get(ErrorModule.PARSER, ER_M_BIGGER_THAN_D, "");
    }

    if (precision == 0 && frac == 0) {
      frac = data.scale();
      // for bigDecimal, if it is pure decimal(0.01), the precision is to remove the leading 0, e. the precision of 0.01 is 1
      precision = data.precision() > frac ? data.precision() : frac;
    }
    if (data == null) {
      return null;
    }
    String str = data.toPlainString();
    if (str.length() <= 0) {
      throw new IllegalArgumentException(String.format("the length[%d] that bigDecimal convert to String less or "
              + "equal 0", str.length()));
    }
    if (str.charAt(0) == '+' || str.charAt(0) == '-') {
      str = str.substring(1);
    }

    int mask = data.signum() < 0 ? -1 : 0;

    int intSize;
    int fracSize = 0;
    List<String> digitsList = new ArrayList<>(8);
    String[] split = str.split("\\.");
    String digitsInt = split[0];

    char[] intChars = digitsInt.toCharArray();
    int digitsIntLen = digitsInt.length();
    int leadingInt = digitsIntLen % DIGITS_PER_WORD;
    int wordsInt = digitsIntLen / DIGITS_PER_WORD;

    String digitsFrac = "";
    int digitsFracLen = 0;
    if (split.length > 1) {
      digitsFrac = split[1];
      digitsFracLen = digitsFrac.length();
    }

    int wordsFrac = 0;
    int trailingFrac = 0;
    int start = 0;
    int end = leadingInt == 0 ? DIGITS_PER_WORD - 1 : leadingInt - 1;
    StringBuilder builder = new StringBuilder();
    do {
      for (; start <= end && start < digitsIntLen; start++) {
        builder.append(intChars[start]);
      }
      digitsList.add(builder.toString());
      builder = new StringBuilder();
      end += DIGITS_PER_WORD;
    } while (start < digitsIntLen);

    if (digitsFracLen > 0) {
      digitsFracLen = digitsFrac.length();
      char[] fracChars = digitsFrac.toCharArray();
      trailingFrac = digitsFracLen % DIGITS_PER_WORD;
      wordsFrac = digitsFracLen / DIGITS_PER_WORD;
      start = 0;
      end = DIGITS_PER_WORD - 1;
      do {
        for (; start <= end && start < digitsFracLen; start++) {
          builder.append(fracChars[start]);
        }
        digitsList.add(builder.toString());
        builder = new StringBuilder();
        end += DIGITS_PER_WORD;
      } while (start < digitsFracLen);
      fracSize = wordsFrac * WORD_SIZE + DIG_2_BYTES[trailingFrac];
    }
    //builder bytes size
    intSize = wordsInt * WORD_SIZE + DIG_2_BYTES[leadingInt];
    // builder target precision and scale
    int specDigitsInt = precision - frac;
    int specWordsInt = specDigitsInt / DIGITS_PER_WORD;
    int specLeadingDigitsInt = specDigitsInt - specWordsInt * DIGITS_PER_WORD;
    int specIntSize = specWordsInt * WORD_SIZE + DIG_2_BYTES[specLeadingDigitsInt];

    int specDigitsFrac = frac;
    int specWordsFrac = specDigitsFrac / DIGITS_PER_WORD;
    int specTrailingDigitsFrac = specDigitsFrac - specWordsFrac * DIGITS_PER_WORD;
    int specFracSize = specWordsFrac * WORD_SIZE + DIG_2_BYTES[specTrailingDigitsFrac];

    byte[] bin = new byte[specIntSize + specFracSize + 2];
    int wordIndex = 0;
    int binIndex = 0; // skip
    // set precision and scale
    bin[binIndex++] = (byte) precision;
    bin[binIndex++] = (byte) frac;

    if (specDigitsInt < digitsIntLen) {
      // TODO ErrOverflow
      if (!ZERO1.equals(digitsInt)) {
        throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_STD_OVERFLOW_ERROR, String.valueOf(precision));
      }
      leadingInt = 0;
      digitsList.remove(0);
    } else if (specIntSize > intSize) {
      // fill zeros
      while (specIntSize > intSize) {
        bin[binIndex++] = (byte) mask;
        specIntSize--;
      }
    }

    if (specDigitsFrac < digitsFracLen) {
      // TODO ErrOverflow
      throw DBException.get(ErrorModule.ENGINE, ErrorCode.ER_STD_OVERFLOW_ERROR, String.valueOf(precision));
    }

    if (leadingInt > 0) {
      int dig2byte = DIG_2_BYTES[leadingInt];
      String digitStr = digitsList.get(wordIndex);
      int digit = (Integer.parseInt(digitStr) % POWERS_10[leadingInt]) ^ mask;
      wordIndex++;
      writeWord(bin, digit, binIndex, dig2byte);
      binIndex += dig2byte;
    }

    for (int stop = wordIndex + wordsInt + wordsFrac; wordIndex < stop; wordIndex++) {
      String digitStr = digitsList.get(wordIndex);
      int digit = Integer.parseInt(digitStr) ^ mask;
      writeWord(bin, digit, binIndex, WORD_SIZE);
      binIndex += WORD_SIZE;
    }
    int totalLength = bin.length;
    if (trailingFrac > 0) {
      int fill0;
      if (totalLength - 1 - binIndex >= WORD_SIZE) {
        fill0 = DIGITS_PER_WORD - trailingFrac;
      } else {
        fill0 = specTrailingDigitsFrac == 0 ? DIGITS_PER_WORD - trailingFrac : specTrailingDigitsFrac - trailingFrac;
      }
      int dig2byte = DIG_2_BYTES[trailingFrac + fill0];
      String digitStr = digitsList.get(wordIndex);
      if (fill0 > 0) {
        digitStr = new StringBuilder(digitStr).append(ZERO_STRS[fill0]).toString();
      }
      int digit = Integer.parseInt(digitStr) ^ mask;
      wordIndex++;
      writeWord(bin, digit, binIndex, dig2byte);
      binIndex += dig2byte;
    }

    if (specFracSize > fracSize) {
      while (specFracSize > fracSize & binIndex < totalLength) {
        bin[binIndex++] = (byte) mask;
        specFracSize--;
      }
    }

    bin[2] ^= 0x80;

    return bin;
  }

  /**
   * convert to bigDecimal from myDecimal byte array
   *
   * @param data
   * @return
   */
  public static BigDecimal decodeFromMySqlDecimal(byte[] data) {
    if (data == null) {
      return null;
    }
    int precision = data[0];
    int frac = data[1];
    int digitsInt = precision - frac;
    int wordsInt = digitsInt / DIGITS_PER_WORD;
    int leadingDigits = digitsInt - wordsInt * DIGITS_PER_WORD;
    int wordsFrac = frac / DIGITS_PER_WORD;
    int trailingDigits = frac - wordsFrac * DIGITS_PER_WORD;
    int wordsIntTo = wordsInt;
    int wordsFracTo = wordsFrac;
    if (leadingDigits > 0) {
      wordsIntTo++;
    }
    if (trailingDigits > 0) {
      wordsFracTo++;
    }

    int binIndex = 2; // skip precision and frac
    int mask = -1;
    if ((data[binIndex] & 0x80) > 0) {
      mask = 0;
    }
    data[binIndex] ^= 0x80;

    int[] wordsIntToArr = new int[wordsIntTo];
    int[] wordsFracArr = new int[wordsFracTo];
    int wordsIndex = 0;
    if (leadingDigits > 0) {
      int dig2byte = DIG_2_BYTES[leadingDigits];
      int digits = readWord(data, binIndex, dig2byte) ^ mask;
      wordsIntToArr[wordsIndex] = digits;
      binIndex += dig2byte;
      wordsIndex++;
    }
    for (int stop = binIndex + wordsInt * WORD_SIZE; binIndex < stop; binIndex += WORD_SIZE) {
      int i = readWord(data, binIndex, WORD_SIZE) ^ mask;
      wordsIntToArr[wordsIndex++] = i;
//      if (wordsIndex > 0 || i != 0) {
//        wordsIndex++;
//      } else {
//        // TODO
//      }
    }

    wordsIndex = 0;
    for (int stop = binIndex + wordsFrac * WORD_SIZE; binIndex < stop; binIndex += WORD_SIZE) {
      int i = readWord(data, binIndex, WORD_SIZE) ^ mask;
      wordsFracArr[wordsIndex++] = i;
    }

    if (trailingDigits > 0) {
      int dig2byte = DIG_2_BYTES[trailingDigits];
      int i = readWord(data, binIndex, dig2byte) ^ mask;
      wordsFracArr[wordsIndex] = i;
    }
    // builder str
    StringBuilder builder = new StringBuilder();
    if (mask < 0) {
      builder.append('-');
    }
    for (int i = 0; i < wordsIntToArr.length; i++) {
      if (i == 0) {
        builder.append(wordsIntToArr[i]);
      } else {
        String word = String.valueOf(wordsIntToArr[i]);
        builder.append(ZERO_STRS[DIGITS_PER_WORD - word.length()]).append(word);
      }
    }
    if (wordsFracTo > 0) {
      builder.append('.');
      int fracIndex = 0;
      for (; fracIndex < wordsFracArr.length; fracIndex++) {
        String word = String.valueOf(wordsFracArr[fracIndex]);
        if (fracIndex == wordsFracArr.length - 1) {
          int fillPrefix = trailingDigits == 0 ? DIGITS_PER_WORD : trailingDigits;
          builder.append(ZERO_STRS[fillPrefix - word.length()]).append(word);
        } else {
          builder.append(ZERO_STRS[DIGITS_PER_WORD - word.length()]).append(word);
        }
      }
    }

    return new BigDecimal(builder.toString());
  }

  /**
   * Get the length based on precision and scale
   * @param precision
   * @param scale
   * @return
   */
  public static int getBytesLength(int precision, int scale) {
    int digitsInt = precision - scale;
    int intWords = digitsInt / DIGITS_PER_WORD;
    int leadingBytes = DIG_2_BYTES[digitsInt - intWords * DIGITS_PER_WORD];
    int fracWords = scale / DIGITS_PER_WORD;
    int trailingBytes = DIG_2_BYTES[scale - fracWords * DIGITS_PER_WORD];
    return intWords * WORD_SIZE + leadingBytes + fracWords * WORD_SIZE + trailingBytes;
  }



  /**
   * encode digit for Big-endian
   * <p>
   * write digit to byte array from the specified index and write size depend on the specified size
   *
   * @param bin
   * @param digit
   * @param index
   * @param size
   */
  private static void writeWord(byte[] bin, int digit, int index, int size) {
    switch (size) {
      case 1:
        bin[index] = (byte) digit;
        break;
      case 2:
        bin[index] = (byte) (digit >> 8);
        bin[++index] = (byte) digit;
        break;
      case 3:
        bin[index] = (byte) (digit >> 16);
        bin[++index] = (byte) (digit >> 8);
        bin[++index] = (byte) digit;
        break;
      case 4:
        bin[index] = (byte) (digit >> 24);
        bin[++index] = (byte) (digit >> 16);
        bin[++index] = (byte) (digit >> 8);
        bin[++index] = (byte) digit;
        break;
      default:
        throw new IllegalArgumentException(String.format("size[%d] is not support", size));
    }
  }

  /**
   * decode binary to int
   * <p>
   * read data from specified index and read size depend on specified size
   *
   * @param data
   * @param index
   * @param size
   * @return
   */
  private static int readWord(byte[] data, int index, int size) {
    int x;
    switch (size) {
      case 1:
        x = data[index];
        break;
      case 2:
        x = data[index++] << 8 | data[index] & 0xFF;
        break;
      case 3:
        x = data[index++] << 16 | data[index++] << 8 & 0xFF00 | data[index] & 0xFF;
        break;
      case 4:
        x = data[index++] << 24 | data[index++] << 16 & 0xFF0000 | data[index++] << 8 & 0xFF00 | data[index] & 0xFF;
        break;
      default:
        throw new IllegalArgumentException(String.format("size[%d] is not support", size));
    }
    return x;
  }
}
