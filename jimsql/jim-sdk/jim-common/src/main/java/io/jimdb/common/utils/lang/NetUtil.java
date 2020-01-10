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

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketAddress;
import java.util.Enumeration;

/**
 * @version V1.0
 */
public final class NetUtil {
  private NetUtil() {
  }

  public static String getIP() {
    try {
      Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
      while (networkInterfaces.hasMoreElements()) {
        NetworkInterface networkInterface = networkInterfaces.nextElement();
        Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
        while (inetAddresses.hasMoreElements()) {
          InetAddress inetAddress = inetAddresses.nextElement();
          if (inetAddress instanceof Inet4Address) {
            if (!inetAddress.isLoopbackAddress()) {
              return inetAddress.getHostAddress();
            }
          }
        }
      }
    } catch (Exception e) {
    }

    try {
      return InetAddress.getByName(null).getHostAddress();
    } catch (Exception e) {
    }
    return "127.0.0.1";
  }

  public static byte[] getMacAddress() {
    byte[] address = null;
    try {
      Enumeration<NetworkInterface> nis = NetworkInterface.getNetworkInterfaces();
      if (nis != null) {
        while (nis.hasMoreElements()) {
          NetworkInterface ni = nis.nextElement();
          if (!ni.isLoopback()) {
            address = ni.getHardwareAddress();
            if (validAddress(address)) {
              break;
            }
            address = null;
          }
        }
      }
    } catch (Exception ex) {
      address = null;
    }

    if (address == null) {
      address = randomAddress();
    }

    byte[] dummy = new byte[6];
    RandomHolder.INSTANCE.nextBytes(dummy);
    for (int i = 0; i < 6; ++i) {
      dummy[i] ^= address[i];
    }
    return dummy;
  }

  public static String toIP(final SocketAddress address) {
    if (address == null) {
      return "";
    }

    if (address instanceof InetSocketAddress) {
      InetSocketAddress isa = (InetSocketAddress) address;
      StringBuilder builder = new StringBuilder(50);
      builder.append(isa.getAddress().getHostAddress()).append(':').append(isa.getPort());
      return builder.toString();
    } else {
      return address.toString();
    }
  }

  private static byte[] randomAddress() {
    byte[] dummy = new byte[6];
    RandomHolder.INSTANCE.nextBytes(dummy);
    dummy[0] |= (byte) 0x01;
    return dummy;
  }

  private static boolean validAddress(byte[] address) {
    if (address == null || address.length != 6) {
      return false;
    }

    for (byte b : address) {
      if (b != 0x00) {
        return true;
      }
    }
    return false;
  }
}
