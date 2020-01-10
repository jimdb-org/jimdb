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
package io.jimdb.common.utils.lang;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
public final class IOUtil {
  private static final Logger LOG = LoggerFactory.getLogger(IOUtil.class);

  private IOUtil() {
  }

  @SuppressFBWarnings("EXS_EXCEPTION_SOFTENING_NO_CONSTRAINTS")
  public static Properties loadResource(final String file) {
    final Properties props = new Properties();

    URL url = IOUtil.class.getClassLoader().getResource(file);
    if (url == null) {
      throw new RuntimeException(String.format("not found resource file '%s'", file));
    }

    try (InputStream resource = url.openStream();
         InputStreamReader reader = new InputStreamReader(resource, StandardCharsets.UTF_8)) {
      props.load(reader);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }

    return props;
  }
}
