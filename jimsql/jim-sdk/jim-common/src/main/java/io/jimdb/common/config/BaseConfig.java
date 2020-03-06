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
package io.jimdb.common.config;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base configuration setup
 */
public abstract class BaseConfig {
  private static final Logger LOG = LoggerFactory.getLogger(BaseConfig.class);

  protected final Properties props = new Properties();

  public BaseConfig(final Properties props) {
    if (props == null || props.isEmpty()) {
      LOG.error("the configuration property is not specified and the system default value is used");
      return;
    }

    this.props.putAll(props);
  }

  public final boolean getBoolean(String key, boolean defaultValue) {
    final String value = props.getProperty(key);

    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Boolean.parseBoolean(value);
  }

  public final int getInt(String key, int defaultValue) {
    final String value = props.getProperty(key);

    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Integer.parseInt(value);
  }

  public final long getLong(String key, long defaultValue) {
    final String value = props.getProperty(key);

    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Long.parseLong(value);
  }

  public final float getFloat(String key, float defaultValue) {
    final String value = props.getProperty(key);

    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Float.parseFloat(value);
  }

  public final double getDouble(String key, double defaultValue) {
    final String value = props.getProperty(key);

    if (StringUtils.isEmpty(value)) {
      return defaultValue;
    }
    return Double.parseDouble(value);
  }

  public final String getString(String key, String defaultValue) {
    return props.getProperty(key, defaultValue);
  }
}
