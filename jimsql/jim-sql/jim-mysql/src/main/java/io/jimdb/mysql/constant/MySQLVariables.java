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
package io.jimdb.mysql.constant;

import java.util.HashMap;
import java.util.Map;

import io.jimdb.core.variable.SysVariable;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * @version V1.0
 */
@SuppressFBWarnings("PMB_POSSIBLE_MEMORY_BLOAT")
public final class MySQLVariables {
  private static final Map<String, SysVariable> SYS_VARS = new HashMap<>();

  // CHARACTER_SET_CONNECTION is the name for character_set_connection system variable.
  public static final String CHARACTER_SET_CONNECTION = "character_set_connection";
  // COLLATION_CONNECTION is the name for collation_connection system variable.
  public static final String COLLATION_CONNECTION = "collation_connection";
  // CHARACTER_SET_DATABASE is the name for character_set_database system variable.
  public static final String CHARACTER_SET_DATABASE = "character_set_database";
  // COLLATION_DATABASE is the name for collation_database system variable.
  public static final String COLLATION_DATABASE = "collation_database";
  // GENERAL_LOG is the name for 'general_log' system variable.
  public static final String GENERAL_LOG = "general_log";
  // AVOID_TEMPORAL_UPGRADE is the name for 'avoid_temporal_upgrade' system variable.
  public static final String AVOID_TEMPORAL_UPGRADE = "avoid_temporal_upgrade";
  // MAX_PREPARED_STMT_COUNT is the name for 'max_prepared_stmt_count' system variable.
  public static final String MAX_PREPARED_STMT_COUNT = "max_prepared_stmt_count";
  // BIG_TABLES is the name for 'big_tables' system variable.
  public static final String BIG_TABLES = "big_tables";
  // CHECK_PROXY_USERS is the name for 'check_proxy_users' system variable.
  public static final String CHECK_PROXY_USERS = "check_proxy_users";
  // CORE_FILE is the name for 'core_file' system variable.
  public static final String CORE_FILE = "core_file";
  // DEFAULT_WEEK_FORMAT is the name for 'default_week_format' system variable.
  public static final String DEFAULT_WEEK_FORMAT = "default_week_format";
  // GROUP_CONCAT_MAX_LEN is the name for 'group_concat_max_len' system variable.
  public static final String GROUP_CONCAT_MAX_LEN = "group_concat_max_len";
  // DELAY_KEY_WRITE is the name for 'delay_key_write' system variable.
  public static final String DELAY_KEY_WRITE = "delay_key_write";
  // END_MARKERS_IN_JSON is the name for 'end_markers_in_json' system variable.
  public static final String END_MARKERS_IN_JSON = "end_markers_in_json";
  // INNODB_COMMIT_CONCURRENCY is the name for 'innodb_commit_concurrency' system variable.
  public static final String INNODB_COMMIT_CONCURRENCY = "innodb_commit_concurrency";
  // INNODB_FAST_SHUTDOWN is the name for 'innodb_fast_shutdown' system variable.
  public static final String INNODB_FAST_SHUTDOWN = "innodb_fast_shutdown";
  // INNODB_LOCK_WAIT_TIMEOUT is the name for 'innodb_lock_wait_timeout' system variable.
  public static final String INNODB_LOCK_WAIT_TIMEOUT = "innodb_lock_wait_timeout";
  // SQL_LOG_BIN is the name for 'sql_log_bin' system variable.
  public static final String SQL_LOG_BIN = "sql_log_bin";
  // LOG_BIN is the name for 'log_bin' system variable.
  public static final String LOG_BIN = "log_bin";
  // MAX_SORT_LENGTH is the name for 'max_sort_length' system variable.
  public static final String MAX_SORT_LENGTH = "max_sort_length";
  // MAX_SP_RECURSION_DEPTH is the name for 'max_sp_recursion_depth' system variable.
  public static final String MAX_SP_RECURSION_DEPTH = "max_sp_recursion_depth";
  // MAX_USER_CONNECTIONS is the name for 'max_user_connections' system variable.
  public static final String MAX_USER_CONNECTIONS = "max_user_connections";
  // OFFLINE_MODE is the name for 'offline_mode' system variable.
  public static final String OFFLINE_MODE = "offline_mode";
  // INTERACTIVE_TIMEOUT is the name for 'interactive_timeout' system variable.
  public static final String INTERACTIVE_TIMEOUT = "interactive_timeout";
  // FLUSH_TIME is the name for 'flush_time' system variable.
  public static final String FLUSH_TIME = "flush_time";
  // PSEUDO_SLAVE_MODE is the name for 'pseudo_slave_mode' system variable.
  public static final String PSEUDO_SLAVE_MODE = "pseudo_slave_mode";
  // LOW_PRIORITY_UPDATES is the name for 'low_priority_updates' system variable.
  public static final String LOW_PRIORITY_UPDATES = "low_priority_updates";
  // SESSION_TRACK_GTIDS is the name for 'session_track_gtids' system variable.
  public static final String SESSION_TRACK_GTIDS = "session_track_gtids";
  // OLD_PASSWORDS is the name for 'old_passwords' system variable.
  public static final String OLD_PASSWORDS = "old_passwords";
  // MAX_CONNECTIONS is the name for 'max_connections' system variable.
  public static final String MAX_CONNECTIONS = "max_connections";
  // SKIP_NAME_RESOLVE is the name for 'skip_name_resolve' system variable.
  public static final String SKIP_NAME_RESOLVE = "skip_name_resolve";
  // FOREIGN_KEY_CHECKS is the name for 'foreign_key_checks' system variable.
  public static final String FOREIGN_KEY_CHECKS = "foreign_key_checks";
  // SQL_SAFE_UPDATES is the name for 'sql_safe_updates' system variable.
  public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
  // WARNING_COUNT is the name for 'warning_count' system variable.
  public static final String WARNING_COUNT = "warning_count";
  // ERROR_COUNT is the name for 'error_count' system variable.
  public static final String ERROR_COUNT = "error_count";
  // SQL_SELECT_LIMIT is the name for 'sql_select_limit' system variable.
  public static final String SQL_SELECT_LIMIT = "sql_select_limit";
  // MAX_CONNECT_ERRORS is the name for 'max_connect_errors' system variable.
  public static final String MAX_CONNECT_ERRORS = "max_connect_errors";
  // TABLE_DEFINITION_CACHE is the name for 'table_definition_cache' system variable.
  public static final String TABLE_DEFINITION_CACHE = "table_definition_cache";
  // TMP_TABLE_SIZE is the name for 'tmp_table_size' system variable.
  public static final String TMP_TABLE_SIZE = "tmp_table_size";
  // CONNECT_TIMEOUT is the name for 'connect_timeout' system variable.
  public static final String CONNECT_TIMEOUT = "connect_timeout";
  // SYNC_BINLOG is the name for 'sync_binlog' system variable.
  public static final String SYNC_BINLOG = "sync_binlog";
  // BLOCK_ENCRYPTION_MODE is the name for 'block_encryption_mode' system variable.
  public static final String BLOCK_ENCRYPTION_MODE = "block_encryption_mode";
  // WAIT_TIMEOUT is the name for 'wait_timeout' system variable.
  public static final String WAIT_TIMEOUT = "wait_timeout";
  // VALIDATE_PASSWORD_NUMBER_COUNT is the name of 'validate_password_number_count' system variable.
  public static final String VALIDATE_PASSWORD_NUMBER_COUNT = "validate_password_number_count";
  // VALIDATE_PASSWORD_LENGTH is the name of 'validate_password_length' system variable.
  public static final String VALIDATE_PASSWORD_LENGTH = "validate_password_length";
  // PLUGIN_DIR is the name of 'plugin_dir' system variable.
  public static final String PLUGIN_DIR = "plugin_dir";
  // PLUGIN_LOAD is the name of 'plugin_load' system variable.
  public static final String PLUGIN_LOAD = "plugin_load";
  // PORT is the name for 'port' system variable.
  public static final String PORT = "port";
  // DATA_DIR is the name for 'datadir' system variable.
  public static final String DATA_DIR = "datadir";
  // SOCKET is the name for 'socket' system variable.
  public static final String SOCKET = "socket";

  public static final String SQL_MODE = "sql_mode";

  public static final String AUTOCOMMIT_VAR = "autocommit";

  public static final String CHARACTER_SET_RESULTS = "character_set_results";

  public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";

  public static final String TIME_ZONE = "time_zone";

  public static final String TX_ISOLATION = "tx_isolation";

  public static final String TRANSACTION_ISOLATION = "transaction_isolation";

  public static final String TX_ISOLATION_ONE_SHOT = "tx_isolation_one_shot";

  public static final String MYSQL_SERVER_ENCODING = "mysql_server_encoding";

  public static final String MYSQL_SERVER_VERSION = "version";

  public static final String PROTOCOL_VERSION = "protocol_version";

  public static final String MAX_EXECUTION_TIME = "max_execution_time";

  static {
    initSysVariables();
    initSysVariablesAppend();
  }

  private MySQLVariables() {
  }

  public static SysVariable getVariable(String name) {
    return SYS_VARS.get(name);
  }

  public static void addVariable(String name, SysVariable var) {
    SYS_VARS.put(name.toLowerCase(), var);
  }

  private static void initSysVariables() {
    addVariable("gtid_mode", new SysVariable(SysVariable.SCOPE_GLOBAL, "gtid_mode", "OFF"));
    addVariable(FLUSH_TIME, new SysVariable(SysVariable.SCOPE_GLOBAL, FLUSH_TIME, "0"));
    addVariable(PSEUDO_SLAVE_MODE, new SysVariable(SysVariable.SCOPE_SESSION, PSEUDO_SLAVE_MODE, ""));
    addVariable("performance_schema_max_mutex_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_mutex_classes", "200"));
    addVariable(LOW_PRIORITY_UPDATES, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, LOW_PRIORITY_UPDATES, "0"));
    addVariable(SESSION_TRACK_GTIDS, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, SESSION_TRACK_GTIDS, "OFF"));
    addVariable("ndbinfo_max_rows", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndbinfo_max_rows", ""));
    addVariable("ndb_index_stat_option", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_index_stat_option", ""));
    addVariable(OLD_PASSWORDS, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, OLD_PASSWORDS, "0"));
    addVariable("innodb_version", new SysVariable(SysVariable.SCOPE_NONE, "innodb_version", "5.6.25"));
    addVariable(MAX_CONNECTIONS, new SysVariable(SysVariable.SCOPE_GLOBAL, MAX_CONNECTIONS, "151"));
    addVariable(BIG_TABLES, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, BIG_TABLES, "0"));
    addVariable("skip_external_locking", new SysVariable(SysVariable.SCOPE_NONE, "skip_external_locking", "ON"));
    addVariable("slave_pending_jobs_size_max", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_pending_jobs_size_max", "16777216"));
    addVariable("innodb_sync_array_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_sync_array_size", "1"));
    addVariable("rand_seed2", new SysVariable(SysVariable.SCOPE_SESSION, "rand_seed2", ""));
    addVariable("validate_password_check_user_name", new SysVariable(SysVariable.SCOPE_GLOBAL, "validate_password_check_user_name", "OFF"));
    addVariable("validate_password_number_count", new SysVariable(SysVariable.SCOPE_GLOBAL, "validate_password_number_count", "1"));
    addVariable("gtid_next", new SysVariable(SysVariable.SCOPE_SESSION, "gtid_next", ""));
    addVariable(SQL_SELECT_LIMIT, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, SQL_SELECT_LIMIT, "18446744073709551615"));
    addVariable("ndb_show_foreign_key_mock_tables", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_show_foreign_key_mock_tables", ""));
    addVariable("multi_range_count", new SysVariable(SysVariable.SCOPE_NONE, "multi_range_count", "256"));
    addVariable(DEFAULT_WEEK_FORMAT, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, DEFAULT_WEEK_FORMAT, "0"));
    addVariable("binlog_error_action", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlog_error_action", "IGNORE_ERROR"));
    addVariable("slave_transaction_retries", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_transaction_retries", "10"));
    addVariable("default_storage_engine", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "default_storage_engine", "InnoDB"));
    addVariable("ft_query_expansion_limit", new SysVariable(SysVariable.SCOPE_NONE, "ft_query_expansion_limit", "20"));
    addVariable(MAX_CONNECT_ERRORS, new SysVariable(SysVariable.SCOPE_GLOBAL, MAX_CONNECT_ERRORS, "100"));
    addVariable(SYNC_BINLOG, new SysVariable(SysVariable.SCOPE_GLOBAL, SYNC_BINLOG, "0"));
    addVariable("max_digest_length", new SysVariable(SysVariable.SCOPE_NONE, "max_digest_length", "1024"));
    addVariable("performance_schema_max_table_handles", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_table_handles", "4000"));
    addVariable(INNODB_FAST_SHUTDOWN, new SysVariable(SysVariable.SCOPE_GLOBAL, INNODB_FAST_SHUTDOWN, "1"));
    addVariable("ft_max_word_len", new SysVariable(SysVariable.SCOPE_NONE, "ft_max_word_len", "84"));
    addVariable("log_backward_compatible_user_definitions", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_backward_compatible_user_definitions", ""));
    addVariable("lc_messages_dir", new SysVariable(SysVariable.SCOPE_NONE, "lc_messages_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/"));
    addVariable("ft_boolean_syntax", new SysVariable(SysVariable.SCOPE_GLOBAL, "ft_boolean_syntax", "+ -><()~*:\"\"&|"));
    addVariable(TABLE_DEFINITION_CACHE, new SysVariable(SysVariable.SCOPE_GLOBAL, TABLE_DEFINITION_CACHE, "-1"));
    addVariable(SKIP_NAME_RESOLVE, new SysVariable(SysVariable.SCOPE_NONE, SKIP_NAME_RESOLVE, "0"));
    addVariable("performance_schema_max_file_handles", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_file_handles", "32768"));
    addVariable("transaction_allow_batching", new SysVariable(SysVariable.SCOPE_SESSION, "transaction_allow_batching", ""));
    addVariable(SQL_MODE, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, SQL_MODE, "SQL99"));
    addVariable("performance_schema_max_statement_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_statement_classes", "168"));
    addVariable("server_id", new SysVariable(SysVariable.SCOPE_GLOBAL, "server_id", "0"));
    addVariable("innodb_flushing_avg_loops", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_flushing_avg_loops", "30"));
    addVariable(TMP_TABLE_SIZE, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, TMP_TABLE_SIZE, "16777216"));
    addVariable("innodb_max_purge_lag", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_max_purge_lag", "0"));
    addVariable("preload_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "preload_buffer_size", "32768"));
    addVariable("slave_checkpoint_period", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_checkpoint_period", "300"));
    addVariable(CHECK_PROXY_USERS, new SysVariable(SysVariable.SCOPE_GLOBAL, CHECK_PROXY_USERS, "0"));
    addVariable("have_query_cache", new SysVariable(SysVariable.SCOPE_NONE, "have_query_cache", "YES"));
    addVariable("innodb_flush_log_at_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_flush_log_at_timeout", "1"));
    addVariable("innodb_max_undo_log_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_max_undo_log_size", ""));
    addVariable("range_alloc_block_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "range_alloc_block_size", "4096"));
    addVariable(CONNECT_TIMEOUT, new SysVariable(SysVariable.SCOPE_GLOBAL, CONNECT_TIMEOUT, "10"));
    addVariable("collation_server", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "collation_server", ""));
    addVariable("have_rtree_keys", new SysVariable(SysVariable.SCOPE_NONE, "have_rtree_keys", "YES"));
    addVariable("innodb_old_blocks_pct", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_old_blocks_pct", "37"));
    addVariable("innodb_file_format", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_file_format", "Antelope"));
    addVariable("innodb_compression_failure_threshold_pct", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_compression_failure_threshold_pct", "5"));
    addVariable("performance_schema_events_waits_history_long_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_waits_history_long_size", "10000"));
    addVariable("innodb_checksum_algorithm", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_checksum_algorithm", "innodb"));
    addVariable("innodb_ft_sort_pll_degree", new SysVariable(SysVariable.SCOPE_NONE, "innodb_ft_sort_pll_degree", "2"));
    addVariable("thread_stack", new SysVariable(SysVariable.SCOPE_NONE, "thread_stack", "262144"));
    addVariable("relay_log_info_repository", new SysVariable(SysVariable.SCOPE_GLOBAL, "relay_log_info_repository", "FILE"));
    addVariable(SQL_LOG_BIN, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, SQL_LOG_BIN, "1"));
    addVariable("super_read_only", new SysVariable(SysVariable.SCOPE_GLOBAL, "super_read_only", "OFF"));
    addVariable("max_delayed_threads", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_delayed_threads", "20"));
    addVariable("protocol_version", new SysVariable(SysVariable.SCOPE_NONE, "protocol_version", "0x0A"));
    addVariable("new", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "new", "OFF"));
    addVariable("myisam_sort_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "myisam_sort_buffer_size", "8388608"));
    addVariable("optimizer_trace_offset", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_trace_offset", "-1"));
    addVariable("innodb_buffer_pool_dump_at_shutdown", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_dump_at_shutdown", "OFF"));
    addVariable("sql_notes", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_notes", "ON"));
    addVariable("innodb_cmp_per_index_enabled", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_cmp_per_index_enabled", "OFF"));
    addVariable("innodb_ft_server_stopword_table", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_server_stopword_table", ""));
    addVariable("performance_schema_max_file_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_file_instances", "7693"));
    addVariable("log_output", new SysVariable(SysVariable.SCOPE_NONE, "log_output", "FILE"));
    addVariable("binlog_group_commit_sync_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_group_commit_sync_delay", ""));
    addVariable("binlog_group_commit_sync_no_delay_count", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_group_commit_sync_no_delay_count", ""));
    addVariable("have_crypt", new SysVariable(SysVariable.SCOPE_NONE, "have_crypt", "YES"));
    addVariable("innodb_log_write_ahead_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_log_write_ahead_size", ""));
    addVariable("innodb_log_group_home_dir", new SysVariable(SysVariable.SCOPE_NONE, "innodb_log_group_home_dir", "./"));
    addVariable("performance_schema_events_statements_history_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_statements_history_size", "10"));
    addVariable(GENERAL_LOG, new SysVariable(SysVariable.SCOPE_GLOBAL, GENERAL_LOG, "0"));
    addVariable("validate_password_dictionary_file", new SysVariable(SysVariable.SCOPE_GLOBAL, "validate_password_dictionary_file", ""));
    addVariable("binlog_order_commits", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_order_commits", "ON"));
    addVariable("master_verify_checksum", new SysVariable(SysVariable.SCOPE_GLOBAL, "master_verify_checksum", "OFF"));
    addVariable("key_cache_division_limit", new SysVariable(SysVariable.SCOPE_GLOBAL, "key_cache_division_limit", "100"));
    addVariable("rpl_semi_sync_master_trace_level", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_trace_level", ""));
    addVariable("max_insert_delayed_threads", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_insert_delayed_threads", "20"));
    addVariable("performance_schema_session_connect_attrs_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_session_connect_attrs_size", "512"));
    addVariable("time_zone", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "time_zone", "SYSTEM"));
    addVariable("innodb_max_dirty_pages_pct", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_max_dirty_pages_pct", "75"));
    addVariable("innodb_file_per_table", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_file_per_table", "ON"));
    addVariable("innodb_log_compressed_pages", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_log_compressed_pages", "ON"));
    addVariable("master_info_repository", new SysVariable(SysVariable.SCOPE_GLOBAL, "master_info_repository", "FILE"));
    addVariable("rpl_stop_slave_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_stop_slave_timeout", "31536000"));
    addVariable("skip_networking", new SysVariable(SysVariable.SCOPE_NONE, "skip_networking", "OFF"));
    addVariable("innodb_monitor_reset", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_monitor_reset", ""));
    addVariable("have_ssl", new SysVariable(SysVariable.SCOPE_NONE, "have_ssl", "DISABLED"));
    addVariable("have_openssl", new SysVariable(SysVariable.SCOPE_NONE, "have_openssl", "DISABLED"));
    addVariable("ssl_ca", new SysVariable(SysVariable.SCOPE_NONE, "ssl_ca", ""));
    addVariable("ssl_cert", new SysVariable(SysVariable.SCOPE_NONE, "ssl_cert", ""));
    addVariable("ssl_key", new SysVariable(SysVariable.SCOPE_NONE, "ssl_key", ""));
    addVariable("ssl_cipher", new SysVariable(SysVariable.SCOPE_NONE, "ssl_cipher", ""));
    addVariable("tls_version", new SysVariable(SysVariable.SCOPE_NONE, "tls_version", "TLSv1,TLSv1.1,TLSv1.2"));
    addVariable("system_time_zone", new SysVariable(SysVariable.SCOPE_NONE, "system_time_zone", "CST"));
    addVariable("innodb_print_all_deadlocks", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_print_all_deadlocks", "OFF"));
    addVariable("innodb_autoinc_lock_mode", new SysVariable(SysVariable.SCOPE_NONE, "innodb_autoinc_lock_mode", "1"));
    addVariable("slave_net_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_net_timeout", "3600"));
    addVariable("key_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "key_buffer_size", "8388608"));
    addVariable(FOREIGN_KEY_CHECKS, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, FOREIGN_KEY_CHECKS, "OFF"));
    addVariable("host_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "host_cache_size", "279"));
    addVariable(DELAY_KEY_WRITE, new SysVariable(SysVariable.SCOPE_GLOBAL, DELAY_KEY_WRITE, "ON"));
    addVariable("metadata_locks_cache_size", new SysVariable(SysVariable.SCOPE_NONE, "metadata_locks_cache_size", "1024"));
    addVariable("innodb_force_recovery", new SysVariable(SysVariable.SCOPE_NONE, "innodb_force_recovery", "0"));
    addVariable("innodb_file_format_max", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_file_format_max", "Antelope"));
    addVariable("debug", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "debug", ""));
    addVariable("log_warnings", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_warnings", "1"));
    addVariable("innodb_strict_mode", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "innodb_strict_mode", "OFF"));
    addVariable("innodb_rollback_segments", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_rollback_segments", "128"));
    addVariable("join_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "join_buffer_size", "262144"));
    addVariable("innodb_mirrored_log_groups", new SysVariable(SysVariable.SCOPE_NONE, "innodb_mirrored_log_groups", "1"));
    addVariable("max_binlog_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_binlog_size", "1073741824"));
    addVariable("sync_master_info", new SysVariable(SysVariable.SCOPE_GLOBAL, "sync_master_info", "10000"));
    addVariable("concurrent_insert", new SysVariable(SysVariable.SCOPE_GLOBAL, "concurrent_insert", "AUTO"));
    addVariable("innodb_adaptive_hash_index", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_adaptive_hash_index", "ON"));
    addVariable("innodb_ft_enable_stopword", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_enable_stopword", "ON"));
    addVariable("general_log_file", new SysVariable(SysVariable.SCOPE_GLOBAL, "general_log_file", "/usr/local/mysql/data/localhost.log"));
    addVariable("innodb_support_xa", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "innodb_support_xa", "ON"));
    addVariable("innodb_compression_level", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_compression_level", "6"));
    addVariable("innodb_file_format_check", new SysVariable(SysVariable.SCOPE_NONE, "innodb_file_format_check", "ON"));
    addVariable("myisam_mmap_size", new SysVariable(SysVariable.SCOPE_NONE, "myisam_mmap_size", "18446744073709551615"));
    addVariable("init_slave", new SysVariable(SysVariable.SCOPE_GLOBAL, "init_slave", ""));
    addVariable("innodb_buffer_pool_instances", new SysVariable(SysVariable.SCOPE_NONE, "innodb_buffer_pool_instances", "8"));
    addVariable(BLOCK_ENCRYPTION_MODE, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, BLOCK_ENCRYPTION_MODE, "aes-128-ecb"));
    addVariable("max_length_for_sort_data", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_length_for_sort_data", "1024"));
    addVariable("character_set_system", new SysVariable(SysVariable.SCOPE_NONE, "character_set_system", "utf8"));
    addVariable(INTERACTIVE_TIMEOUT, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, INTERACTIVE_TIMEOUT, "28800"));
    addVariable("innodb_optimize_fulltext_only", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_optimize_fulltext_only", "OFF"));
    addVariable("character_sets_dir", new SysVariable(SysVariable.SCOPE_NONE, "character_sets_dir", "/usr/local/mysql-5.6.25-osx10.8-x86_64/share/charsets/"));
    addVariable("query_cache_type", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "query_cache_type", "OFF"));
    addVariable("innodb_rollback_on_timeout", new SysVariable(SysVariable.SCOPE_NONE, "innodb_rollback_on_timeout", "OFF"));
    addVariable("query_alloc_block_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "query_alloc_block_size", "8192"));
    addVariable("slave_compressed_protocol", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_compressed_protocol", "OFF"));
    addVariable("init_connect", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "init_connect", ""));
    addVariable("rpl_semi_sync_slave_trace_level", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_slave_trace_level", ""));
    addVariable("have_compress", new SysVariable(SysVariable.SCOPE_NONE, "have_compress", "YES"));
    addVariable("thread_concurrency", new SysVariable(SysVariable.SCOPE_NONE, "thread_concurrency", "10"));
    addVariable("query_prealloc_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "query_prealloc_size", "8192"));
    addVariable("relay_log_space_limit", new SysVariable(SysVariable.SCOPE_NONE, "relay_log_space_limit", "0"));
    addVariable("performance_schema_max_thread_classes", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, MAX_USER_CONNECTIONS, "0"));
    addVariable("performance_schema_max_thread_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_thread_classes", "50"));
    addVariable("innodb_api_trx_level", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_api_trx_level", "0"));
    addVariable("disconnect_on_expired_password", new SysVariable(SysVariable.SCOPE_NONE, "disconnect_on_expired_password", "ON"));
    addVariable("performance_schema_max_file_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_file_classes", "50"));
    addVariable("expire_logs_days", new SysVariable(SysVariable.SCOPE_GLOBAL, "expire_logs_days", "0"));
    addVariable("binlog_rows_query_log_events", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlog_rows_query_log_events", "OFF"));
    addVariable("default_password_lifetime", new SysVariable(SysVariable.SCOPE_GLOBAL, "default_password_lifetime", ""));
    addVariable("pid_file", new SysVariable(SysVariable.SCOPE_NONE, "pid_file", "/usr/local/mysql/data/localhost.pid"));
    addVariable("innodb_undo_tablespaces", new SysVariable(SysVariable.SCOPE_NONE, "innodb_undo_tablespaces", "0"));
    addVariable("performance_schema_accounts_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_accounts_size", "100"));
    addVariable("max_error_count", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_error_count", "64"));
    addVariable("max_write_lock_count", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_write_lock_count", "18446744073709551615"));
    addVariable("performance_schema_max_socket_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_socket_instances", "322"));
    addVariable("performance_schema_max_table_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_table_instances", "12500"));
    addVariable("innodb_stats_persistent_sample_pages", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_persistent_sample_pages", "20"));
    addVariable("show_compatibility_56", new SysVariable(SysVariable.SCOPE_GLOBAL, "show_compatibility_56", ""));
    addVariable("log_slow_slave_statements", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_slow_slave_statements", "OFF"));
    addVariable("innodb_open_files", new SysVariable(SysVariable.SCOPE_NONE, "innodb_open_files", "2000"));
    addVariable("innodb_spin_wait_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_spin_wait_delay", "6"));
    addVariable("thread_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "thread_cache_size", "9"));
    addVariable("log_slow_admin_statements", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_slow_admin_statements", "OFF"));
    addVariable("innodb_checksums", new SysVariable(SysVariable.SCOPE_NONE, "innodb_checksums", "ON"));
    addVariable("hostname", new SysVariable(SysVariable.SCOPE_NONE, "hostname", ""));
    addVariable("auto_increment_offset", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "auto_increment_offset", "1"));
    addVariable("ft_stopword_file", new SysVariable(SysVariable.SCOPE_NONE, "ft_stopword_file", "(built-in)"));
    addVariable("innodb_max_dirty_pages_pct_lwm", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_max_dirty_pages_pct_lwm", "0"));
    addVariable("log_queries_not_using_indexes", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_queries_not_using_indexes", "OFF"));
    addVariable("timestamp", new SysVariable(SysVariable.SCOPE_SESSION, "timestamp", ""));
    addVariable("query_cache_wlock_invalidate", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "query_cache_wlock_invalidate", "OFF"));
    addVariable("sql_buffer_result", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_buffer_result", "OFF"));
    addVariable("character_set_filesystem", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "character_set_filesystem", "binary"));
    addVariable("collation_database", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "collation_database", ""));
    addVariable("auto_increment_increment", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "auto_increment_increment", "1"));
    addVariable("max_heap_table_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_heap_table_size", "16777216"));
    addVariable("div_precision_increment", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "div_precision_increment", "4"));
    addVariable("innodb_lru_scan_depth", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_lru_scan_depth", "1024"));
    addVariable("innodb_purge_rseg_truncate_frequency", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_purge_rseg_truncate_frequency", ""));
    addVariable("sql_auto_is_null", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_auto_is_null", "OFF"));
    addVariable("innodb_api_enable_binlog", new SysVariable(SysVariable.SCOPE_NONE, "innodb_api_enable_binlog", "OFF"));
    addVariable("innodb_ft_user_stopword_table", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "innodb_ft_user_stopword_table", ""));
    addVariable("server_id_bits", new SysVariable(SysVariable.SCOPE_NONE, "server_id_bits", "32"));
    addVariable("innodb_log_checksum_algorithm", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_log_checksum_algorithm", ""));
    addVariable("innodb_buffer_pool_load_at_startup", new SysVariable(SysVariable.SCOPE_NONE, "innodb_buffer_pool_load_at_startup", "OFF"));
    addVariable("sort_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sort_buffer_size", "262144"));
    addVariable("innodb_flush_neighbors", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_flush_neighbors", "1"));
    addVariable("innodb_use_sys_malloc", new SysVariable(SysVariable.SCOPE_NONE, "innodb_use_sys_malloc", "ON"));
    addVariable(PLUGIN_LOAD, new SysVariable(SysVariable.SCOPE_SESSION, PLUGIN_LOAD, ""));
    addVariable(PLUGIN_DIR, new SysVariable(SysVariable.SCOPE_SESSION, PLUGIN_DIR, "/data/deploy/plugin"));
    addVariable("performance_schema_max_socket_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_socket_classes", "10"));
    addVariable("performance_schema_max_stage_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_stage_classes", "150"));
    addVariable("innodb_purge_batch_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_purge_batch_size", "300"));
    addVariable("have_profiling", new SysVariable(SysVariable.SCOPE_NONE, "have_profiling", "NO"));
    addVariable("slave_checkpoint_group", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_checkpoint_group", "512"));
    addVariable("character_set_client", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "character_set_client", "UTF-8"));
    addVariable("slave_load_tmpdir", new SysVariable(SysVariable.SCOPE_NONE, "slave_load_tmpdir", "/var/tmp/"));
    addVariable("innodb_buffer_pool_dump_now", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_dump_now", "OFF"));
    addVariable("relay_log_purge", new SysVariable(SysVariable.SCOPE_GLOBAL, "relay_log_purge", "ON"));
    addVariable("ndb_distribution", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_distribution", ""));
    addVariable("myisam_data_pointer_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "myisam_data_pointer_size", "6"));
    addVariable("ndb_optimization_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_optimization_delay", ""));
    addVariable("innodb_ft_num_word_optimize", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_num_word_optimize", "2000"));
    addVariable("max_join_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_join_size", "18446744073709551615"));
    addVariable(CORE_FILE, new SysVariable(SysVariable.SCOPE_NONE, CORE_FILE, "0"));
    addVariable("max_seeks_for_key", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_seeks_for_key", "18446744073709551615"));
    addVariable("innodb_log_buffer_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_log_buffer_size", "8388608"));
    addVariable("delayed_insert_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL, "delayed_insert_timeout", "300"));
    addVariable("max_relay_log_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_relay_log_size", "0"));
    addVariable(MAX_SORT_LENGTH, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, MAX_SORT_LENGTH, "1024"));
    addVariable("metadata_locks_hash_instances", new SysVariable(SysVariable.SCOPE_NONE, "metadata_locks_hash_instances", "8"));
    addVariable("ndb_eventbuffer_free_percent", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_eventbuffer_free_percent", ""));
    addVariable("binlog_max_flush_queue_time", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_max_flush_queue_time", "0"));
    addVariable("innodb_fill_factor", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_fill_factor", ""));
    addVariable("log_syslog_facility", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_syslog_facility", ""));
    addVariable("innodb_ft_min_token_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_ft_min_token_size", "3"));
    addVariable("transaction_write_set_extraction", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "transaction_write_set_extraction", ""));
    addVariable("ndb_blob_write_batch_bytes", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_blob_write_batch_bytes", ""));
    addVariable("automatic_sp_privileges", new SysVariable(SysVariable.SCOPE_GLOBAL, "automatic_sp_privileges", "ON"));
    addVariable("innodb_flush_sync", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_flush_sync", ""));
    addVariable("performance_schema_events_statements_history_long_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_statements_history_long_size", "10000"));
    addVariable("innodb_monitor_disable", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_monitor_disable", ""));
    addVariable("innodb_doublewrite", new SysVariable(SysVariable.SCOPE_NONE, "innodb_doublewrite", "ON"));
    addVariable("slave_parallel_type", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_parallel_type", ""));
    addVariable("log_bin_use_v1_row_events", new SysVariable(SysVariable.SCOPE_NONE, "log_bin_use_v1_row_events", "OFF"));
    addVariable("innodb_optimize_point_storage", new SysVariable(SysVariable.SCOPE_SESSION, "innodb_optimize_point_storage", ""));
    addVariable("innodb_api_disable_rowlock", new SysVariable(SysVariable.SCOPE_NONE, "innodb_api_disable_rowlock", "OFF"));
    addVariable("innodb_adaptive_flushing_lwm", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_adaptive_flushing_lwm", "10"));
    addVariable("innodb_log_files_in_group", new SysVariable(SysVariable.SCOPE_NONE, "innodb_log_files_in_group", "2"));
    addVariable("innodb_buffer_pool_load_now", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_load_now", "OFF"));
    addVariable("performance_schema_max_rwlock_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_rwlock_classes", "40"));
    addVariable("binlog_gtid_simple_recovery", new SysVariable(SysVariable.SCOPE_NONE, "binlog_gtid_simple_recovery", "OFF"));
    addVariable(PORT, new SysVariable(SysVariable.SCOPE_NONE, PORT, "4000"));
    addVariable("performance_schema_digests_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_digests_size", "10000"));
    addVariable("profiling", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "profiling", "OFF"));
    addVariable("lower_case_table_names", new SysVariable(SysVariable.SCOPE_NONE, "lower_case_table_names", "2"));
  }

  private static void initSysVariablesAppend() {
    addVariable("rand_seed1", new SysVariable(SysVariable.SCOPE_SESSION, "rand_seed1", ""));
    addVariable("sha256_password_proxy_users", new SysVariable(SysVariable.SCOPE_GLOBAL, "sha256_password_proxy_users", ""));
    addVariable("sql_quote_show_create", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_quote_show_create", "ON"));
    addVariable("binlogging_impossible_mode", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlogging_impossible_mode", "IGNORE_ERROR"));
    addVariable("query_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "query_cache_size", "1048576"));
    addVariable("innodb_stats_transient_sample_pages", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_transient_sample_pages", "8"));
    addVariable("innodb_stats_on_metadata", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_on_metadata", "OFF"));
    addVariable("server_uuid", new SysVariable(SysVariable.SCOPE_NONE, "server_uuid", "d530594e-1c86-11e5-878b-6b36ce6799ca"));
    addVariable("open_files_limit", new SysVariable(SysVariable.SCOPE_NONE, "open_files_limit", "5000"));
    addVariable("ndb_force_send", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_force_send", ""));
    addVariable("skip_show_database", new SysVariable(SysVariable.SCOPE_NONE, "skip_show_database", "OFF"));
    addVariable("log_timestamps", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_timestamps", ""));
    addVariable("version_compile_machine", new SysVariable(SysVariable.SCOPE_NONE, "version_compile_machine", "x86_64"));
    addVariable("slave_parallel_workers", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_parallel_workers", "0"));
    addVariable("event_scheduler", new SysVariable(SysVariable.SCOPE_GLOBAL, "event_scheduler", "OFF"));
    addVariable("ndb_deferred_constraints", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_deferred_constraints", ""));
    addVariable("log_syslog_include_pid", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_syslog_include_pid", ""));
    addVariable("last_insert_id", new SysVariable(SysVariable.SCOPE_SESSION, "last_insert_id", ""));
    addVariable("innodb_ft_cache_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_ft_cache_size", "8000000"));
    addVariable(LOG_BIN, new SysVariable(SysVariable.SCOPE_NONE, LOG_BIN, "0"));
    addVariable("innodb_disable_sort_file_cache", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_disable_sort_file_cache", "OFF"));
    addVariable("log_error_verbosity", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_error_verbosity", ""));
    addVariable("performance_schema_hosts_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_hosts_size", "100"));
    addVariable("innodb_replication_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_replication_delay", "0"));
    addVariable("slow_query_log", new SysVariable(SysVariable.SCOPE_GLOBAL, "slow_query_log", "OFF"));
    addVariable("debug_sync", new SysVariable(SysVariable.SCOPE_SESSION, "debug_sync", ""));
    addVariable("innodb_stats_auto_recalc", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_auto_recalc", "ON"));
    addVariable("timed_mutexes", new SysVariable(SysVariable.SCOPE_GLOBAL, "timed_mutexes", "OFF"));
    addVariable("lc_messages", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "lc_messages", "en_US"));
    addVariable("bulk_insert_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "bulk_insert_buffer_size", "8388608"));
    addVariable("binlog_direct_non_transactional_updates", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlog_direct_non_transactional_updates", "OFF"));
    addVariable("innodb_change_buffering", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_change_buffering", "all"));
    addVariable("sql_big_selects", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_big_selects", "ON"));
    addVariable(CHARACTER_SET_RESULTS, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, CHARACTER_SET_RESULTS, ""));
    addVariable("innodb_max_purge_lag_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_max_purge_lag_delay", "0"));
    addVariable("session_track_schema", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "session_track_schema", ""));
    addVariable("innodb_io_capacity_max", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_io_capacity_max", "2000"));
    addVariable("innodb_autoextend_increment", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_autoextend_increment", "64"));
    addVariable("binlog_format", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlog_format", "STATEMENT"));
    addVariable("optimizer_trace", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_trace", "enabled=off,one_line=off"));
    addVariable("read_rnd_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "read_rnd_buffer_size", "262144"));
    addVariable("version_comment", new SysVariable(SysVariable.SCOPE_NONE, "version_comment", "MySQL Community Server (Apache License 2.0)"));
    addVariable("net_write_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "net_write_timeout", "60"));
    addVariable("innodb_buffer_pool_load_abort", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_load_abort", "OFF"));
    addVariable("tx_isolation", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "tx_isolation", "REPEATABLE-READ"));
    addVariable("transaction_isolation", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "transaction_isolation", "REPEATABLE-READ"));
    addVariable("collation_connection", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "collation_connection", ""));
    addVariable("rpl_semi_sync_master_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_timeout", ""));
    addVariable("transaction_prealloc_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "transaction_prealloc_size", "4096"));
    addVariable("slave_skip_errors", new SysVariable(SysVariable.SCOPE_NONE, "slave_skip_errors", "OFF"));
    addVariable("performance_schema_setup_objects_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_setup_objects_size", "100"));
    addVariable("sync_relay_log", new SysVariable(SysVariable.SCOPE_GLOBAL, "sync_relay_log", "10000"));
    addVariable("innodb_ft_result_cache_limit", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_result_cache_limit", "2000000000"));
    addVariable("innodb_sort_buffer_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_sort_buffer_size", "1048576"));
    addVariable("innodb_ft_enable_diag_print", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_enable_diag_print", "OFF"));
    addVariable("thread_handling", new SysVariable(SysVariable.SCOPE_NONE, "thread_handling", "one-thread-per-connection"));
    addVariable("stored_program_cache", new SysVariable(SysVariable.SCOPE_GLOBAL, "stored_program_cache", "256"));
    addVariable("performance_schema_max_mutex_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_mutex_instances", "15906"));
    addVariable("innodb_adaptive_max_sleep_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_adaptive_max_sleep_delay", "150000"));
    addVariable("large_pages", new SysVariable(SysVariable.SCOPE_NONE, "large_pages", "OFF"));
    addVariable("session_track_system_variables", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "session_track_system_variables", ""));
    addVariable("innodb_change_buffer_max_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_change_buffer_max_size", "25"));
    addVariable("log_bin_trust_function_creators", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_bin_trust_function_creators", "OFF"));
    addVariable("innodb_write_io_threads", new SysVariable(SysVariable.SCOPE_NONE, "innodb_write_io_threads", "4"));
    addVariable("mysql_native_password_proxy_users", new SysVariable(SysVariable.SCOPE_GLOBAL, "mysql_native_password_proxy_users", ""));
    addVariable("read_only", new SysVariable(SysVariable.SCOPE_GLOBAL, "read_only", "OFF"));
    addVariable("large_page_size", new SysVariable(SysVariable.SCOPE_NONE, "large_page_size", "0"));
    addVariable("table_open_cache_instances", new SysVariable(SysVariable.SCOPE_NONE, "table_open_cache_instances", "1"));
    addVariable("innodb_stats_persistent", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_persistent", "ON"));
    addVariable("session_track_state_change", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "session_track_state_change", ""));
    addVariable("optimizer_switch", new SysVariable(SysVariable.SCOPE_NONE, "optimizer_switch", "index_merge=on,index_merge_union=on,index_merge_sort_union=on,"
            + "index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on"));
    addVariable("delayed_queue_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "delayed_queue_size", "1000"));
    addVariable("innodb_read_only", new SysVariable(SysVariable.SCOPE_NONE, "innodb_read_only", "OFF"));
    addVariable("datetime_format", new SysVariable(SysVariable.SCOPE_NONE, "datetime_format", "%Y-%m-%d %H:%i:%s"));
    addVariable("log_syslog", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_syslog", ""));
    addVariable("version", new SysVariable(SysVariable.SCOPE_NONE, "version", "5.6.0"));
    addVariable("transaction_alloc_block_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "transaction_alloc_block_size", "8192"));
    addVariable("sql_slave_skip_counter", new SysVariable(SysVariable.SCOPE_GLOBAL, "sql_slave_skip_counter", "0"));
    addVariable("innodb_large_prefix", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_large_prefix", "OFF"));
    addVariable("performance_schema_max_cond_classes", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_cond_classes", "80"));
    addVariable("innodb_io_capacity", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_io_capacity", "200"));
    addVariable("max_binlog_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_binlog_cache_size", "18446744073709547520"));
    addVariable("ndb_index_stat_enable", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_index_stat_enable", ""));
    addVariable("executed_gtids_compression_period", new SysVariable(SysVariable.SCOPE_GLOBAL, "executed_gtids_compression_period", ""));
    addVariable("time_format", new SysVariable(SysVariable.SCOPE_NONE, "time_format", "%H:%i:%s"));
    addVariable("old_alter_table", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "old_alter_table", "OFF"));
    addVariable("long_query_time", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "long_query_time", "10.000000"));
    addVariable("innodb_use_native_aio", new SysVariable(SysVariable.SCOPE_NONE, "innodb_use_native_aio", "OFF"));
    addVariable("log_throttle_queries_not_using_indexes", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_throttle_queries_not_using_indexes", "0"));
    addVariable("locked_in_memory", new SysVariable(SysVariable.SCOPE_NONE, "locked_in_memory", "OFF"));
    addVariable("innodb_api_enable_mdl", new SysVariable(SysVariable.SCOPE_NONE, "innodb_api_enable_mdl", "OFF"));
    addVariable("binlog_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_cache_size", "32768"));
    addVariable("innodb_compression_pad_pct_max", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_compression_pad_pct_max", "50"));
    addVariable(INNODB_COMMIT_CONCURRENCY, new SysVariable(SysVariable.SCOPE_GLOBAL, INNODB_COMMIT_CONCURRENCY, "0"));
    addVariable("ft_min_word_len", new SysVariable(SysVariable.SCOPE_NONE, "ft_min_word_len", "4"));
    addVariable("enforce_gtid_consistency", new SysVariable(SysVariable.SCOPE_GLOBAL, "enforce_gtid_consistency", "OFF"));
    addVariable("secure_auth", new SysVariable(SysVariable.SCOPE_GLOBAL, "secure_auth", "ON"));
    addVariable("max_tmp_tables", new SysVariable(SysVariable.SCOPE_NONE, "max_tmp_tables", "32"));
    addVariable("innodb_random_read_ahead", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_random_read_ahead", "OFF"));
    addVariable("unique_checks", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "unique_checks", "ON"));
    addVariable("internal_tmp_disk_storage_engine", new SysVariable(SysVariable.SCOPE_GLOBAL, "internal_tmp_disk_storage_engine", ""));
    addVariable("myisam_repair_threads", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "myisam_repair_threads", "1"));
    addVariable("ndb_eventbuffer_max_alloc", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_eventbuffer_max_alloc", ""));
    addVariable("innodb_read_ahead_threshold", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_read_ahead_threshold", "56"));
    addVariable("key_cache_block_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "key_cache_block_size", "1024"));
    addVariable("rpl_semi_sync_slave_enabled", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_slave_enabled", ""));
    addVariable("ndb_recv_thread_cpu_mask", new SysVariable(SysVariable.SCOPE_NONE, "ndb_recv_thread_cpu_mask", ""));
    addVariable("gtid_purged", new SysVariable(SysVariable.SCOPE_GLOBAL, "gtid_purged", ""));
    addVariable("max_binlog_stmt_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_binlog_stmt_cache_size", "18446744073709547520"));
    addVariable("lock_wait_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "lock_wait_timeout", "31536000"));
    addVariable("read_buffer_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "read_buffer_size", "131072"));
    addVariable("innodb_read_io_threads", new SysVariable(SysVariable.SCOPE_NONE, "innodb_read_io_threads", "4"));
    addVariable(MAX_SP_RECURSION_DEPTH, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, MAX_SP_RECURSION_DEPTH, "0"));
    addVariable("ignore_builtin_innodb", new SysVariable(SysVariable.SCOPE_NONE, "ignore_builtin_innodb", "OFF"));
    addVariable("rpl_semi_sync_master_enabled", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_enabled", ""));
    addVariable("slow_query_log_file", new SysVariable(SysVariable.SCOPE_GLOBAL, "slow_query_log_file", "/usr/local/mysql/data/localhost-slow.log"));
    addVariable("innodb_thread_sleep_delay", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_thread_sleep_delay", "10000"));
    addVariable("license", new SysVariable(SysVariable.SCOPE_NONE, "license", "Apache License 2.0"));
    addVariable("innodb_ft_aux_table", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_ft_aux_table", ""));
    addVariable("sql_warnings", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_warnings", "OFF"));
    addVariable("keep_files_on_create", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "keep_files_on_create", "OFF"));
    addVariable("slave_preserve_commit_order", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_preserve_commit_order", ""));
    addVariable("innodb_data_file_path", new SysVariable(SysVariable.SCOPE_NONE, "innodb_data_file_path", "ibdata1:12M:autoextend"));
    addVariable("performance_schema_setup_actors_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_setup_actors_size", "100"));
    addVariable("innodb_additional_mem_pool_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_additional_mem_pool_size", "8388608"));
    addVariable("log_error", new SysVariable(SysVariable.SCOPE_NONE, "log_error", "/usr/local/mysql/data/localhost.err"));
    addVariable("slave_exec_mode", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_exec_mode", "STRICT"));
    addVariable("binlog_stmt_cache_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_stmt_cache_size", "32768"));
    addVariable("relay_log_info_file", new SysVariable(SysVariable.SCOPE_NONE, "relay_log_info_file", "relay-log.info"));
    addVariable("innodb_ft_total_cache_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_ft_total_cache_size", "640000000"));
    addVariable("performance_schema_max_rwlock_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_rwlock_instances", "9102"));
    addVariable("table_open_cache", new SysVariable(SysVariable.SCOPE_GLOBAL, "table_open_cache", "2000"));
    addVariable("log_slave_updates", new SysVariable(SysVariable.SCOPE_NONE, "log_slave_updates", "OFF"));
    addVariable("performance_schema_events_stages_history_long_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_stages_history_long_size", "10000"));
    addVariable("autocommit", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "autocommit", "ON"));
    addVariable("insert_id", new SysVariable(SysVariable.SCOPE_SESSION, "insert_id", ""));
    addVariable("default_tmp_storage_engine", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "default_tmp_storage_engine", "InnoDB"));
    addVariable("optimizer_search_depth", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_search_depth", "62"));
    addVariable("max_points_in_geometry", new SysVariable(SysVariable.SCOPE_GLOBAL, "max_points_in_geometry", ""));
    addVariable("innodb_stats_sample_pages", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_sample_pages", "8"));
    addVariable("profiling_history_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "profiling_history_size", "15"));
    addVariable("character_set_database", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "character_set_database", ""));
    addVariable("have_symlink", new SysVariable(SysVariable.SCOPE_NONE, "have_symlink", "YES"));
    addVariable("storage_engine", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "storage_engine", "InnoDB"));
    addVariable("sql_log_off", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "sql_log_off", "OFF"));
    addVariable("explicit_defaults_for_timestamp", new SysVariable(SysVariable.SCOPE_NONE, "explicit_defaults_for_timestamp", "ON"));
    addVariable("performance_schema_events_waits_history_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_waits_history_size", "10"));
    addVariable("log_syslog_tag", new SysVariable(SysVariable.SCOPE_GLOBAL, "log_syslog_tag", ""));
    addVariable("tx_read_only", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "tx_read_only", "0"));
    addVariable("transaction_read_only", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "transaction_read_only", "0"));
    addVariable("rpl_semi_sync_master_wait_point", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_wait_point", ""));
    addVariable("innodb_undo_log_truncate", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_undo_log_truncate", ""));
    addVariable("simplified_binlog_gtid_recovery", new SysVariable(SysVariable.SCOPE_NONE, "simplified_binlog_gtid_recovery", "OFF"));
    addVariable("innodb_create_intrinsic", new SysVariable(SysVariable.SCOPE_SESSION, "innodb_create_intrinsic", ""));
    addVariable("gtid_executed_compression_period", new SysVariable(SysVariable.SCOPE_GLOBAL, "gtid_executed_compression_period", ""));
    addVariable("ndb_log_empty_epochs", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_log_empty_epochs", ""));
    addVariable(MAX_PREPARED_STMT_COUNT, new SysVariable(SysVariable.SCOPE_GLOBAL, MAX_PREPARED_STMT_COUNT, "10"));
    addVariable("have_geometry", new SysVariable(SysVariable.SCOPE_NONE, "have_geometry", "YES"));
    addVariable("optimizer_trace_max_mem_size", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_trace_max_mem_size", "16384"));
    addVariable("net_retry_count", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "net_retry_count", "10"));
    addVariable("ndb_table_no_logging", new SysVariable(SysVariable.SCOPE_SESSION, "ndb_table_no_logging", ""));
    addVariable("optimizer_trace_features", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_trace_features",
            "greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on"));
    addVariable("innodb_flush_log_at_trx_commit", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_flush_log_at_trx_commit", "1"));
    addVariable("rewriter_enabled", new SysVariable(SysVariable.SCOPE_GLOBAL, "rewriter_enabled", ""));
    addVariable("query_cache_min_res_unit", new SysVariable(SysVariable.SCOPE_GLOBAL, "query_cache_min_res_unit", "4096"));
    addVariable("updatable_views_with_limit", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "updatable_views_with_limit", "YES"));
    addVariable("optimizer_prune_level", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_prune_level", "1"));
    addVariable("slave_sql_verify_checksum", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_sql_verify_checksum", "ON"));
    addVariable("completion_type", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "completion_type", "NO_CHAIN"));
    addVariable("binlog_checksum", new SysVariable(SysVariable.SCOPE_GLOBAL, "binlog_checksum", "CRC32"));
    addVariable("report_port", new SysVariable(SysVariable.SCOPE_NONE, "report_port", "3306"));
    addVariable("show_old_temporals", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "show_old_temporals", "OFF"));
    addVariable("query_cache_limit", new SysVariable(SysVariable.SCOPE_GLOBAL, "query_cache_limit", "1048576"));
    addVariable("innodb_buffer_pool_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_size", "134217728"));
    addVariable("innodb_adaptive_flushing", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_adaptive_flushing", "ON"));
    addVariable("datadir", new SysVariable(SysVariable.SCOPE_NONE, "datadir", "/usr/local/mysql/data/"));
    addVariable(WAIT_TIMEOUT, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, WAIT_TIMEOUT, "10"));
    addVariable("innodb_monitor_enable", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_monitor_enable", ""));
    addVariable("date_format", new SysVariable(SysVariable.SCOPE_NONE, "date_format", "%Y-%m-%d"));
    addVariable("innodb_buffer_pool_filename", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_filename", "ib_buffer_pool"));
    addVariable("slow_launch_time", new SysVariable(SysVariable.SCOPE_GLOBAL, "slow_launch_time", "2"));
    addVariable("slave_max_allowed_packet", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_max_allowed_packet", "1073741824"));
    addVariable("ndb_use_transactions", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_use_transactions", ""));
    addVariable("innodb_purge_threads", new SysVariable(SysVariable.SCOPE_NONE, "innodb_purge_threads", "1"));
    addVariable("innodb_concurrency_tickets", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_concurrency_tickets", "5000"));
    addVariable("innodb_monitor_reset_all", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_monitor_reset_all", ""));
    addVariable("performance_schema_users_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_users_size", "100"));
    addVariable("ndb_log_updated_only", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_log_updated_only", ""));
    addVariable("basedir", new SysVariable(SysVariable.SCOPE_NONE, "basedir", "/usr/local/mysql"));
    addVariable("innodb_old_blocks_time", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_old_blocks_time", "1000"));
    addVariable("innodb_stats_method", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_stats_method", "nulls_equal"));
    addVariable(INNODB_LOCK_WAIT_TIMEOUT, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, INNODB_LOCK_WAIT_TIMEOUT, "50"));
    addVariable("local_infile", new SysVariable(SysVariable.SCOPE_GLOBAL, "local_infile", "ON"));
    addVariable("myisam_stats_method", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "myisam_stats_method", "nulls_unequal"));
    addVariable("version_compile_os", new SysVariable(SysVariable.SCOPE_NONE, "version_compile_os", "osx10.8"));
    addVariable("relay_log_recovery", new SysVariable(SysVariable.SCOPE_NONE, "relay_log_recovery", "OFF"));
    addVariable("old", new SysVariable(SysVariable.SCOPE_NONE, "old", "OFF"));
    addVariable("innodb_table_locks", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "innodb_table_locks", "ON"));
    addVariable("performance_schema", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema", "OFF"));
    addVariable("myisam_recover_options", new SysVariable(SysVariable.SCOPE_NONE, "myisam_recover_options", "OFF"));
    addVariable("net_buffer_length", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "net_buffer_length", "16384"));
    addVariable("rpl_semi_sync_master_wait_for_slave_count", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_wait_for_slave_count", ""));
    addVariable("binlog_row_image", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "binlog_row_image", "FULL"));
    addVariable("innodb_locks_unsafe_for_binlog", new SysVariable(SysVariable.SCOPE_NONE, "innodb_locks_unsafe_for_binlog", "OFF"));
    addVariable("rbr_exec_mode", new SysVariable(SysVariable.SCOPE_SESSION, "rbr_exec_mode", ""));
    addVariable("myisam_max_sort_file_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "myisam_max_sort_file_size", "9223372036853727232"));
    addVariable("back_log", new SysVariable(SysVariable.SCOPE_NONE, "back_log", "80"));
    addVariable("lower_case_file_system", new SysVariable(SysVariable.SCOPE_NONE, "lower_case_file_system", "ON"));
    addVariable("rpl_semi_sync_master_wait_no_slave", new SysVariable(SysVariable.SCOPE_GLOBAL, "rpl_semi_sync_master_wait_no_slave", ""));
    addVariable(GROUP_CONCAT_MAX_LEN, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, GROUP_CONCAT_MAX_LEN, "1024"));
    addVariable("pseudo_thread_id", new SysVariable(SysVariable.SCOPE_SESSION, "pseudo_thread_id", ""));
    addVariable("socket", new SysVariable(SysVariable.SCOPE_NONE, "socket", "/tmp/myssock"));
    addVariable("have_dynamic_loading", new SysVariable(SysVariable.SCOPE_NONE, "have_dynamic_loading", "YES"));
    addVariable("rewriter_verbose", new SysVariable(SysVariable.SCOPE_GLOBAL, "rewriter_verbose", ""));
    addVariable("innodb_undo_logs", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_undo_logs", "128"));
    addVariable("performance_schema_max_cond_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_cond_instances", "3504"));
    addVariable("delayed_insert_limit", new SysVariable(SysVariable.SCOPE_GLOBAL, "delayed_insert_limit", "100"));
    addVariable("flush", new SysVariable(SysVariable.SCOPE_GLOBAL, "flush", "OFF"));
    addVariable("eq_range_index_dive_limit", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "eq_range_index_dive_limit", "10"));
    addVariable("performance_schema_events_stages_history_size", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_events_stages_history_size", "10"));
    addVariable("character_set_connection", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "character_set_connection", ""));
    addVariable("myisam_use_mmap", new SysVariable(SysVariable.SCOPE_GLOBAL, "myisam_use_mmap", "OFF"));
    addVariable("ndb_join_pushdown", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndb_join_pushdown", ""));
    addVariable("character_set_server", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "character_set_server", ""));
    addVariable("validate_password_special_char_count", new SysVariable(SysVariable.SCOPE_GLOBAL, "validate_password_special_char_count", "1"));
    addVariable("performance_schema_max_thread_instances", new SysVariable(SysVariable.SCOPE_NONE, "performance_schema_max_thread_instances", "402"));
    addVariable("slave_rows_search_algorithms", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_rows_search_algorithms", "TABLE_SCAN,INDEX_SCAN"));
    addVariable("ndbinfo_show_hidden", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "ndbinfo_show_hidden", ""));
    addVariable("net_read_timeout", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "net_read_timeout", "30"));
    addVariable("innodb_page_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_page_size", "16384"));
    addVariable(MAX_ALLOWED_PACKET, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, MAX_ALLOWED_PACKET, "67108864"));
    addVariable("innodb_log_file_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_log_file_size", "50331648"));
    addVariable("sync_relay_log_info", new SysVariable(SysVariable.SCOPE_GLOBAL, "sync_relay_log_info", "10000"));
    addVariable("optimizer_trace_limit", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "optimizer_trace_limit", "1"));
    addVariable("innodb_ft_max_token_size", new SysVariable(SysVariable.SCOPE_NONE, "innodb_ft_max_token_size", "84"));
    addVariable("validate_password_length", new SysVariable(SysVariable.SCOPE_GLOBAL, "validate_password_length", "8"));
    addVariable("ndb_log_binlog_index", new SysVariable(SysVariable.SCOPE_GLOBAL, "ndb_log_binlog_index", ""));
    addVariable("innodb_api_bk_commit_interval", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_api_bk_commit_interval", "5"));
    addVariable("innodb_undo_directory", new SysVariable(SysVariable.SCOPE_NONE, "innodb_undo_directory", "."));
    addVariable("bind_address", new SysVariable(SysVariable.SCOPE_NONE, "bind_address", "*"));
    addVariable("innodb_sync_spin_loops", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_sync_spin_loops", "30"));
    addVariable(SQL_SAFE_UPDATES, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, SQL_SAFE_UPDATES, "0"));
    addVariable("tmpdir", new SysVariable(SysVariable.SCOPE_NONE, "tmpdir", "/var/tmp/"));
    addVariable("innodb_thread_concurrency", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_thread_concurrency", "0"));
    addVariable("slave_allow_batching", new SysVariable(SysVariable.SCOPE_GLOBAL, "slave_allow_batching", "OFF"));
    addVariable("innodb_buffer_pool_dump_pct", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_buffer_pool_dump_pct", ""));
    addVariable("lc_time_names", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "lc_time_names", "en_US"));
    addVariable("max_statement_time", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_statement_time", ""));
    addVariable(END_MARKERS_IN_JSON, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, END_MARKERS_IN_JSON, "0"));
    addVariable(AVOID_TEMPORAL_UPGRADE, new SysVariable(SysVariable.SCOPE_GLOBAL, AVOID_TEMPORAL_UPGRADE, "0"));
    addVariable("key_cache_age_threshold", new SysVariable(SysVariable.SCOPE_GLOBAL, "key_cache_age_threshold", "300"));
    addVariable("innodb_status_output", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_status_output", "OFF"));
    addVariable("identity", new SysVariable(SysVariable.SCOPE_SESSION, "identity", ""));
    addVariable("min_examined_row_limit", new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "min_examined_row_limit", "0"));
    addVariable("sync_frm", new SysVariable(SysVariable.SCOPE_GLOBAL, "sync_frm", "ON"));
    addVariable("innodb_online_alter_log_max_size", new SysVariable(SysVariable.SCOPE_GLOBAL, "innodb_online_alter_log_max_size", "134217728"));
    addVariable(WARNING_COUNT, new SysVariable(SysVariable.SCOPE_SESSION, WARNING_COUNT, "0"));
    addVariable(ERROR_COUNT, new SysVariable(SysVariable.SCOPE_SESSION, ERROR_COUNT, "0"));
    addVariable("MYSQL_SERVER_ENCODING", new SysVariable(SysVariable.SCOPE_NONE, "MYSQL_SERVER_ENCODING", "UTF-8"));
    addVariable(MAX_EXECUTION_TIME, new SysVariable(SysVariable.SCOPE_GLOBAL | SysVariable.SCOPE_SESSION, "max_execution_time", "0"));
  }

  public static Map<String, SysVariable> getAllVars() {
    return SYS_VARS;
  }
}
