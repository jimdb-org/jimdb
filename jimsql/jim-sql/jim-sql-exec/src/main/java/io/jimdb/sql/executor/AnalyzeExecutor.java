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

package io.jimdb.sql.executor;

import io.jimdb.core.Session;
import io.jimdb.common.exception.BaseException;
import io.jimdb.core.model.result.impl.QueryExecResult;

import reactor.core.publisher.Flux;

/**
 * The analyze executor classifies all the push-down stats analysis into two categories:
 * analyzeIndexExecutor and analyzeColumnsExecutor.
 * It dispatches all these executions and then gather the results back.
 * Finally it updates the stats map and also returns the consolidated execution results to the client
 */
public abstract class AnalyzeExecutor implements Executor {

  public abstract Flux<QueryExecResult> execute(Session session) throws BaseException;
}
