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
package io.jimdb.core.plugin;

import io.jimdb.pb.Mspb;

/**
 * RouterStore
 * <p>
 * This client handles master interfaces
 *
 * @since 2019/11/22
 */
public interface RouterStore extends Plugin {

  void createRange(Mspb.CreateRangesRequest request);

  void deleteRange(Mspb.DeleteRangesRequest request);

  Mspb.GetRouteResponse getRoute(long dbId, long tableId, byte[] key, int retry);

  Mspb.GetNodeResponse getNode(long id, int retry);

  int generateId();
}
