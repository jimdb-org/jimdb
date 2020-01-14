// Copyright 2019 The JIMDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

#include "db_manager.h"

#include <sstream>

#include "mass_tree_impl/manager_impl.h"
#include "rocksdb_impl/manager_impl.h"

namespace jim {
namespace ds {
namespace db {

std::string DBUsage::ToString() const {
    std::ostringstream ss;
    ss << "{ total: " << total_size << ", used: " << used_size;
    ss << ", free: " << free_size << " }";
    return ss.str();
}

Status NewDBManager(std::unique_ptr<DBManager>& db_manager) {
    switch (ds_config.engine_type) {
        case EngineType::kMassTree:
            db_manager.reset(new MasstreeDBManager(ds_config.masstree_config));
            break;
        case EngineType::kRocksdb:
            db_manager.reset(new RocksDBManager(ds_config.rocksdb_config));
            break;
        default:
            return Status(Status::kNotSupported, "unknown engine name", "");
    }
    return db_manager->Init();
}

} // namespace db
} // namespace ds
} // namespace jim
