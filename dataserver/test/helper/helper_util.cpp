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

#include "helper_util.h"

#include "common/ds_encoding.h"
#include "db/mass_tree_impl/manager_impl.h"
#include "db/rocksdb_impl/manager_impl.h"

namespace jim {
namespace test {
namespace helper {

using namespace jim::ds;
using namespace jim::ds::db;

uint64_t GetPeerID(uint64_t node_id) {
    return node_id + 100;
}

basepb::Range MakeRangeMeta(Table *t, size_t peers_num) {
    const uint32_t RID = 1;
    return MakeRangeMeta(t, peers_num, RID);
}

basepb::Range MakeRangeMeta(Table *t, size_t peers_num, uint32_t rid) {
    basepb::Range meta;
    meta.set_id(rid);
    EncodeKeyPrefix(meta.mutable_start_key(), t->GetID());
    EncodeKeyPrefix(meta.mutable_end_key(), t->GetID() + 1);

    for (size_t i = 0; i < peers_num; ++i) {
        auto peer = meta.add_peers();
        peer->set_node_id(i + 1);
        peer->set_id(GetPeerID(peer->node_id()));
        peer->set_type(basepb::PeerType_Normal);
    }
    meta.mutable_range_epoch()->set_version(peers_num);
    meta.mutable_range_epoch()->set_conf_ver(peers_num);

    meta.set_table_id(t->GetID());
    auto pks = t->GetPKs();
    if (pks.size() == 0) {
        throw std::runtime_error("invalid table(no primary key)");
    }

    auto key_schema = meta.mutable_key_schema();

    for (const auto& pk : pks) {
        auto p = key_schema->add_key_cols();

        p->set_id(pk.id());
        p->set_type(pk.sql_type().type());
        p->set_unsigned_(pk.sql_type().unsigned_());
    }

    return meta;
}

void EncodeKeyPrefix(std::string *buf, uint32_t id) {
    EncodeUint32Ascending(buf, id);
}

void EncodeKvKey(std::string* kv_key, const basepb::ColumnInfo& col, const std::string& val) {
    switch (col.sql_type().type()) {
        case basepb::TinyInt:
        case basepb::SmallInt:
        case basepb::MediumInt:
        case basepb::Int:
        case basepb::BigInt:
        case basepb::Year: {
            if (!col.sql_type().unsigned_()) {
                int64_t i = strtoll(val.c_str(), NULL, 10);
                EncodeVarintAscending(kv_key, i);
            } else {
                uint64_t i = strtoull(val.c_str(), NULL, 10);
                EncodeUvarintAscending(kv_key, i);
            }
            break;
        }

        case basepb::Float:
        case basepb::Double: {
            double d = strtod(val.c_str(), NULL);
            EncodeFloatAscending(kv_key, d);
            break;
        }

        case basepb::Varchar:
        case basepb::Binary:
        case basepb::Char:
        case basepb::NChar:
        case basepb::VarBinary:
        case basepb::TinyBlob:
        case basepb::Blob:
        case basepb::MediumBlob:
        case basepb::LongBlob:
        case basepb::TinyText:
        case basepb::Text:
        case basepb::MediumText:
        case basepb::LongText:
        case basepb::Json: {
            EncodeBytesAscending(kv_key, val.c_str(), val.size());
            break;
        }

        case basepb::Decimal: {
            datatype::MyDecimal dec;
            int32_t ret;

            ret = dec.FromString(val);
            if ( ret != datatype::E_DEC_OK) {
                dec.ResetZero();
            }

            EncodeDecimalAscending(kv_key, &dec);
            break;
        }

        case basepb::Date:
        case basepb::DateTime:
        case basepb::TimeStamp: {
            datatype::MyDateTime dt;
            datatype::MyDateTime::StWarn st;

            auto b = dt.FromString(val, st);
            if (!b) {
                dt.SetZero();
            }

            EncodeDateAscending(kv_key, &dt);
            break;
        }

        case basepb::Time: {
            datatype::MyTime t;
            datatype::MyTime::StWarn st;

            auto b = t.FromString(val, st);
            if (!b) {
                t.SetZero();
            }

            EncodeTimeAscending(kv_key, &t);
            break;
        }

        default:
            throw std::runtime_error(std::string("EncodeKvKey: invalid column data type: ") +
                std::to_string(static_cast<int>(col.sql_type().type())));
    }
}

void EncodeKvValue(std::string* kv_value, const basepb::ColumnInfo& col, const std::string& val) {
    switch (col.sql_type().type()) {
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year: {
        if (!col.sql_type().unsigned_()) {
            int64_t i = strtoll(val.c_str(), NULL, 10);
            EncodeIntValue(kv_value, static_cast<uint32_t>(col.id()), i);
        } else {
            uint64_t i = strtoull(val.c_str(), NULL, 10);
            EncodeIntValue(kv_value, static_cast<uint32_t>(col.id()), static_cast<int64_t>(i));
        }
        break;
    }

    case basepb::Float:
    case basepb::Double: {
        double d = strtod(val.c_str(), NULL);
        EncodeFloatValue(kv_value, static_cast<uint32_t>(col.id()), d);
        break;
    }
    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Char:
    case basepb::NChar:
    case basepb::VarBinary:
    case basepb::TinyBlob:
    case basepb::Blob:
    case basepb::MediumBlob:
    case basepb::LongBlob:
    case basepb::TinyText:
    case basepb::Text:
    case basepb::MediumText:
    case basepb::LongText:
    case basepb::Json: {
        EncodeBytesValue(kv_value, static_cast<uint32_t>(col.id()), val.c_str(), val.size());
        break;
    }

    case basepb::Decimal: {

        datatype::MyDecimal dec;
        int32_t ret;

        ret = dec.FromString(val);
        if ( ret != datatype::E_DEC_OK) {
            dec.ResetZero();
        }

        EncodeDecimalValue(kv_value, static_cast<uint32_t>(col.id()), &dec);
        break;
    }

    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp: {

        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        auto b = dt.FromString(val, st);
        if (!b) {
            dt.SetZero();
        }

        EncodeDateValue(kv_value, static_cast<uint32_t>(col.id()), &dt);
        break;
    }

    case basepb::Time: {
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        auto b = t.FromString(val, st);
        if (!b) {
            t.SetZero();
        }

        EncodeTimeValue(kv_value, static_cast<uint32_t>(col.id()), &t);
        break;
    }

    default:
        throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                 std::to_string(static_cast<int>(col.sql_type().type())));
    }
}

void DecodeValueColumn(const std::string& buf, size_t& offset, const dspb::ColumnInfo& col, std::string *val) {
    switch (col.typ()) {
    case basepb::TinyInt:
    case basepb::SmallInt:
    case basepb::MediumInt:
    case basepb::Int:
    case basepb::BigInt:
    case basepb::Year: {
        int64_t i = 0;
        DecodeIntValue(buf, offset, &i);
        if (col.unsigned_()) {
            *val = std::to_string(static_cast<uint64_t>(i));
        } else {
            *val = std::to_string(i);
        }
        break;
    }

    case basepb::Float:
    case basepb::Double: {
        double d = 0.0;
        DecodeFloatValue(buf, offset, &d);
        *val = std::to_string(d);
        break;
    }

    case basepb::Varchar:
    case basepb::Binary:
    case basepb::Char:
    case basepb::NChar:
    case basepb::VarBinary:
    case basepb::TinyBlob:
    case basepb::Blob:
    case basepb::MediumBlob:
    case basepb::LongBlob:
    case basepb::TinyText:
    case basepb::Text:
    case basepb::MediumText:
    case basepb::LongText:
    case basepb::Json: {
        DecodeBytesValue(buf, offset, val);
        break;
    }

    case basepb::Decimal: {

        datatype::MyDecimal dec;

        DecodeDecimalValue(buf, offset, &dec);

        *val = dec.ToString();

        break;
    }
    case basepb::Date:
    case basepb::DateTime:
    case basepb::TimeStamp: {
        datatype::MyDateTime dt;
        DecodeDateValue(buf, offset, &dt);

        *val = dt.ToString();
        break;
    }

    case basepb::Time: {
        datatype::MyTime t;
        DecodeTimeValue(buf, offset, &t);

        *val = t.ToString();
        break;
    }
    default:
        throw std::runtime_error(std::string("EncodeColumnValue: invalid column data type: ") +
                                 std::to_string(static_cast<int>(col.typ())));
    }
}

void InitLog() {
    auto str = ::getenv("LOG_LEVEL");
    std::string log_level = "info";
    if (str != nullptr) {
        log_level = str;
    }
    LoggerSetLevel(log_level);
}

Status NewDBManager(const std::string& path, std::unique_ptr<ds::db::DBManager>& db_manager) {
    auto engine = ::getenv("ENGINE");

    if (engine != nullptr && strcmp(engine, "rocksdb") == 0) {
        std::cout << "Engine: rocksdb ...." << std::endl;
        RocksDBConfig opt;
        opt.path = path;
        db_manager.reset(new RocksDBManager(opt));

    } else {
        std::cout << "Engine: masstree...." << std::endl;

        MasstreeOptions opt;
        opt.rcu_interval_ms = 2000;
        opt.data_path = path;
        opt.rcu_interval_ms = 1000;
        opt.checkpoint_opt.disabled = false;
        opt.checkpoint_opt.checksum = true;
        opt.checkpoint_opt.work_threads = 1;
        opt.checkpoint_opt.threshold_bytes = 4096;
        opt.checkpoint_opt.max_history = 2;
        db_manager.reset(new MasstreeDBManager(opt));
    }

    return db_manager->Init();
}

} /* namespace helper */
} /* namespace test */
} /* namespace jim */

