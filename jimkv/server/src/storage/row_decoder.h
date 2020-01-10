// Copyright 2019 The JimDB Authors.
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

_Pragma("once");

#include <functional>
#include <map>
#include <unordered_set>
#include <string>

#include "base/status.h"
#include "dspb/txn.pb.h"

#include "field_value.h"

namespace jimkv {
namespace ds {
namespace storage {

struct RowColumnInfo {
    u_int64_t col_id;
    bool asc;
};

class RowResult {
public:
    RowResult();
    ~RowResult();

    RowResult(const RowResult &);
    RowResult &operator=(const RowResult &);

    bool operator==(const RowResult &other) const {
        bool bEq = true;
        if (col_order_by_infos_ != nullptr && other.col_order_by_infos_ != nullptr 
            && col_order_by_infos_->size() == other.col_order_by_infos_->size()) {
            for (auto &colInfo : *(col_order_by_infos_)) {
                auto field1 = GetField(colInfo.col_id);
                auto field2 = other.GetField(colInfo.col_id);
                if (field1 != nullptr && field2 != nullptr) {
                    if (!fcompare(*(field1), *(field2), CompareOp::kEqual)) {
                        bEq = false;
                        break;
                    }
                } else if (field1 != field2){
                    bEq = false;
                    break;
                }
            }
        }

        return bEq;
    }

    friend bool operator<(const RowResult &r1, const RowResult &r2) {
        bool bCmp = false;
        if (r1.col_order_by_infos_ != nullptr && r2.col_order_by_infos_ != nullptr 
            && r1.col_order_by_infos_->size() == r2.col_order_by_infos_->size()) {
            for (auto &colInfo : *(r1.col_order_by_infos_)) {
                auto field1 = r1.GetField(colInfo.col_id);
                auto field2 = r2.GetField(colInfo.col_id);
                if (!field1 || !field2) {
                    continue;
                }
                if (fcompare(*(field1), *(field2), CompareOp::kEqual)) {
                    continue;
                }

                bCmp = fcompare(*(field1), *(field2), CompareOp::kLess);
                if (!colInfo.asc) {
                    bCmp = !bCmp;
                }
                break;
            }
        }

        return bCmp;
    }

    const std::string &GetKey() const { return key_; }
    std::string& MutableKey() {return key_; }
    void SetKey(const std::string &key) { key_ = key; }
    void SetKey(std::string&& key) { key_ = std::move(key); }

    const std::string &GetValue() const { return value_; }
    std::string& MutableValue() {return value_; }
    void SetValue(const std::string &value) { value_ = value; }
    void SetValue(std::string&& value) { value_ = std::move(value); }

    uint64_t GetVersion() const { return version_; }
    void SetVersion(uint64_t ver) { version_ = ver; }

    bool AddField(uint64_t col, std::unique_ptr<FieldValue> &field);
    FieldValue *GetField(uint64_t col) const;

    const std::string& GetAggFields() { return fields_agg_; }
    std::string&  MutableAggFields() { return fields_agg_; }
    void SetAggFields(const std::string &fields) { fields_agg_ = fields; }
    void SetAggFields(std::string&& fields) { fields_agg_ = std::move(fields); }

    bool GetPksFlag() const { return pks_flag_; }
    void SetPksFlag(bool flag) { pks_flag_ = flag; }

    const std::string & GetPksInfo() const { return pks_info_; }
    void SetPksInfo(const std::string& str) { pks_info_ = str; }

    void SetColumnInfos(std::vector<RowColumnInfo> &infos) {
        col_order_by_infos_ = &infos;
    }

    std::string hash() const {
        if (col_order_by_infos_ != nullptr) {
            static const std::string tag = "\x01";
            std::string str(tag);
            for (const auto & col : *col_order_by_infos_)  {
                auto field = GetField(col.col_id);
                if (field != nullptr) {
                    str += field->ToString();
                }
                str += tag;
            }
            return std::move(str);
        }
        return std::move(std::string(""));
    }

    void Reset();

    void EncodeTo(const dspb::SelectRequest &req, dspb::RowValue *to);
    void EncodeTo(const std::vector<int> &off_sets, const std::vector<uint64_t> &col_ids, dspb::RowValue *to);
    const std::unordered_map<uint64_t, FieldValue*>& GetMapFields() {return fields_;}

private:
    void CopyFrom(const RowResult &);

private:
    std::string fields_agg_;
    std::string key_;
    std::string value_;
    uint64_t version_ = 0;
    std::unordered_map<uint64_t, FieldValue *> fields_;
    const std::vector<RowColumnInfo> *col_order_by_infos_ = nullptr;
    bool pks_flag_ = false;
    std::string pks_info_;
};

class Decoder {
public:
    Decoder() = default;
    virtual ~Decoder() = default;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result) = 0;
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match) = 0;

    virtual std::string DebugString() const = 0;

    static std::unique_ptr<Decoder> CreateDecoder(const basepb::KeySchema &key_schema,
                                                  const dspb::SelectRequest &req);
    static std::unique_ptr<Decoder> CreateDecoder(const basepb::KeySchema &key_schema,
                                                  const dspb::TableRead &req);
    static std::unique_ptr<Decoder> CreateDecoder(const basepb::KeySchema &key_schema,
                                                  const dspb::IndexRead &req);
    static std::unique_ptr<Decoder> CreateDecoder(const basepb::KeySchema &key_schema,
                                                  const dspb::DataSample &req);

private:
    virtual void addExprColumn(const dspb::Expr &expr) = 0;
    virtual Status decodeKeyColumns(const std::string &key, RowResult &result) = 0;
    virtual Status decodeValueColumns(const std::string &buf, RowResult &result) = 0;
};

class RowDecoder : public Decoder {
public:
    RowDecoder(const basepb::KeySchema &key_schema, const dspb::SelectRequest &req);
    RowDecoder(const basepb::KeySchema &key_schema, const dspb::TableRead &req);
    RowDecoder(const basepb::KeySchema &key_schema, const dspb::DataSample &req);

    ~RowDecoder();

    RowDecoder(const RowDecoder &) = delete;
    RowDecoder &operator=(const RowDecoder &) = delete;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result);
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match);

    virtual std::string DebugString() const;

private:
    virtual void addExprColumn(const dspb::Expr &expr);
    virtual Status decodeKeyColumns(const std::string &key, RowResult &result);
    virtual Status decodeValueColumns(const std::string &buf, RowResult &result);

private:
    const basepb::KeySchema &key_schema_;
    std::unordered_map<uint64_t, dspb::ColumnInfo> cols_;
    std::unique_ptr<dspb::Expr> where_expr_;
    std::map<uint64_t, dspb::ColumnInfo> read_cols_;
};

class IndexDecoder : public Decoder {
public:

    IndexDecoder(const basepb::KeySchema &key_schema, const dspb::IndexRead &req);
    ~IndexDecoder();

    IndexDecoder(const IndexDecoder &) = delete;
    IndexDecoder &operator=(const IndexDecoder &) = delete;

    virtual Status Decode(const std::string &key, const std::string &buf, RowResult &result);
    virtual Status DecodeAndFilter(const std::string &key, const std::string &buf, RowResult &result, bool &match);

    virtual std::string DebugString() const;

private:
    virtual void addExprColumn(const dspb::Expr &expr);
    virtual Status decodeKeyColumns(const std::string &key, RowResult &result);
    virtual Status decodeValueColumns(const std::string &buf, RowResult &result);

private:
    const basepb::KeySchema &key_schema_;
    std::unordered_map<uint64_t, dspb::ColumnInfo> cols_;
    std::map<uint64_t, dspb::ColumnInfo> read_cols_;
    bool pks_flag_;
};

} /* namespace storage */
} /* namespace ds */
} /* namespace jimkv */

namespace std {
template <>
struct hash<jimkv::ds::storage::RowResult> {
    size_t operator()(const jimkv::ds::storage::RowResult &res) const noexcept {
        return std::hash<decltype(res.hash())>()(res.hash());
    }
};
} // namespace std
