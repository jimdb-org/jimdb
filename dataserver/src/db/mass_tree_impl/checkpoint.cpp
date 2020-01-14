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

#include "checkpoint.h"

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <sys/types.h>
#include <libgen.h>
#include <sstream>
#include <iomanip>
#include <algorithm>

#include "base/byte_order.h"
#include "base/fs_util.h"
#include "base/crc64.h"
#include "common/logger.h"

#include "write_batch_impl.h"

namespace jim {
namespace ds {
namespace db {

static const char* kCheckpointFileSuffix = ".ckp";
static const char* kCheckpointTmpSuffix = ".tmp";

static std::string makeCheckpointFile(const std::string& path, uint64_t applied) {
    std::stringstream ss;
    ss << std::hex << std::setfill('0') << std::setw(16) << applied;
    ss << kCheckpointFileSuffix;
    return JoinFilePath({path, ss.str()});
}

static bool isCheckpointFile(const std::string& filename) {
    std::size_t pos = filename.find(kCheckpointFileSuffix);
    if (pos == std::string::npos) {
        return false;
    }
    return filename.substr(pos) == kCheckpointFileSuffix;
}

static bool isTempFile(const std::string& filename) {
    std::string suffix = std::string(kCheckpointFileSuffix) + kCheckpointTmpSuffix;
    std::size_t pos = filename.find(suffix);
    if (pos == std::string::npos) {
        return false;
    }
    return filename.substr(pos) == suffix;
}

struct RecordHeader {
    uint8_t cf_type = 0;        // 0 means CFType::kDdata; 1 means CFType::kTxn
    uint32_t key_size = 0;
    uint32_t value_size = 0;

    void Encode();
    void Decode();

    Status Validate() const;
    std::string ToString() const;

}__attribute__((packed));

void RecordHeader::Encode() {
    key_size = htobe32(key_size);
    value_size = htobe32(value_size);
}

void RecordHeader::Decode() {
    key_size = be32toh(key_size);
    value_size = be32toh(value_size);
}

Status RecordHeader::RecordHeader::Validate() const {
    if (key_size > kMaxDBKeySize || value_size > kMaxDBValueSize) {
        return Status(Status::kInvalidArgument, "size too large", ToString());
    }
    return Status::OK();
}

std::string RecordHeader::ToString() const {
    std::ostringstream ss;
    ss << "{ cf: " << static_cast<int>(cf_type) << ", ksize: " << key_size;
    ss << ", vsize: " << value_size << " }";
    return ss.str();
}


void CheckpointHeader::Encode() {
    magic = htobe32(magic);
    version = htobe16(version);
    timestamp = htobe32(timestamp);
    applied_index = htobe64(applied_index);
}

bool CheckpointHeader::Decode() {
    magic = be32toh(magic);
    version = be16toh(version);
    timestamp = be32toh(timestamp);
    applied_index = be64toh(applied_index);
    return magic == kCheckpointMagic;
}

std::string CheckpointHeader::ToString() const {
    std::ostringstream ss;
    ss << "{ magic: "  << magic << ", version: " << version;
    ss << ", timestamp: " << timestamp << ", applied: " << applied_index << "}";
    return ss.str();
}

CheckpointWriter::CheckpointWriter(const std::string& file_path, uint64_t applied, bool checksum) :
    file_path_(file_path),
    tmp_file_path_(file_path_ +  kCheckpointTmpSuffix),
    checksum_(checksum),
    timestamp_(::time(NULL)) {
    header_.timestamp = timestamp_;
    header_.applied_index = applied;
}

CheckpointWriter::~CheckpointWriter() {
    if (fp_ != nullptr) {
        ::fclose(fp_);
        fp_ = nullptr;
    }
}

Status CheckpointWriter::Open() {
    fp_ = ::fopen(tmp_file_path_.c_str(), "w");
    if (fp_ == NULL) {
        return Status(Status::kIOError,
                std::string("create checkpoint file: ") + tmp_file_path_, strErrno(errno));
    }

    assert(sizeof(CheckpointHeader) == CheckpointHeader::kSize);
    // encode and write file header
    header_.Encode();
    auto ret = ::fwrite(&header_, sizeof(header_), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "write checkpoint header", strErrno(errno));
    }
    if (checksum_) {
        crc_ = crc64(crc_, (const unsigned char *)&header_, sizeof(header_));
    }

    return Status::OK();
}


void CheckpointWriter::encRecord(CFType cf, const std::string& key, const std::string& value, std::string& buf) {
    buf.reserve(sizeof(RecordHeader) + key.size() + value.size());

    RecordHeader rec_hdr;
    rec_hdr.cf_type = (cf == CFType::kTxn? 1 : 0);
    rec_hdr.key_size = key.size();
    rec_hdr.value_size = value.size();
    rec_hdr.Encode();
    buf.append((const char *)&rec_hdr, sizeof(rec_hdr));

    if (!key.empty()) {
        buf.append(key);
    }
    if (!value.empty()) {
        buf.append(value);
    }

    assert(buf.size() == sizeof(RecordHeader) + key.size() + value.size());
}

Status CheckpointWriter::Append(CFType cf, const std::string& key, const std::string& value) {
    std::string rec_buf;
    encRecord(cf, key, value, rec_buf);
    auto ret = ::fwrite(rec_buf.c_str(), rec_buf.size(), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "write checkpoint record", strErrno(errno));
    }

    if (checksum_) {
        crc_ = crc64(crc_, (const unsigned char *)(rec_buf.c_str()), rec_buf.size());
    }
    return Status::OK();
}

Status CheckpointWriter::writeCRC() {
    auto crc = htobe64(crc_);
    int ret = ::fwrite(&crc, sizeof(crc), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "write checkpoint crc", strErrno(errno));
    }
    return Status::OK();
}

Status CheckpointWriter::Finish() {
    // append a empty record as record finish flag
    auto s = Append(CFType::kData, "", "");
    if (!s.ok()) {
        return s;
    }

    // write crc
    s = writeCRC();
    if (!s.ok()) {
        return s;
    }

    // flush and sync, close
    int ret = ::fflush(fp_);
    if (ret < 0) {
        return Status(Status::kIOError, "flush checkpoint file", strErrno(errno));
    }
    ret = ::fsync(::fileno(fp_));
    if (ret < 0) {
        return Status(Status::kIOError, "sync checkpoint file", strErrno(errno));
    }
    ret = ::fclose(fp_);
    if (ret < 0) {
        return Status(Status::kIOError, "close checkpoint file", strErrno(errno));
    }
    fp_ = nullptr;

    // rename to checkpoint file
    ret = std::rename(tmp_file_path_.c_str(), file_path_.c_str());
    if (ret < 0) {
        return Status(Status::kIOError, "rename checkpoint file", strErrno(errno));
    }

    return Status::OK();
}

Status CheckpointWriter::Abort() {
    // close and remove file
    if (fp_ != nullptr) {
        ::fclose(fp_);
        fp_ = nullptr;
    }
    auto ret = std::remove(tmp_file_path_.c_str());
    if (ret != 0) {
        return Status(Status::kIOError, "remove " + tmp_file_path_, strErrno(errno));
    }
    return Status::OK();
}

CheckpointReader::CheckpointReader(const std::string& path, bool verify_checksum):
    path_(path),
    verify_checksum_(verify_checksum) {
}

CheckpointReader::~CheckpointReader() {
    if (fp_ != nullptr) {
        ::fclose(fp_);
    }
}

Status CheckpointReader::Open() {
    if (opened_) {
        return Status(Status::kDuplicate, "checkepoint already opened", "");
    }

    fp_ = ::fopen(path_.c_str(), "r");
    if (fp_ == nullptr) {
        return Status(Status::kIOError, "open checkpoint", strErrno(errno));
    }

    // reader and decode header
    auto ret = ::fread(&header_, sizeof(header_), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "read checkpoint header", strErrno(errno));
    }
    if (verify_checksum_) {
        crc_ = crc64(crc_, (const unsigned char *)&header_, sizeof(header_));
    }
    if (!header_.Decode()) {
        return Status(Status::kCorruption, "checkpoint header", header_.ToString());
    }

    opened_ = true;

    return Status::OK();
}

const CheckpointHeader& CheckpointReader::Header() const {
    assert(opened_);
    return header_;
}

uint64_t CheckpointReader::AppliedIndex() const {
    assert(opened_);
    return header_.applied_index;
}

Status CheckpointReader::readRecord(CFType &cf, std::string& key, std::string& value, bool &over) {
    // read record header
    RecordHeader rec_hdr;
    auto ret = ::fread(&rec_hdr, sizeof(rec_hdr), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "read checkpoint record header", strErrno(errno));
    }
    if (verify_checksum_)  {
        crc_ = crc64(crc_, (const unsigned char *)&rec_hdr, sizeof(rec_hdr));
    }

    // decode and check record header
    rec_hdr.Decode();
    auto s = rec_hdr.Validate();
    if (!s.ok()) {
        return s; // TODO: return file offset
    }

    // if keysize == 0 means last record, over and return
    over = (rec_hdr.key_size == 0);
    if (over) {
        return Status::OK();
    }

    // set cf
    cf = rec_hdr.cf_type == 1 ? CFType::kTxn : CFType::kData;

    // read key
    assert(rec_hdr.key_size > 0);
    key.resize(rec_hdr.key_size);
    ret = ::fread((char *)key.data(), rec_hdr.key_size, 1, fp_);
    if (ret != 1) {
        // TODO: return file offset
        return Status(Status::kIOError, "read checkpoint record key", strErrno(errno));
    }
    if (verify_checksum_) {
        crc_ = crc64(crc_, (const unsigned char *)key.c_str(), rec_hdr.key_size);
    }

    // read value
    if (rec_hdr.value_size > 0) {
        value.resize(rec_hdr.value_size);
        ret = ::fread((char *)value.data(), rec_hdr.value_size, 1, fp_);
        if (ret != 1) {
            // TODO: return file offset
            return Status(Status::kIOError, "read checkpoint record value", strErrno(errno));
        }
        if (verify_checksum_) {
            crc_ = crc64(crc_, (const unsigned char *)value.c_str(), rec_hdr.value_size);
        }
    }
    return Status::OK();
}

Status CheckpointReader::Next(CFType &cf, std::string& key, std::string& value, bool& over) {
    assert(opened_);

    auto s = readRecord(cf, key, value, over);
    if (!s.ok()) {
        return s;
    }

    if (!over) {
        return Status::OK();
    }

    // last record, read and verify checksum
    uint64_t expected_crc = 0;
    auto ret = ::fread(&expected_crc, sizeof(expected_crc), 1, fp_);
    if (ret != 1) {
        return Status(Status::kIOError, "read checkpoint crc", strErrno(errno));
    }
    expected_crc = be64toh(expected_crc);
    if (verify_checksum_ && expected_crc != 0 && crc_ != expected_crc) {
        return Status(Status::kCorruption, "crc mismatched",
                std::to_string(crc_) + " != " + std::to_string(expected_crc));
    }

    return Status::OK();
}


std::string CheckpointJob::JobMetric::ToString() const {
    std::ostringstream ss;
    ss << "{ keys: " << keys_written << ", bytes: " << bytes_written;
    ss << ", used: " << used_msecs << "ms }";
    return ss.str();
}

CheckpointJob::CheckpointJob(CheckpointManager* manager, const std::string& job_name,
        uint64_t applied, IteratorPtr default_iter, IteratorPtr txn_iter,
        const std::string& output_file):
    manager_(manager),
    name_(job_name),
    applied_(applied),
    default_iter_(std::move(default_iter)),
    txn_iter_(std::move(txn_iter)),
    writer_(new CheckpointWriter(output_file, applied, manager->Options().checksum)),
    start_tp_(std::chrono::system_clock::now()) {
}

CheckpointJob::~CheckpointJob() {
}

Status CheckpointJob::Run() {
    auto s = runJob();
    if (s.ok()) {
        markFinish(JobState::kSuccess);
        FLOG_INFO("{} run success: {}", name_, metric_.ToString());
    } else {
        if (cancel_flag_) {
            FLOG_INFO("{} is canceled, aborting", name_);
        } else {
            FLOG_ERROR("{} run failed: {}, aborting", name_, s.ToString())
        }
        s = writer_->Abort();
        if (!s.ok()) {
            FLOG_WARN("{} abort failed: {}", name_, s.ToString());
        }
        markFinish(JobState::kAborted);
    }
    return s;
}

Status CheckpointJob::dumpIter(CFType cf, IteratorPtr& iter) {
    while (iter->Valid()) {
        if (cancel_flag_) {
            return Status(Status::kAborted, "job is canceled", "");
        }
        metric_.bytes_written += iter->KeySize() + iter->ValueSize();
        metric_.keys_written += 1;
        auto s = writer_->Append(cf, iter->Key(), iter->Value());
        if (!s.ok()) {
            return s;
        }
        iter->Next();
    }
    return iter->status();
}

Status CheckpointJob::runJob() {
    auto s = writer_->Open();
    if (!s.ok()) {
        return Status(Status::kIOError, writer_->Path(), s.ToString());
    }

    s = dumpIter(CFType::kData, default_iter_);
    if (!s.ok()) {
        return s;
    }
    s = dumpIter(CFType::kTxn, txn_iter_);
    if (!s.ok()) {
        return s;
    }

    s = writer_->Finish();
    if (!s.ok()) {
        return s;
    }

    manager_->Commit(applied_);

    return Status::OK();
}

void CheckpointJob::Cancel() {
    cancel_flag_ = true;
}

void CheckpointJob::markFinish(JobState state) {
    assert(state != JobState::kRunning);
    writer_.reset();
    default_iter_.reset();
    txn_iter_.reset();

    auto end_tp = std::chrono::system_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end_tp - start_tp_).count();
    metric_.used_msecs = elapsed;
    {
        std::lock_guard<std::mutex> lock(mu_);
        assert(state_ == JobState::kRunning);
        state_ = state;
        cond_.notify_all();
    }
}

CheckpointJob::JobState CheckpointJob::WaitFinish() {
    std::unique_lock<std::mutex> lock(mu_);
    while (state_ == JobState::kRunning) {
        cond_.wait(lock);
    }
    return state_;
}

bool CheckpointJob::Finished() const {
    return state_ != JobState::kRunning;
}

CheckpointManager::CheckpointManager(const CheckpointOptions& opt, uint64_t id,
        std::string path, CheckpointSchedular *schedular) :
    opt_(opt),
    id_(id),
    path_(std::move(path)),
    schedular_(schedular) {
}

CheckpointManager::~CheckpointManager() {
    cancelRunningJob();
}

Status CheckpointManager::Open(std::unique_ptr<CheckpointReader>& recover_point) {
    // create if dir not exist
    if (!MakeDirAll(path_, 0755)) {
        return Status(Status::kIOError, "init checkpoint directory " + path_, strErrno(errno));
    }

    std::vector<std::string> ckp_files;
    auto s = listCheckpoints(ckp_files);
    if (!s.ok()) {
        return Status(s.code(), "list checkpoint " + path_, s.ToString());
    }
    // open recover point
    if (!ckp_files.empty()) {
        auto last_one = JoinFilePath({path_, ckp_files[ckp_files.size()-1]});
        recover_point.reset(new CheckpointReader(last_one, opt_.checksum));
        s = recover_point->Open();
        if (!s.ok()) {
            return Status(s.code(), "open " + last_one, s.ToString());
        }
        applied_ = recover_point->AppliedIndex();
    }

    // TODO: logging error
    clearTempFiles();
    clearHistories();

    return Status::OK();
}

Status CheckpointManager::listCheckpoints(std::vector<std::string>& ckp_files) {
    std::vector<std::string> files;
    if (!ListDirFiles(path_, files)) {
        return Status(Status::kIOError, "list checkpoint files at " + path_, strErrno(errno));
    }
    for (const auto& file : files) {
        if (isCheckpointFile(file)) {
            ckp_files.push_back(file);
        }
    }
    std::sort(ckp_files.begin(), ckp_files.end());
    return Status::OK();
}

Status CheckpointManager::clearTempFiles() {
    std::vector<std::string> tmp_files;
    if (!ListDirFiles(path_, tmp_files)) {
        return Status(Status::kIOError, "list checkpoint tmp files at " + path_, strErrno(errno));
    }
    for (const auto& file : tmp_files) {
        if (isTempFile(file)) {
            auto absolute_path = JoinFilePath({path_, file});
            auto ret = std::remove(absolute_path.c_str());
            if (ret != 0) {
                return Status(Status::kIOError, "remove file: " + absolute_path, strErrno(errno));
            }
        }
    }
    return Status::OK();
}

Status CheckpointManager::clearHistories() {
    std::vector<std::string> checkpoints;
    auto s = listCheckpoints(checkpoints);
    if (!s.ok()) {
        return s;
    }
    while (checkpoints.size() > opt_.max_history) {
        auto absolute_path = JoinFilePath({path_, checkpoints[0]});
        auto ret = std::remove(absolute_path.c_str());
        if (ret != 0) {
            return Status(Status::kIOError, "remove file: " + absolute_path, strErrno(errno));
        }
        checkpoints.erase(checkpoints.begin());
    }
    return Status::OK();
}

void CheckpointManager::cancelRunningJob() {
    if (current_job_) {
        current_job_->Cancel();
        current_job_->WaitFinish();
        current_job_.reset();
    }
}

void CheckpointManager::Commit(uint64_t index) {
    assert(index >= applied_);
    SetApplied(index);

    // TODO: logging error
    clearHistories();
    clearTempFiles();
}

Status CheckpointManager::Truncate() {
    cancelRunningJob();
    SetApplied(0);

    std::vector<std::string> all_files;
    if (!ListDirFiles(path_, all_files)) {
        return Status(Status::kIOError, "list checkpoint files at " + path_, strErrno(errno));
    }
    for (const auto& file : all_files) {
        if (isCheckpointFile(file) || isTempFile(file)) {
            auto absolute_path = JoinFilePath({path_, file});
            auto ret = std::remove(absolute_path.c_str());
            if (ret != 0) {
                return Status(Status::kIOError, "remove file: " + absolute_path, strErrno(errno));
            }
        }
    }
    return Status::OK();
}

Status CheckpointManager::Destroy() {
    cancelRunningJob();

    if (!RemoveDirAll(path_.c_str())) {
        return Status(Status::kIOError, "remove dir " + path_, strErrno(errno));
    }
    return Status::OK();
}

Status CheckpointManager::NewJob(uint64_t applied, IteratorPtr default_iter,
        IteratorPtr txn_iter, bool run_sync) {
    if (current_job_) {
        if (run_sync) {
            cancelRunningJob();
        } else if (!current_job_->Finished()) {
            return Status(Status::kBusy, "current running", current_job_->Name());
        }
    }
    assert(!current_job_ || current_job_->Finished());
    auto job_name = std::string("ckp/R") + std::to_string(id_) + "/A" + std::to_string(applied);
    auto output = makeCheckpointFile(path_, applied);
    current_job_.reset(new CheckpointJob(this, job_name, applied,
            std::move(default_iter), std::move(txn_iter), output));
    schedular_->Schedule(current_job_);
    if (run_sync) {
        current_job_->WaitFinish();
    }
    return Status::OK();
}

Status CheckpointManager::ApplySnapshotStart(uint64_t raft_index) {
    auto s = Truncate();
    if (!s.ok()) {
        return s;
    }

    auto output = makeCheckpointFile(path_, raft_index);
    snapshot_writer_.reset(new CheckpointWriter(output, raft_index, opt_.checksum));
    s = snapshot_writer_->Open();
    if (!s.ok()) {
        return Status(Status::kIOError, "open snapshot writer", s.ToString());
    }
    return Status::OK();
}

Status CheckpointManager::ApplySnapshotData(MassTreeBatch *batch) {
    if (snapshot_writer_ == nullptr) {
        return Status(Status::kInvalidArgument, "no available snapshot checkpoint context", "");
    }

    Status s;
    for (const auto& ent: batch->Entries()) {
        switch (ent.type) {
        case KvEntryType::kPut:
            s = snapshot_writer_->Append(ent.cf, ent.key, ent.value);
            break;
        default:
            s = Status(Status::kInvalidArgument, "invalid snapshot entry type", "delete");
        }
        if (!s.ok()) {
            return s;
        }
    }
    return s;
}

Status CheckpointManager::ApplySnapshotFinish(uint64_t raft_index) {
    if (snapshot_writer_ == nullptr) {
        return Status(Status::kInvalidArgument, "no available snapshot checkpoint context", "");
    }
    auto s = snapshot_writer_->Finish();
    if (!s.ok()) {
        return s;
    }
    snapshot_writer_.reset();
    SetApplied(raft_index);
    return s;
}

Status CheckpointManager::CopyTo(const std::string& dest_path) {
    // TODO: avoid be cleared during copying
    cancelRunningJob();

    if (!MakeDirAll(dest_path, 0755)) {
        return Status(Status::kIOError, "create checkpoint path " + dest_path, strErrno(errno));
    }

    std::vector<std::string> files;
    listCheckpoints(files);
    if (!files.empty()) {
        const auto& last_name = files[files.size()-1];
        auto src_file = JoinFilePath({path_, last_name});
        auto dest_file = JoinFilePath({dest_path, last_name});
        auto ret = ::link(src_file.c_str(), dest_file.c_str());
        if (ret != 0) {
            return Status(Status::kIOError, "link checkpoint " + src_file  + " to " + dest_file, strErrno(errno));
        }
    }
    return Status::OK();
}

CheckpointSchedular::CheckpointSchedular(const Options& opt) :
    opt_(opt) {
    for (size_t i = 0; i < opt.num_threads; ++i) {
        threads_.emplace_back(std::thread([this]{ workLoop(); }));
        std::string name = "ckp" + std::to_string(i);
        AnnotateThread(threads_.back().native_handle(), name.c_str());
    }
}

CheckpointSchedular::~CheckpointSchedular() {
    {
        std::lock_guard<std::mutex> lock(mu_);
        running_ = false;
    }
    cond_.notify_all();
    for (auto& t : threads_) {
        t.join();
    }
}

void CheckpointSchedular::workLoop() {
    while (true) {
        std::shared_ptr<CheckpointJob> job;
        {
            std::unique_lock<std::mutex> lock(mu_);
            while (running_ && que_.empty()) {
                cond_.wait(lock);
            }
            if (!running_) {
                return;
            }
            job = que_.front();
            que_.pop();
        }
        if (job) {
            job->Run();
        }
    }
}

void CheckpointSchedular::Schedule(std::shared_ptr<CheckpointJob> job) {
    std::lock_guard<std::mutex> lock(mu_);
    que_.push(job);
    cond_.notify_all();
}

} // namespace db
} // namespace ds
} // namespace jim
