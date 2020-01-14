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

_Pragma("once");

#include <stdio.h>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>
#include <queue>
#include <functional>
#include <condition_variable>
#include <thread>
#include <chrono>

#include "base/status.h"
#include "common/masstree_options.h"
#include "db/db.h"

namespace jim {
namespace ds {
namespace db {

static const uint32_t kCheckpointMagic = 0x43424350; // "CBCK"
static const uint16_t kCheckpointVersion = 1;

// fixed 128 bytes checkpoint file header
struct CheckpointHeader {
    static constexpr size_t kSize = 128;

    uint32_t magic = kCheckpointMagic;
    uint16_t version = kCheckpointVersion;
    uint32_t timestamp = 0;
    uint64_t applied_index = 0;  // raft applied index
    char reserved[110] = {'\0'};  // keep sizeof(CheckpointHeader) == kSize

    // convert to big-endian when write to file
    void Encode();
    // convert to host-endian when read from file
    bool Decode();

    std::string ToString() const;

} __attribute__((packed));


class CheckpointWriter {
public:
    CheckpointWriter(const std::string& file_path, uint64_t applied, bool checksum = true);
    ~CheckpointWriter();

    CheckpointWriter(const CheckpointWriter&) = delete;
    CheckpointWriter& operator=(const CheckpointWriter&) = delete;

    Status Open();
    Status Append(CFType cf, const std::string& key, const std::string& value);
    Status Finish();
    Status Abort();

    uint32_t Timestamp() const { return timestamp_; }
    const std::string& Path() const { return file_path_; }

private:
    void encRecord(CFType cf, const std::string& key, const std::string& value, std::string& buf);
    Status writeCRC();

private:
    const std::string file_path_;
    const std::string tmp_file_path_;
    const bool checksum_ = false;

    CheckpointHeader header_;
    uint32_t timestamp_ = 0;
    uint64_t crc_ = 0;
    FILE *fp_ = nullptr;
};

class CheckpointReader {
public:
    explicit CheckpointReader(const std::string& path, bool verify_checksum = true);
    ~CheckpointReader();

    CheckpointReader(const CheckpointReader&) = delete;
    CheckpointReader& operator=(const CheckpointReader&) = delete;

    Status Open();

    const CheckpointHeader& Header() const;

    uint64_t AppliedIndex() const;

    Status Next(CFType &cf, std::string& key, std::string& value, bool& over);

private:
    Status readRecord(CFType &cf, std::string& key, std::string& value, bool &over);

private:
    const std::string path_;
    const bool verify_checksum_ = false;
    bool opened_ = false;
    uint64_t crc_ = 0;
    FILE *fp_ = nullptr;
    CheckpointHeader header_;
};


class CheckpointManager;

class CheckpointJob {
public:
    enum JobState { kRunning = 0, kSuccess = 1, kAborted = 2 };

    struct JobMetric {
        size_t keys_written = 0;
        size_t bytes_written = 0;
        uint64_t used_msecs = 0;

        std::string ToString() const;
    };

    CheckpointJob(CheckpointManager* manager, const std::string& job_name,
            uint64_t applied, IteratorPtr default_iter, IteratorPtr txn_iter,
            const std::string& output_file);

    ~CheckpointJob();

    CheckpointJob(const CheckpointJob&) = delete;
    CheckpointJob& operator=(const CheckpointJob&) = delete;

    Status Run();
    void Cancel();
    JobState WaitFinish();
    bool Finished() const;

    const std::string& Name() const { return name_; }

private:
    Status dumpIter(CFType cf, IteratorPtr& iter);
    Status runJob();
    void markFinish(JobState state);

private:
    using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

    CheckpointManager* const manager_ = nullptr;
    const std::string name_;
    const uint64_t applied_ = 0;
    IteratorPtr default_iter_;
    IteratorPtr txn_iter_;
    std::unique_ptr<CheckpointWriter> writer_;

    std::atomic<JobState> state_ = {JobState::kRunning};
    std::mutex mu_;
    std::condition_variable cond_;

    std::atomic<bool> cancel_flag_ = {false};
    TimePoint start_tp_;
    JobMetric metric_;
};

class CheckpointSchedular;
class MassTreeBatch;

class CheckpointManager {
public:
    CheckpointManager(const CheckpointOptions& opt, uint64_t id,
            std::string path, CheckpointSchedular *schedular);
    ~CheckpointManager();

    CheckpointManager(const CheckpointManager&) = delete;
    CheckpointManager& operator=(const CheckpointManager&) = delete;

    const CheckpointOptions Options() const { return opt_; }

    Status Open(std::unique_ptr<CheckpointReader>& recover_point);

    uint64_t Applied() const { return applied_; }
    void SetApplied(uint64_t index) { applied_ = index; }

    void Commit(uint64_t index);

    // remove all file in directory
    Status Truncate();

    // remove whole directory
    Status Destroy();

    Status NewJob(uint64_t applied, IteratorPtr default_iter, IteratorPtr txn_iter,
            bool run_sync = false);

    Status ApplySnapshotStart(uint64_t raft_index);
    Status ApplySnapshotData(MassTreeBatch *batch);
    Status ApplySnapshotFinish(uint64_t raft_index);

    Status CopyTo(const std::string& dest_path);

private:
    Status listCheckpoints(std::vector<std::string>& ckp_files);
    Status clearTempFiles();
    Status clearHistories();
    void cancelRunningJob();

private:
    const CheckpointOptions opt_;
    const uint64_t id_ = 0;
    const std::string path_;
    CheckpointSchedular* schedular_ = nullptr;

    std::atomic<uint64_t> applied_ = {0};
    std::unique_ptr<CheckpointWriter> snapshot_writer_;
    std::shared_ptr<CheckpointJob> current_job_;
};

// CheckpointSchedular schedule
class CheckpointSchedular {
public:
    struct Options {
        size_t num_threads = 4;
    };

public:
    explicit CheckpointSchedular(const Options& opt);
    ~CheckpointSchedular();

    CheckpointSchedular(const CheckpointSchedular&) = delete;
    CheckpointSchedular& operator=(const CheckpointSchedular&) = delete;

    void Schedule(std::shared_ptr<CheckpointJob> job);

private:
    void workLoop();

private:
    using JobQueue = std::queue<std::shared_ptr<CheckpointJob>> ;

    const Options opt_;
    JobQueue que_;
    bool running_ = true;
    std::mutex mu_;
    std::condition_variable cond_;
    std::vector<std::thread> threads_;
};

} // namespace db
} // namespace ds
} // namespace jim
