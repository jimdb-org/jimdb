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

#include <unistd.h>
#include <getopt.h>
#include <signal.h>
#include <sys/file.h>

#include <iostream>
#include <string>

#include "base/util.h"
#include "base/fs_util.h"
#include "common/server_config.h"
#include "common/logger.h"
#include "version.h"
#include "server.h"

using namespace jim;
using namespace jim::ds::server;

int pid_fd = -1;

void printUsage(const char *name) {
    std::cout << name << " usage: " << std::endl;
    std::cout << "\t-c or --conf=<config file>: required, configure file" << std::endl;
    std::cout << "\t-t or --test: test scene" << std::endl;
    std::cout << "\t-d or --debug: non-daemon processes for debugging" << std::endl;
    std::cout << "\t-v or --version: print version info" << std::endl;
}

void daemonInit() {
    pid_t pid;

    if((pid=fork()) != 0) {
        exit(0);
    }

    if (setsid() == -1) {
        FLOG_CRIT("setsid fail, errno: {}, error info: {}", errno, strErrno(errno));
        exit(EXIT_FAILURE);
    }

    umask(0);

    if((pid=fork()) != 0) {
        ::close(pid_fd);
        exit(0);
    }

    if (chdir("/") != 0) {
        FLOG_WARN("change directory to / fail, errno: {}, error info: {}",
                errno, strErrno(errno));
    }
}

bool lockPid(const std::string &pid_file) {
    auto pid_path = GetDirName(pid_file);
    if (!MakeDirAll(pid_path, 0755)) {
        FLOG_ERROR("create pid path {} failed: {}", pid_path, strErrno(errno));
        return false;
    }

    pid_fd = ::open(pid_file.c_str(), O_CREAT | O_RDWR, 0644);
    if (pid_fd < 0) {
        FLOG_ERROR("open pid file {} failed: {}", pid_file, strErrno(errno));
        return false;
    }

    if (flock(pid_fd, LOCK_EX | LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
            char buf[16] = {'\0'};
            auto read_ret = ::read(pid_fd, buf, sizeof(buf));
            (void)read_ret;
            FLOG_ERROR("process is already running, pid: {}", buf);
        } else {
            FLOG_ERROR("lock pid file {} failed: {}", pid_file, strErrno(errno));
        }
        return false;
    } else {
        return true;
    }
}

bool writePid(const std::string &pid_file) {
    if (ftruncate(pid_fd, 0) == -1) {
        FLOG_ERROR("truncate pid file {} failed: {}", pid_file, strErrno(errno));
        return false;
    }

    char pid_buf[16] = {'\0'};
    snprintf(pid_buf, 16, "%d", getpid());
    if (write(pid_fd, pid_buf, strlen(pid_buf)) < 0) {
        FLOG_ERROR("write pid file {} failed: {}", pid_file, strErrno(errno));
        return false;
    }

    return true;
}

void sigQuitHandler(int sig) {
    DataServer::Instance().Stop();
}

void sigHupHandler(int sig) {
    FLOG_INFO("catch signal {}", sig);
}

void sigUsrHandler(int sig) {
    if (sig == SIGUSR1) {
#ifdef USE_GPERF
        static bool prof_trigger = false;
        if (!prof_trigger) {
            ProfilerStart("/tmp/ds.pprof");
        } else {
            ProfilerStop();
        }
        prof_trigger = !prof_trigger;
#endif
    } else if (sig == SIGUSR2) {
        static bool log_debug_trigger = false;
        if (!log_debug_trigger) {
            std::string level = "debug";
            LoggerSetLevel(level);
        } else {
            LoggerSetLevel(ds_config.logger_config.level);
        }
        log_debug_trigger = !log_debug_trigger;
    } else {
        FLOG_INFO("catch signal {}, ignore it", sig);
    }
}

bool setSignalHandler() {
#ifdef __linux__
    struct sigaction act;
    memset(&act, 0, sizeof(act));
    sigemptyset(&act.sa_mask);

    act.sa_handler = sigUsrHandler;
    if (sigaction(SIGUSR1, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGUSR1 fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }
    if (sigaction(SIGUSR2, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGUSR2 fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }

    act.sa_handler = sigHupHandler;
    if (sigaction(SIGHUP, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGHUP fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }

    act.sa_handler = SIG_IGN;
    if (sigaction(SIGPIPE, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGPIPE fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }

    act.sa_handler = sigQuitHandler;
    if (sigaction(SIGINT, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGINT fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }

    if (sigaction(SIGTERM, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGTERM fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }
    if ( sigaction(SIGQUIT, &act, NULL) < 0) {
        FLOG_CRIT("sigaction SIGQUIT fail, errno: {}, error info: {}",
                errno, strErrno(errno));
        FLOG_CRIT("exit abnormally!");
        return false;
    }
#endif

    return true;
}

void runServer(const std::string &server_name, const std::string& conf_file, bool daemon, bool btest = false) {
    // load config file
    if (!ds_config.LoadFromFile(conf_file, btest)) {
        exit(EXIT_FAILURE);
    }

    if (!lockPid(ds_config.pid_file)) {
        exit(EXIT_FAILURE);
    }
    if (daemon) {
        daemonInit();
    }
    if (!writePid(ds_config.pid_file)) {
        exit(EXIT_FAILURE);
    }

    // init logger
    if (!LoggerInit(ds_config.logger_config)) {
        exit(EXIT_FAILURE);
    }
    if (!setSignalHandler()) {
        exit(EXIT_FAILURE);
    }

    if (!DataServer::Instance().Start()) {
        exit(EXIT_FAILURE);
    }

    while (true) {
        DataServer::Instance().ServerCron();
        // use node heartbeat interval to trigger server cron jobs
        sleep(ds_config.cluster_config.node_interval_secs);
    }
}

int main(int argc, char *argv[]) {
    struct option longopts[] = {
        { "conf",      optional_argument,   NULL,   'c' },
        { "version",   no_argument,         NULL,   'v' },
        { "debug",     no_argument,         NULL,   'd' },
        { "test",      no_argument,         NULL,   't' },
        { "help",      no_argument,         NULL,   'h' },
        { NULL,        0,                   NULL,    0  }
    };

    int ch = 0;
    bool daemon = true;
    bool btest = false;
    std::string server_name = argv[0];
    std::string conf_file;

    while ((ch = getopt_long(argc, argv, "c:vdth", longopts, NULL)) != -1) {
        switch (ch) {
            case 'v':
                std::cout << GetVersionInfo() << std::endl;
                return 0;
            case 'c':
                if (optarg != nullptr) conf_file = optarg;
                break;
            case 'd':
                daemon = false;
                break;
            case 't':
                btest = true;
                break;
            default:
                printUsage(argv[0]);
                return 0;
        }
    }

    if (conf_file.empty()) {
        std::cerr << server_name << ": configure file is required" << std::endl;
        printUsage(argv[0]);
        return -1;
    }

    runServer(server_name, conf_file, daemon, btest);

    return 0;
}
