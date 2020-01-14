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

#include <gtest/gtest.h>

#include "base/status.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;

TEST(STATUS, Basic) {
    {
        Status s;
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(s.code(), Status::kOk);
        ASSERT_EQ(s.ToString(), "ok");
    }

    {
        Status s = Status::OK();
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(s.ToString(), "ok");
    }

    {
        Status s = Status(Status::kBusy, "msg1", "msg2");
        ASSERT_EQ(s.code(), Status::kBusy);
        ASSERT_EQ(s.ToString(), "Busy: msg1: msg2");
    }

    {
        Status s = Status(Status::kBusy, "msg1", "msg2");
        Status s1 = s;
        ASSERT_EQ(s1, s);
        ASSERT_EQ(s1.code(), Status::kBusy);
        ASSERT_EQ(s1.ToString(), "Busy: msg1: msg2");
        Status s2(s);
        ASSERT_EQ(s2, s);
        ASSERT_EQ(s2.code(), Status::kBusy);
        ASSERT_EQ(s2.ToString(), "Busy: msg1: msg2");

        Status s3(std::move(s));
        ASSERT_TRUE(s.ok());
        ASSERT_EQ(s.ToString(), "ok");
        ASSERT_EQ(s3, s2);
        ASSERT_EQ(s3.code(), Status::kBusy);
        ASSERT_EQ(s3.ToString(), "Busy: msg1: msg2");

        Status s4 = std::move(s1);
        ASSERT_TRUE(s1.ok());
        ASSERT_EQ(s1.ToString(), "ok");
        ASSERT_EQ(s4, s2);
        ASSERT_EQ(s4.code(), Status::kBusy);
        ASSERT_EQ(s4.ToString(), "Busy: msg1: msg2");

        Status s5;
        s5 = s2;
        ASSERT_EQ(s5, s2);
        ASSERT_EQ(s5.code(), Status::kBusy);
        ASSERT_EQ(s5.ToString(), "Busy: msg1: msg2");
    }
}

} /* namespace  */
