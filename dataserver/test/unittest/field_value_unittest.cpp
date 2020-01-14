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

#include "common/ds_encoding.h"
#include "storage/field_value.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::ds::storage;

TEST(FieldVal, Types) {
    {
        int64_t i = 123;
        FieldValue val(i);
        ASSERT_EQ(val.Type(), FieldType::kInt);
        ASSERT_EQ(val.Int(), i);
        ASSERT_EQ(val.UInt(), 0U);
        ASSERT_EQ(val.Double(), 0);
        ASSERT_EQ(val.Bytes(), "");
    }

    {
        uint64_t i = 123;
        FieldValue val(i);
        ASSERT_EQ(val.Type(), FieldType::kUInt);
        ASSERT_EQ(val.UInt(), i);
        ASSERT_EQ(val.Int(), 0);
    }

    {
        double i = 123;
        FieldValue val(i);
        ASSERT_EQ(val.Type(), FieldType::kDouble);
        ASSERT_EQ(val.Double(), i);
    }

    {
        auto s = new std::string("123");
        FieldValue val(s);
        ASSERT_EQ(val.Type(), FieldType::kBytes);
        ASSERT_EQ(val.Bytes(), *s);
    }
}

TEST(FieldVal, Compare) {
    {
        int64_t a = -456, b = -123;
        FieldValue fa(a), fb(b), fc(b);
        EXPECT_TRUE(fcompare(fa, fb, CompareOp::kLess));
        EXPECT_FALSE(fcompare(fb, fa, CompareOp::kLess));
        EXPECT_TRUE(fcompare(fb, fa, CompareOp::kGreater));
        EXPECT_FALSE(fcompare(fa, fb, CompareOp::kEqual));
        EXPECT_TRUE(fcompare(fb, fc, CompareOp::kEqual));
    }

    {
        uint64_t a = 123, b = 456;
        FieldValue fa(a), fb(b), fc(b);
        EXPECT_TRUE(fcompare(fa, fb, CompareOp::kLess));
        EXPECT_FALSE(fcompare(fb, fa, CompareOp::kLess));
        EXPECT_TRUE(fcompare(fb, fa, CompareOp::kGreater));
        EXPECT_FALSE(fcompare(fa, fb, CompareOp::kEqual));
        EXPECT_TRUE(fcompare(fb, fc, CompareOp::kEqual));
    }

    {
        double a = 123, b = 456;
        FieldValue fa(a), fb(b), fc(b);
        EXPECT_TRUE(fcompare(fa, fb, CompareOp::kLess));
        EXPECT_FALSE(fcompare(fb, fa, CompareOp::kLess));
        EXPECT_TRUE(fcompare(fb, fa, CompareOp::kGreater));
        EXPECT_FALSE(fcompare(fa, fb, CompareOp::kEqual));
        EXPECT_TRUE(fcompare(fb, fc, CompareOp::kEqual));
    }

    {
        std::string* a = new std::string("123");
        std::string* b = new std::string("456");
        std::string* c = new std::string("456");
        FieldValue fa(a), fb(b), fc(c);
        EXPECT_TRUE(fcompare(fa, fb, CompareOp::kLess));
        EXPECT_FALSE(fcompare(fb, fa, CompareOp::kLess));
        EXPECT_TRUE(fcompare(fb, fa, CompareOp::kGreater));
        EXPECT_FALSE(fcompare(fa, fb, CompareOp::kEqual));
        EXPECT_TRUE(fcompare(fb, fc, CompareOp::kEqual));
    }
}

TEST(FieldVal, Encode) {
    {
        std::string buf;
        EncodeFieldValue(&buf, nullptr);
        EXPECT_EQ(EncodeToHexString(buf), "01");
    }

    {
        int64_t i = -123;
        FieldValue val(i);
        std::string buf;
        EncodeFieldValue(&buf, &val);
        EXPECT_EQ(EncodeToHexString(buf), "03f501");
    }

    {
        uint64_t i = 123;
        FieldValue val(i);
        std::string buf;
        EncodeFieldValue(&buf, &val);
        EXPECT_EQ(EncodeToHexString(buf), "03f601");
    }

    {
        double i = 123.123;
        FieldValue val(i);
        std::string buf;
        EncodeFieldValue(&buf, &val);
        EXPECT_EQ(EncodeToHexString(buf), "04405ec7df3b645a1d");
    }

    {
        auto s = new std::string("abcdefg \x04");
        FieldValue val(s);
        std::string buf;
        EncodeFieldValue(&buf, &val);
        EXPECT_EQ(EncodeToHexString(buf), "0609616263646566672004");
    }
}

} /* namespace  */
