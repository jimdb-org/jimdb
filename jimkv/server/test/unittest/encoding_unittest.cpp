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

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <iomanip>
#include <iostream>
#include <limits>
#include <random>
#include <cmath>

#include "base/util.h"
#include "common/ds_encoding.h"

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace std;
using namespace jimkv;

std::string toHex(const std::string& str) {
    std::string result;
    char buf[3];
    for (std::string::size_type i = 0; i < str.size(); ++i) {
        snprintf(buf, sizeof(buf), "%02x", static_cast<unsigned char>(str[i]));
        result.append(buf, 2);
    }
    return result;
}

TEST(Encoding, NonSortingUVariant) {
    auto testFunc = [](uint64_t num, const std::string& expected_buf) {
        std::string buf;
        EncodeNonSortingUvarint(&buf, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }
        ASSERT_EQ(buf.size(), NonSortingUvarintSize(num));

        uint64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeNonSortingUvarint(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);
    };

    uint64_t a = 1;
    testFunc(0, "00");
    testFunc(a << 7, "8100");
    testFunc(a << 14, "818000");
    testFunc(a << 21, "81808000");
    testFunc(a << 28, "8180808000");
    testFunc(a << 35, "818080808000");
    testFunc(a << 42, "81808080808000");
    testFunc(a << 49, "8180808080808000");
    testFunc(a << 56, "818080808080808000");
    testFunc(std::numeric_limits<uint64_t>::max(), "81ffffffffffffffff7f");
}

TEST(Encoding, NonSortingVariant) {
    auto testFunc = [](int64_t num, const std::string& expected_buf) {
        std::string buf;
        EncodeNonSortingVarint(&buf, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }
        ASSERT_EQ(buf.size(), NonSortingVarintSize(num));

        int64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeNonSortingVarint(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);
    };

    testFunc(0, "00");
    testFunc(1, "02");
    testFunc(-1, "01");
    testFunc(123, "f601");
    testFunc(-123, "f501");
    testFunc(std::numeric_limits<int64_t>::min(), "ffffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::max(), "feffffffffffffffff01");
}

TEST(Encoding, IntValue) {
    auto testFunc = [](int64_t num, uint32_t col_id, const std::string expected_buf) {
        std::string buf;
        EncodeIntValue(&buf, col_id, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }
        ASSERT_EQ(IntValueSize(col_id, num), buf.size());

        int64_t value;
        size_t offset = 0;
        ASSERT_TRUE(DecodeIntValue(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
        ASSERT_LE(offset, buf.size());
        ASSERT_EQ(type, EncodeType::Int);
        ASSERT_EQ(actual_col_id, col_id);
    };

    testFunc(0, kNoColumnID, "0300");
    testFunc(123, kNoColumnID, "03f601");
    testFunc(-123, kNoColumnID, "03f501");
    testFunc(std::numeric_limits<int64_t>::max(), kNoColumnID, "03feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), kNoColumnID, "03ffffffffffffffffff01");

    testFunc(0, 123, "8f3300");
    testFunc(123, 123, "8f33f601");
    testFunc(-123, 123, "8f33f501");
    testFunc(std::numeric_limits<int64_t>::max(), 123, "8f33feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), 123, "8f33ffffffffffffffffff01");

    uint32_t max_col = std::numeric_limits<uint32_t>::max();
    testFunc(0, max_col, "81ffffffff7300");
    testFunc(123, max_col, "81ffffffff73f601");
    testFunc(-123, max_col, "81ffffffff73f501");
    testFunc(std::numeric_limits<int64_t>::max(), max_col, "81ffffffff73feffffffffffffffff01");
    testFunc(std::numeric_limits<int64_t>::min(), max_col, "81ffffffff73ffffffffffffffffff01");
}

TEST(Encoding, FloatValue) {
    auto testFunc = [](double num, uint32_t col_id, const std::string expected_buf) {
        std::string buf;
        EncodeFloatValue(&buf, col_id, num);
        if (!expected_buf.empty()) {
            ASSERT_EQ(toHex(buf), expected_buf)
                << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        double value = 0;
        size_t offset = 0;
        ASSERT_TRUE(DecodeFloatValue(buf, offset, &value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, num);

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
        ASSERT_LE(offset, buf.size());
        ASSERT_EQ(type, EncodeType::Float);
        ASSERT_EQ(actual_col_id, col_id);
    };

    testFunc(0, kNoColumnID, "040000000000000000");
    testFunc(0.00123, kNoColumnID, "043f5426fe718a86d7");
    testFunc(-0.00123, kNoColumnID, "04bf5426fe718a86d7");
    double val = 1.797693134862315708145274237317043567981e+30;
    testFunc(val, kNoColumnID, "");
}

TEST(Encoding, BytesValue) {
    std::string buf;
    EncodeBytesValue(&buf, 123, "aabbccdd", 8);
    ASSERT_EQ(toHex(buf), "8f36086161626263636464");

    std::string value;
    size_t offset = 0;
    ASSERT_TRUE(DecodeBytesValue(buf, offset, &value));
    ASSERT_EQ(offset, buf.size());
    ASSERT_EQ(value, "aabbccdd");

    EncodeType type;
    offset = 0;
    uint32_t actual_col_id = 0;
    ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
    ASSERT_LE(offset, buf.size());
    ASSERT_EQ(type, EncodeType::Bytes);
    ASSERT_EQ(actual_col_id, 123U);
}

TEST(Encoding, DecimalValue) {
    for (auto i = 0; i < 1000; i++ ) {
        std::string buf;
        datatype::MyDecimal dec_src;
        dec_src.FromInt(i);
        EncodeDecimalValue(&buf, i, &dec_src);

        toHex(buf);

        size_t offset = 0;

        datatype::MyDecimal dec_dst;
        offset = 0;
        ASSERT_TRUE(DecodeDecimalValue(buf, offset, &dec_dst));
        ASSERT_EQ(offset, buf.size());
        ASSERT_TRUE(dec_src.Compare(dec_dst) == 0 );

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
        ASSERT_LE(offset, buf.size());
        ASSERT_EQ(type, EncodeType::Decimal);
        ASSERT_EQ(actual_col_id, i);
    }
}

TEST(Encoding, DateValue) {
        auto func_data = [](const int32_t begin, const int32_t end) -> int32_t  {
            std::random_device r;
            std::default_random_engine e1(r());
            std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
            int mean = uniform_dist(e1);
            return mean;
        };

        for (auto i = 0; i < 1000; i++) {
            std::string buf;
            datatype::MyDateTime dt_src;
            dt_src.SetYear(func_data(1000, 9999));
            dt_src.SetMonth(func_data(1,12));
            dt_src.SetDay(func_data(1, 28));
            dt_src.SetHour(func_data(0, 23));
            dt_src.SetMinute(func_data(0, 59));
            dt_src.SetSecond(func_data(0, 59));
            dt_src.SetMicroSecond(func_data(0, 999999));

            EncodeDateValue(&buf, i, &dt_src);

            toHex(buf);

            size_t offset = 0;
            datatype::MyDateTime dt_dst;
            offset = 0;
            ASSERT_TRUE(DecodeDateValue(buf, offset, &dt_dst))
                << "buf:" << EncodeToHexString(buf)
                << ", offset:" << offset
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", i:" << i
                << ", Time->src: " << dt_src ;

            ASSERT_EQ(offset, buf.size())
                << "buf:" << EncodeToHexString(buf)
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
                << ", offset:" << offset
                << ", buf.size():" << buf.size()
                << ", i:" << i
                << ", Time->src: " << dt_src
                << ", Time->dst: " << dt_dst;

            ASSERT_TRUE(dt_src == dt_dst)
                << "buf:" << EncodeToHexString(buf)
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
                << ", i:" << i
                << ", Time->src: " << dt_src
                << ", Time->dst: " << dt_dst;

            EncodeType type;
            offset = 0;
            uint32_t actual_col_id = 0;
            ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type))
                << "buf:" << EncodeToHexString(buf)
                << ", offset:" << offset
                << ", i:" << i
                <<  ", actual_col_id:" << actual_col_id
                << ", type:" << static_cast<int>(type)
                << ", Time->src: " << dt_src;

            ASSERT_LE(offset, buf.size())
                << "buf:" << EncodeToHexString(buf)
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", offset:" << offset
                << ", buf.size():" << buf.size()
                << ", i:" << i
                << ", Time->src: " << dt_src;
            ASSERT_EQ(type, EncodeType::Date)
                << "buf:" << EncodeToHexString(buf)
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", type:" << static_cast<int>(type)
                << ", i:" << i
                << ", Time->src: " << dt_src;
            ASSERT_EQ(actual_col_id, i)
                << "buf:" << EncodeToHexString(buf)
                << ", Time->src.ToInt:" << dt_src.ToPackInt64()
                << ", actual_col_id:" << actual_col_id
                << ", i:" << i
                << ", Time->src: " << dt_src;
        }
}

TEST(Encoding, TimeValue) {
    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t  {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 1000; i++) {
        std::string buf;
        datatype::MyTime dt_src;
        dt_src.SetHour(func_data(0, 838));
        dt_src.SetMinute(func_data(0, 59));
        dt_src.SetSecond(func_data(0, 59));
        dt_src.SetMicroSecond(func_data(0, 999999));
        dt_src.SetNeg(func_data(1,10000)%2 == 0);

        EncodeTimeValue(&buf, i, &dt_src);

        toHex(buf);

        size_t offset = 0;
        datatype::MyTime dt_dst;
        offset = 0;
        ASSERT_TRUE(DecodeTimeValue(buf, offset, &dt_dst))
            << "buf:" << EncodeToHexString(buf)
            << ", offset:" << offset
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src ;
        ASSERT_EQ(offset, buf.size())
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", offset:" << offset
            << ", buf.size():" << buf.size()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;
        ASSERT_TRUE(dt_src == dt_dst)
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;

        EncodeType type;
        offset = 0;
        uint32_t actual_col_id = 0;
        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type))
            << "buf:" << EncodeToHexString(buf)
            << ", offset:" << offset
            << ", i:" << i
            <<  ", actual_col_id:" << actual_col_id
            << ", type:" << static_cast<int>(type)
            << ", Time->src: " << dt_src;

        ASSERT_LE(offset, buf.size())
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", offset:" << offset
            << ", buf.size():" << buf.size()
            << ", i:" << i
            << ", Time->src: " << dt_src;

        ASSERT_EQ(type, EncodeType::Time)
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", type:" << static_cast<int>(type)
            << ", i:" << i
            << ", Time->src: " << dt_src;
        ASSERT_EQ(actual_col_id, i)
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", actual_col_id:" << actual_col_id
            << ", i:" << i
            << ", Time->src: " << dt_src;
    }
}

TEST(Encoding, AscInt) {
    std::string buf;
    EncodeVarintAscending(&buf, 0);
    ASSERT_EQ(toHex(buf), "88");
    int64_t value = 0;
    size_t offset = 0;
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, 0);

    EncodeVarintAscending(&buf, -100);
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, -100);

    EncodeVarintAscending(&buf, 1);
    ASSERT_TRUE(DecodeVarintAscending(buf, offset, &value));
    ASSERT_EQ(value, 1);
}

TEST(Encoding, DescUvarint) {
    auto testFunc = [](uint64_t value, const std::string expected_hex) {
        std::string buf;
        EncodeUvarintDescending(&buf, value);
        if (!expected_hex.empty()) {
            ASSERT_EQ(toHex(buf), expected_hex) << "incorrect encoded result: " << toHex(buf) << std::endl;
        }

        uint64_t decode_value = 0;
        size_t offset = 0;
        ASSERT_TRUE(DecodeUvarintDescending(buf, offset, &decode_value));
        ASSERT_EQ(offset, buf.size());
        ASSERT_EQ(value, decode_value);
    };

    testFunc(0, "88");
    testFunc(1, "87fe");

    testFunc(0xff, "8700");   // 1
    testFunc(0xff + 1, "86feff");

    testFunc(0xffffUL, "860000"); // 2
    testFunc(0xffffUL + 1, "85feffff");

    testFunc(0xffffffUL, "85000000"); // 3
    testFunc(0xffffffUL + 1, "84feffffff");

    testFunc(0xffffffffUL, "8400000000"); // 4
    testFunc(0xffffffffUL + 1, "83feffffffff");

    testFunc(0xffffffffffUL, "830000000000"); // 5
    testFunc(0xffffffffffUL + 1, "82feffffffffff");

    testFunc(0xffffffffffffUL, "82000000000000"); // 6
    testFunc(0xffffffffffffUL + 1, "81feffffffffffff");

    testFunc(0xffffffffffffffUL, "8100000000000000"); // 7
    testFunc(0xffffffffffffffUL + 1, "80feffffffffffffff");

    testFunc(std::numeric_limits<uint64_t>::max(), "800000000000000000"); // 8
}

TEST(Encoding, BytesAscending) {
    for (int i = 0; i < 1000; ++i) {
        auto raw = randomString(1, 100);
        std::string buf;
        EncodeBytesAscending(&buf, raw.c_str(), raw.size());
        std::string decoded;
        size_t offset = 0;
        ASSERT_TRUE(DecodeBytesAscending(buf, offset, &decoded));
        ASSERT_EQ(decoded, raw);
    }
}

TEST(Encoding, DecimalAscending) {
    for (auto i = 0; i < 1000; i++ ) {
        std::string buf;
        datatype::MyDecimal dec_src;
        dec_src.FromInt(i);
        EncodeDecimalAscending(&buf, &dec_src);

        toHex(buf);

        size_t offset = 0;

        datatype::MyDecimal dec_dst;
        offset = 0;
        ASSERT_TRUE(DecodeDecimalAscending(buf, offset, &dec_dst));
        ASSERT_EQ(offset, buf.size());
        ASSERT_TRUE(dec_src.Compare(dec_dst) == 0 );
    }
}

TEST(Encoding, DateAscending) {
    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t  {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 1000; i++) {
        std::string buf;
        datatype::MyDateTime dt_src;
        dt_src.SetYear(func_data(1000, 9999));
        dt_src.SetMonth(func_data(1,12));
        dt_src.SetDay(func_data(1, 28));
        dt_src.SetHour(func_data(0, 23));
        dt_src.SetMinute(func_data(0, 59));
        dt_src.SetSecond(func_data(0, 59));
        dt_src.SetMicroSecond(func_data(0, 999999));
        dt_src.SetNeg(func_data(1,10000)%2 == 0);

        EncodeDateAscending(&buf, &dt_src);

        toHex(buf);

        size_t offset = 0;
        datatype::MyDateTime dt_dst;
        offset = 0;

        ASSERT_TRUE(DecodeDateAscending(buf, offset, &dt_dst))
            << "buf:" << EncodeToHexString(buf)
            << ", offset:" << offset
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src ;

        ASSERT_EQ(offset, buf.size())
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", offset:" << offset
            << ", buf.size():" << buf.size()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;

        ASSERT_TRUE(dt_src == dt_dst)
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;
    }
}

TEST(Encoding, TimeAscending) {
    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t  {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 1000; i++) {
        std::string buf;
        datatype::MyTime dt_src;
        dt_src.SetHour(func_data(0, 838));
        dt_src.SetMinute(func_data(0, 59));
        dt_src.SetSecond(func_data(0, 59));
        dt_src.SetMicroSecond(func_data(0, 999999));
        dt_src.SetNeg(func_data(1,10000)%2 == 0);

        EncodeTimeAscending(&buf, &dt_src);

        toHex(buf);

        size_t offset = 0;
        datatype::MyTime dt_dst;
        offset = 0;

        ASSERT_TRUE(DecodeTimeAscending(buf, offset, &dt_dst))
            << "buf:" << EncodeToHexString(buf)
            << ", offset:" << offset
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src ;

        ASSERT_EQ(offset, buf.size())
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", offset:" << offset
            << ", buf.size():" << buf.size()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;

        ASSERT_TRUE(dt_src == dt_dst)
            << "buf:" << EncodeToHexString(buf)
            << ", Time->src.ToInt:" << dt_src.ToPackInt64()
            << ", Time->dst.ToInt:" << dt_dst.ToPackInt64()
            << ", i:" << i
            << ", Time->src: " << dt_src
            << ", Time->dst: " << dt_dst;
    }
}

//TEST(Encoding, DecimalValue2222) {
//        std::string buf;
//        datatype::MyDecimal dec_src;
//        //"12.34"
//        //Decimal(4,2) = 12.34
//
//        int32_t err_t = dec_src.FromString("12.34");
//        std::cerr << err_t << std::endl;
//        if ( err_t == 0 ) {
//            std::cerr << "GetDigitsInt:" << static_cast<int32_t>(dec_src.GetDigitsInt()) << std::endl
//                    << "GetDigitsFrac:" << static_cast<int32_t>(dec_src.GetDigitsFrac()) << std::endl
//                    << "GetResultFrac:" << static_cast<int32_t>(dec_src.GetResultFrac()) << std::endl
//                    << "GetNegative:" << dec_src.GetNegative() << std::endl
//                    << "ToString:" << dec_src.ToString() << std::endl
//                    << "String:" << dec_src.String() << std::endl ;
//
//        }
//        EncodeDecimalValue(&buf, 10000, &dec_src);
//
//        std::cerr << "hex buf:" << toHex(buf) << std::endl;
//
//        size_t offset = 0;
//
//        datatype::MyDecimal dec_dst;
//        offset = 0;
//        ASSERT_TRUE(DecodeDecimalValue(buf, offset, &dec_dst));
//        ASSERT_EQ(offset, buf.size());
//        ASSERT_TRUE(dec_src.Compare(dec_dst) == 0 );
//
//        EncodeType type;
//        offset = 0;
//        uint32_t actual_col_id = 0;
//        ASSERT_TRUE(DecodeValueTag(buf, offset, &actual_col_id, &type));
//        ASSERT_LE(offset, buf.size());
//        ASSERT_EQ(type, EncodeType::Decimal);
//        ASSERT_EQ(actual_col_id, 10000);
//}

// end namespace
}

