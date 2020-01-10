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

#include <gtest/gtest.h>

#include "common/data_type/my_time.h"
#include <string>
#include <vector>
#include <random>

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::datatype;

TEST(MyTime, TestMyTimeToString) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t h = func_data(0, 838);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = func_data(0, 100000) % 2 == 0;

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        {
            std::ostringstream ost;
            ost << (neg ? "-" : "")
                << std::setfill('0') << std::setw(2)
                << h << ":" << std::setw(2)
                << mi << ":" << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            str_expect = ost.str();
        }

        std::string str_my = dt.ToString();

        ASSERT_TRUE(str_my == str_expect)
                << "i: " << i
                << ", str_my: " << str_my
                << ", str_expect: " << str_expect
                << ", h: " << h
                << ", mi: " << mi
                << ", s: " << s
                << ", ms: " << ms
                << ", neg: " << neg
                << ", MyTime.GetHour(): " << dt.GetHour()
                << ", MyTime.GetMinute(): " << dt.GetMinute()
                << ", MyTime.GetSecond(): " << dt.GetSecond()
                << ", MyTime.GetMicroSecond(): " << dt.GetMicroSecond()
                << ", MyTime.GetNeg(): " << dt.GetNeg()
                << std::endl;
    }

}

TEST(MyTime, TestMyTimeFromString) {

    typedef struct {
        std::string input;
        std::string output;
        bool ret;
        datatype::MyTime::StWarn st;
    } UseCase;

    UseCase cases[] = {

            {"", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {" ", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {".", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {":", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"#", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"  ", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"..", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"::", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"##", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"--", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"AA", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"55", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {" .", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {" :", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {" #", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {" -", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {" A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {" 5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {". ", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {": ", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"# ", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"- ", "00:00:00.000000", false, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"A ", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5 ", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"#:", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-:", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"A:", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5:", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {":#", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {":-", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {":A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {":5", "00:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {".#", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {".-", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {".A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {".5", "00:00:00.500000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"#.", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-.", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"A.", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5.", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"#-", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"#A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"#5", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"-#", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"A#", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5#", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"-A", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-5", "-00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"A-", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5-", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"A5", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5A", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},



            {"555", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {" 55", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5 5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55 ", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {" 5 ", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"55.", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5.5", "00:00:05.500000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {".55", "00:00:00.550000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {".5.", "00:00:00.500000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"#55", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5#5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"55#", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"#5#", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"-55", "-00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5-5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"55-", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-5-", "-00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"A00", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5A5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"55A", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"A5A", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {":55", "00:55:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5:5", "05:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55:", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {":5:", "00:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},


            {"5555", "00:55:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"555.", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55.5", "00:00:55.500000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5.55", "00:00:05.550000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {".555", "00:00:00.555000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"555-", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"55-5", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5-55", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-555", "-00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"555:", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55:5", "55:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5:55", "05:55:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {":555", "00:555:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"555 ", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55 5", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5 55", "175:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {" 555", "00:05:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"55::", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5:5:", "05:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {":5:5", "00:05:05.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"::55", "00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5::5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"55--", "00:00:55.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5-5-", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-5-5", "-00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"--55", "-00:00:00.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5--5", "00:00:05.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"55555", "05:55:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5555.", "00:55:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"555.5", "00:05:55.500000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55.55", "00:00:55.550000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5.555", "00:00:05.555000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {".5555", "00:00:00.555500", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"5555:", "00:55:55.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"555:5", "555:05:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"55:55", "55:55:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5:555", "05:555:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {":5555", "00:5555:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},

            {"5 123", "243:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5 12:34:12:34", "132:34:12.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5 12.34.12:34", "132:00:00.340000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"5 12:34:12.34", "132:34:12.340000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"5 12:34:12.3443333", "132:34:12.344333", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"-5 123", "-243:00:00.000000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"-5 12:34:12:34", "-132:34:12.000000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-5 12.34.12:34", "-132:00:00.340000", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},
            {"-5 12:34:12.34", "-132:34:12.340000", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"-5 12:34:12.0034", "-132:34:12.003400", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"-5 12:34:12.3443333", "-132:34:12.344333", true, datatype::MyTime::MYTIME_WARN_TRUNCATED},

            {"-2019-11-05 12:34:12.3443333", "-12:34:12.344333", true, datatype::MyTime::MYTIME_WARN_NONE},
            {"2019-11-05 12:34:12.3443333", "12:34:12.344333", true, datatype::MyTime::MYTIME_WARN_NONE},

    };

    for ( const auto & cs : cases ) {
        datatype::MyTime dt;
        datatype::MyTime::StWarn st;

        ASSERT_TRUE(dt.FromString(cs.input, st) == cs.ret && st == cs.st)
                << "cs.input: ("<< cs.input << ")" << std::endl
                << "cs.ret: (" << cs.ret << ")" << std::endl
                << "cs.st: (" << cs.st << ")" << std::endl
                << "st: (" << st << ")" << std::endl;

        std::string str_expect = dt.ToString();

        ASSERT_TRUE( str_expect == cs.output)
                << "input: " << cs.input << std::endl
                << ", output: " << cs.output << std::endl
                << ", MyTime.ToString(): " << str_expect << std::endl
                << ", MyTime.GetHour(): " << dt.GetHour() << std::endl
                << ", MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << ", MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << ", MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << ", MyTime.GetNeg(): " << dt.GetNeg() << std::endl;
    }
}

TEST(MyTime, TestMyTimeToNumberInt64) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t h = func_data(0, 838);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = (func_data(0, 100000)%2 == 0);

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        int64_t expect_int;
        {
            std::ostringstream ost;
            ost << (neg ? '-':' ')
                << std::setfill('0') << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s ;

            std::istringstream ist(ost.str());
            ist >> expect_int;
        }

        int64_t my_int = dt.ToNumberInt64();

        //std::cerr << my_uint << "\t" << expect_uint << std::endl;

        ASSERT_TRUE( my_int == expect_int)
                << "i: " << i << std::endl
                << "my_int: " << my_int << std::endl
                << "expect_int: " << expect_int << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "MyTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << std::endl;
    }
}

TEST(MyTime, TestMyTimeFromNumberInt64 ) {

    typedef struct {
        uint64_t input;
        bool ret;
        std::string expect;
        datatype::MyTime::StWarn st;
    } UseCases;

    UseCases cases [] = {

            {20191111111111, true, "11:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011111111111, true, "11:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {201111111111, true, "11:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {20111111111, true, "11:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011111111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {201111111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {20111111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {2011111, true, "201:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {201111, true, "20:11:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {20111, true, "02:01:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011, true, "00:20:11.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {201, true, "00:02:01.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {20, true, "00:00:20.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {2, true, "00:00:02.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {0, true, "00:00:00.000000", datatype::MyTime::MYTIME_WARN_NONE},


            {99999999999999, true, "99:99:99.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {100000000000000, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {10000102000000, true, "00:00:00.000000", datatype::MyTime::MYTIME_WARN_NONE },
            {19690101000000, true, "00:00:00.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {991231235959, true, "23:59:59.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {691231235959, true, "23:59:59.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {370119031407, true, "03:14:07.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {380120031407, true, "03:14:07.000000", datatype::MyTime::MYTIME_WARN_NONE},
            {11111111111, true, "11:11:11.000000",  datatype::MyTime::MYTIME_WARN_NONE},
    };

    for ( const auto & cs : cases ) {
        datatype::MyTime dt;
        datatype::MyTime::StWarn st;

        ASSERT_TRUE(dt.FromNumberInt64(cs.input, st) == cs.ret)
                << "input: "<< cs.input;
        ASSERT_TRUE(cs.st == st)
                << "input:" << cs.input << std::endl
                << "cs.st:" << cs.st << std::endl
                << "ret.st:" << st;

        std::string str_expect = dt.ToString();

        ASSERT_TRUE( str_expect == cs.expect)
                << "input: " << cs.input << std::endl
                << ", expect: " << cs.expect << std::endl
                << ", MyTime.ToString(): " << str_expect << std::endl
                << ", MyTime.GetHour(): " << dt.GetHour() << std::endl
                << ", MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << ", MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << ", MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << ", MyTime.GetNeg(): " << dt.GetNeg() << std::endl;
    }

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t h = func_data(0, 838);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = (func_data(0, 10000)%2 == 0);

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        int64_t tmp_int;
        {
            std::ostringstream ost;
            ost << (neg ? '-':' ')
                << std::setfill('0') << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s ;

            std::istringstream ist(ost.str());
            ist >> tmp_int;
        }

        int64_t my_int = dt.ToNumberInt64();

        //std::cerr << my_uint << "\t" << expect_uint << std::endl;
        datatype::MyTime t;
        datatype::MyTime::StWarn st;

        ASSERT_TRUE(t.FromNumberInt64(tmp_int, st) == true);

        ASSERT_TRUE( datatype::MyTime::MYTIME_WARN_NONE == st)
                << "tmp_int:" << tmp_int << std::endl
                << "st:" << st;

        int64_t my_int2 = t.ToNumberInt64();

        ASSERT_TRUE( (dt.GetHour() == t.GetHour()) && (dt.GetMinute() == t.GetMinute()) && (dt.GetSecond() == t.GetSecond()) && (dt.GetNeg() == t.GetNeg()) )
                << "i: " << i << std::endl
                << ", tmp_int: " << tmp_int << std::endl
                << ", my_int: " << my_int << std::endl
                << ", my_int2: " << my_int2 << std::endl
                << ", h: " << h << std::endl
                << ", mi: " << mi << std::endl
                << ", s: " << s << std::endl
                << ", ms: " << ms << std::endl
                << ", MyTime.GetHour(): " << dt.GetHour() << std::endl
                << ", MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << ", MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << ", MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << ", MyTime.GetNeg(): " << t.GetNeg() << std::endl
                << ", MyTime.GetHour(): " << t.GetHour() << std::endl
                << ", MyTime.GetMinute(): " << t.GetMinute() << std::endl
                << ", MyTime.GetSecond(): " << t.GetSecond() << std::endl
                << ", MyTime.GetMicroSecond(): " << t.GetMicroSecond() << std::endl
                << ", MyTime.GetNeg(): " << t.GetNeg() << std::endl
                << std::endl;
    }
}

TEST(MyTime, TestMyTimeToNumberFloat64) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t h = func_data(0, 838);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = (func_data(0, 100000)%2 == 0);

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        double d_expect = 0.0;
        {
            std::ostringstream ost;
            ost << (neg ? '-':' ')
                << std::setfill('0') << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            std::string str_tmp = ost.str();
            d_expect = std::stod(str_tmp);
        }

        double d_my = dt.ToNumberFloat64();

        //std::cerr  << std::setprecision(10) << "std::fabs((" << d_expect << ") - (" << d_my << "))="  << std::fabs(d_expect - d_my) <<  std::endl;

        ASSERT_TRUE( std::fabs(d_my - d_expect) < 0.000001)
                << "i: " << i << std::endl
                << "d_my: " << d_my << std::endl
                << "d_expect: " << d_expect << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "neg:" << std::boolalpha << neg << std::noboolalpha << std::endl
                << "MyTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << std::endl;
    }
}

TEST(MyTime, TestMyTimeFromNumberFloat64 ) {

    typedef struct {
        double input;
        bool ret;
        std::string expect;
        datatype::MyTime::StWarn st;
    } UseCases;

    UseCases cases [] = {

            {20191111111111.9, true, "11:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011111111111.9, true, "11:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {201111111111.9, true, "11:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {20111111111.9, true, "11:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011111111.9, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {201111111.9, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {20111111.9, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {2011111.9, true, "201:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {201111.9, true, "20:11:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {20111.9, true, "02:01:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {2011.9, true, "00:20:11.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {201.9, true, "00:02:01.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {20.0, true, "00:00:20.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {2.0, true, "00:00:02.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {0.9, true, "00:00:00.900000", datatype::MyTime::MYTIME_WARN_NONE},


            {99999999999999.9, true, "99:99:99.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {100000000000000.9, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE},
            {10000102000000.9, true, "00:00:00.900000", datatype::MyTime::MYTIME_WARN_NONE },
            {19690101000000.9, true, "00:00:00.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {991231235959.9, true, "23:59:59.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {691231235959.9, true, "23:59:59.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {370119031407.9, true, "03:14:07.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {380120031407.9, true, "03:14:07.900000", datatype::MyTime::MYTIME_WARN_NONE},
            {11111111111.9, true, "11:11:11.900000",  datatype::MyTime::MYTIME_WARN_NONE},

            {2019111111111.1, true, "11:11:11.100000", datatype::MyTime::MYTIME_WARN_NONE },
            {201911111111.11, true, "11:11:11.110000", datatype::MyTime::MYTIME_WARN_NONE },
            {20191111111.111, true, "11:11:11.111000", datatype::MyTime::MYTIME_WARN_NONE },
            {2019111111.1111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE },
            {201911111.11111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE },
            {20191111.111111, true, "838:59:59.000000", datatype::MyTime::MYTIME_WARN_OUT_OF_RANGE },
            {2012111.1111111, true, "201:21:11.111111", datatype::MyTime::MYTIME_WARN_NONE },
            {201911.11111111, true, "20:19:11.111111", datatype::MyTime::MYTIME_WARN_NONE },
            {20121.111111111, true, "02:01:21.111111", datatype::MyTime::MYTIME_WARN_NONE },
            {2019.1111111111, true, "00:20:19.111111", datatype::MyTime::MYTIME_WARN_NONE },
            {201.91111111111, true, "00:02:01.911111", datatype::MyTime::MYTIME_WARN_NONE },
            {20.191111111111, true, "00:00:20.191111", datatype::MyTime::MYTIME_WARN_NONE },
            {2.0191111111111, true, "00:00:02.019111", datatype::MyTime::MYTIME_WARN_NONE },
            {.20191111111111, true, "00:00:00.201911", datatype::MyTime::MYTIME_WARN_NONE },
            {.02019111111111, true, "00:00:00.020191", datatype::MyTime::MYTIME_WARN_NONE },
    };

    for ( const auto & cs : cases ) {
        datatype::MyTime dt;
        datatype::MyTime::StWarn st;

        ASSERT_TRUE(dt.FromNumberFloat64(cs.input, st) == cs.ret)
                << "input: "<< cs.input;
        ASSERT_TRUE(cs.st == st)
                << "input:" << cs.input << std::endl
                << "cs.st:" << cs.st << std::endl
                << "ret.st:" << st;

        datatype::MyTime dt2;
        datatype::MyTime::StWarn st2;
        ASSERT_TRUE(dt2.FromString(cs.expect, st2) == true)
                << "input: "<< cs.input;
        ASSERT_TRUE( datatype::MyTime::StWarn::MYTIME_WARN_NONE == st2)
                << "input:" << cs.input << std::endl
                << "st2:" << st2 << std::endl;

        ASSERT_TRUE( (dt.GetHour() == dt2.GetHour()) && ( dt.GetMinute() == dt2.GetMinute()) && (dt.GetSecond() == dt2.GetSecond()) && (dt.GetNeg() == dt2.GetNeg()))
                << "input: " << cs.input << std::endl
                << "expect: " << cs.expect << std::endl
                << "MyTime.ToString(): " << dt.ToString() << std::endl
                << "MyTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << "MyTime.GetNeg(): " << std::boolalpha << dt.GetNeg() << std::noboolalpha << std::endl
                << "MyTime.ToString(): " << dt2.ToString() << std::endl
                << "MyTime.GetHour(): " << dt2.GetHour() << std::endl
                << "MyTime.GetMinute(): " << dt2.GetMinute() << std::endl
                << "MyTime.GetSecond(): " << dt2.GetSecond() << std::endl
                << "MyTime.GetMicroSecond(): " << dt2.GetMicroSecond() << std::endl
                << "MyTime.GetNeg(): " << std::boolalpha << dt2.GetNeg() << std::noboolalpha << std::endl;
    }
}

TEST(MyTime, TestMyTimeToNumberString) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t h = func_data(0, 838);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = (func_data(0, 100000)%2 == 0);

        datatype::MyTime dt;
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        {
            std::ostringstream ost;
            ost << (neg ? '-':' ')
                << std::setfill('0') << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            str_expect = ost.str();
        }

        std::string str_my = dt.ToNumberString();

        //std::cerr << std::setprecision(10) << "expect:" << str_expect << ", my:" << str_my << std::endl;

        ASSERT_TRUE(str_expect == str_my)
                << "i: " << i << std::endl
                << "str_my: " << str_my << std::endl
                << "str_expect: " << str_expect << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "neg:" << std::boolalpha << neg << std::noboolalpha << std::endl
                << "MyTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << std::endl;
    }
}

} /* namespace  */
