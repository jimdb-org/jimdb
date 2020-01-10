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

#include "common/data_type/my_timestamp.h"
#include <string>
#include <vector>
#include <iostream>
#include <sstream>
#include <random>

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jimkv;
using namespace jimkv::datatype;


TEST(MyDateTime, TestMyDateTimeToString) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t y = func_data(1000, 9999);
        uint32_t mo = func_data(0, 12);
        uint32_t d = func_data(0, 31);
        uint32_t h = func_data(0, 23);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = func_data(0, 100000) % 2 == 0;

        datatype::MyDateTime dt;
        dt.SetYear(y);
        dt.SetMonth(mo);
        dt.SetDay(d);
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        {
            std::ostringstream ost;
            ost << (neg ? "-" : "")
                << std::setfill('0') << std::setw(4)
                << y << "-" << std::setw(2)
                << mo << "-" << std::setw(2)
                << d << " " << std::setw(2)
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
                << ", y: " << y
                << ", mo: " << mo
                << ", d: " << d
                << ", h: " << h
                << ", mi: " << mi
                << ", s: " << s
                << ", ms: " << ms
                << ", MyDateTime.GetYear(): " << dt.GetYear()
                << ", MyDateTime.GetMonth(): " << dt.GetMonth()
                << ", MyDateTime.GetDay(): " << dt.GetDay()
                << ", MyDateTime.GetHour(): " << dt.GetHour()
                << ", MyDateTime.GetMinute(): " << dt.GetMinute()
                << ", MyDateTime.GetSecond(): " << dt.GetSecond()
                << ", MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond()
                << std::endl;
    }
}

TEST(MyDateTime, TestMyDateTimeFromString) {

    typedef struct {
        std::string input;
        std::string output;
    } UseCase;

    UseCase cases[] = {
            {"2019-11-05 11:55:25", "2019-11-05 11:55:25.000000"},
            {"0000-00-00 00:00:00", "0000-00-00 00:00:00.000000"},
            {"0001-01-01 00:00:00", "0001-01-01 00:00:00.000000"},
            {"00-11-05 11:55:25", "2000-11-05 11:55:25.000000"},
            {"12-11-05 11:55:25", "2012-11-05 11:55:25.000000"},
            {"2019-11-05", "2019-11-05 00:00:00.000000"},
            {"20191105", "2019-11-05 00:00:00.000000"},
            {"191105", "2019-11-05 00:00:00.000000"},
            {"2019^11^05 11+55+25", "2019-11-05 11:55:25.000000"},
            {"2019^11^05T11+55+25", "2019-11-05 11:55:25.000000"},
            {"2019-2-1 11:55:25", "2019-02-01 11:55:25.000000"},
            {"19-2-1 11:55:25.000000", "2019-02-01 11:55:25.000000"},
            {"20191105115525", "2019-11-05 11:55:25.000000"},
            {"20191105.115525", "2019-11-05 11:55:25.000000"},
            {"191105115525", "2019-11-05 11:55:25.000000"},
            {"191105.115525", "2019-11-05 11:55:25.000000"},
            {"2012-02-29", "2012-02-29 00:00:00.000000"},
            {"00-00-00", "0000-00-00 00:00:00.000000"},
            {"00-00-00 00:00:00.123", "2000-00-00 00:00:00.123000"},
            {"00-00-00 00:00:00.000123", "2000-00-00 00:00:00.000123"},
            {"11111111111", "2011-11-11 11:11:01.000000"},
            {"1901020301.", "2019-01-02 03:01:00.000000"},
            {"1901020304.1", "2019-01-02 03:04:01.000000"},
            {"1701020302.11", "2017-01-02 03:02:11.000000"},
            {"170102036", "2017-01-02 03:06:00.000000"},
            {"170102039.", "2017-01-02 03:09:00.000000"},
            {"170102037.11", "2017-01-02 03:07:11.000000"},
            {"2018-01-01 18", "2018-01-01 18:00:00.000000"},
            {"18-01-01 18", "2018-01-01 18:00:00.000000"},
            {"2018.01.01", "2018-01-01 00:00:00.000000"},
            {"2018.01.01 00:00:00", "2018-01-01 00:00:00.000000"},
            {"2018/01/01-00:00:00", "2018-01-01 00:00:00.000000"},
            {"2018/01/01T00:00:00", "2018-01-01 00:00:00.000000"},
            {"2056/01/01T00:00:00", "2056-01-01 00:00:00.000000"},
    };

    for ( const auto & cs : cases ) {
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;
        ASSERT_TRUE(dt.FromString(cs.input, st))
            << "input: "<< cs.input;
        std::string str_expect = dt.ToString();

        ASSERT_TRUE( str_expect == cs.output)
            << "input: " << cs.input << std::endl
            << ", output: " << cs.output << std::endl
            << ", MyDateTime.ToString(): " << str_expect << std::endl
            << ", MyDateTime.GetYear(): " << dt.GetYear() << std::endl
            << ", MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
            << ", MyDateTime.GetDay(): " << dt.GetDay() << std::endl
            << ", MyDateTime.GetHour(): " << dt.GetHour() << std::endl
            << ", MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
            << ", MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
            << ", MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
            << ", MyDateTime.GetNeg(): " << dt.GetNeg() << std::endl;
    }

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t y = func_data(1000, 9999);
        uint32_t mo = func_data(0, 12);
        uint32_t d = func_data(0, 31);
        uint32_t h = func_data(0, 23);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = func_data(0, 100000) % 2 == 0;

        datatype::MyDateTime dt;
        dt.SetYear(y);
        dt.SetMonth(mo);
        dt.SetDay(d);
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
//        dt.SetNeg(neg);

        std::string str_input;
        {
            std::ostringstream ost;
            ost << std::setfill('0') << std::setw(4)
                << y << "-" << std::setw(2)
                << mo << "-" << std::setw(2)
                << d << " " << std::setw(2)
                << h << ":" << std::setw(2)
                << mi << ":" << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            str_input = ost.str();
        }

        std::string str_my = dt.ToString();

        datatype::MyDateTime dt_2;
        datatype::MyDateTime::StWarn st;

        ASSERT_TRUE(dt_2.FromString( str_input, st))
                << "input: "<< str_input;

        ASSERT_TRUE( (dt.GetYear() == dt_2.GetYear()) && ( dt.GetMonth() == dt_2.GetMonth())
                    && (dt.GetDay() == dt_2.GetDay()) && ( dt.GetHour() == dt_2.GetHour())
                    && (dt.GetMinute() == dt_2.GetMinute()) && (dt.GetSecond() == dt_2.GetSecond())
                    && (dt.GetMicroSecond() == dt_2.GetMicroSecond()))
                << "i: " << i << std::endl
                << "str_my: " << str_my << std::endl
                << "str_input: " << str_input << std::endl
                << "y: " << y << std::endl
                << "mo: " << mo << std::endl
                << "d: " << d << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "MyDateTime.GetYear(): " << dt.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << "MyDateTime.GetYear(): " << dt_2.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt_2.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt_2.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt_2.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt_2.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt_2.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt_2.GetMicroSecond() << std::endl
                << std::endl;
    }

}

TEST(MyDateTime, TestMyDateTimeToNumberUint64){

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t y = func_data(1000, 9999);
        uint32_t mo = func_data(0, 12);
        uint32_t d = func_data(0, 31);
        uint32_t h = func_data(0, 23);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);

        datatype::MyDateTime dt;
        dt.SetYear(y);
        dt.SetMonth(mo);
        dt.SetDay(d);
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);

        std::string str_expect;
        uint64_t expect_uint;
        {
            std::ostringstream ost;
            ost << std::setfill('0') << std::setw(4)
                << y << std::setw(2)
                << mo << std::setw(2)
                << d << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s ;

            std::istringstream ist(ost.str());
            ist >> expect_uint;
        }

        uint64_t my_uint = dt.ToNumberUint64();

//            std::cerr << my_uint << "\t" << expect_uint << std::endl;

        ASSERT_TRUE( my_uint == expect_uint)
                << "i: " << i
                << ", my_uint: " << my_uint
                << ", expect_uint: " << expect_uint
                << ", y: " << y
                << ", mo: " << mo
                << ", d: " << d
                << ", h: " << h
                << ", mi: " << mi
                << ", s: " << s
                << ", ms: " << ms
                << ", MyDateTime.GetYear(): " << dt.GetYear()
                << ", MyDateTime.GetMonth(): " << dt.GetMonth()
                << ", MyDateTime.GetDay(): " << dt.GetDay()
                << ", MyDateTime.GetHour(): " << dt.GetHour()
                << ", MyDateTime.GetMinute(): " << dt.GetMinute()
                << ", MyDateTime.GetSecond(): " << dt.GetSecond()
                << ", MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond()
                << std::endl;
    }
}

TEST(MyDateTime, TestMyDateTimeFromNumberUint64){

    typedef struct {
        uint64_t input;
        bool ret;
        std::string expect;
        datatype::MyDateTime::StWarn st;
    } UseCases;


    UseCases cases [] = {
            {20191111111111, true, "2019-11-11 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111111111, true, "0201-11-11 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201111111111, true, "2020-11-11 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111111111, true, "2002-01-11 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111111, true, "2000-20-11 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201111111, true, "2000-02-01 11:11:11.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111111, true, "2011-11-11 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED},
            {201111, true, "2020-11-11 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111, true, "2002-01-11 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011, true, "2000-20-11 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201, true, "2000-02-01 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {2, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {0, true, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},

            {99999999999999, true, "9999-99-99 99:99:99.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {100000000000000, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_OUT_OF_RANGE },
            {10000102000000, true, "1000-01-02 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {19690101000000, true, "1969-01-01 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {991231235959, true, "1999-12-31 23:59:59.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {691231235959, true, "2069-12-31 23:59:59.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {370119031407, true, "2037-01-19 03:14:07.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {380120031407, true, "2038-01-20 03:14:07.000000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {11111111111, true, "2001-11-11 11:11:11.000000",  datatype::MyDateTime::MYDATETIME_WARN_NONE},
    };

    for ( const auto & cs : cases ) {
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        ASSERT_TRUE(dt.FromNumberUint64(cs.input, st) == cs.ret)
                << "input: "<< cs.input;
        ASSERT_TRUE(cs.st == st)
                << "input:" << cs.input << std::endl
                << "cs.st:" << cs.st << std::endl
                << "ret.st:" << st;

        std::string str_expect = dt.ToString();

        ASSERT_TRUE( str_expect == cs.expect)
                << "input: " << cs.input << std::endl
                << ", expect: " << cs.expect << std::endl
                << ", MyDateTime.ToString(): " << str_expect << std::endl
                << ", MyDateTime.GetYear(): " << dt.GetYear() << std::endl
                << ", MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
                << ", MyDateTime.GetDay(): " << dt.GetDay() << std::endl
                << ", MyDateTime.GetHour(): " << dt.GetHour() << std::endl
                << ", MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
                << ", MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
                << ", MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << ", MyDateTime.GetNeg(): " << dt.GetNeg() << std::endl;
    }
}

TEST(MyDateTime, TestMyDateTimeToFloat64) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t y = func_data(1000, 9999);
        uint32_t mo = func_data(0, 12);
        uint32_t d = func_data(0, 31);
        uint32_t h = func_data(0, 23);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = func_data(0, 100000) % 2 == 0;

        datatype::MyDateTime dt;
        dt.SetYear(y);
        dt.SetMonth(mo);
        dt.SetDay(d);
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        double d_expect = 0.0;
        {
            std::ostringstream ost;
            ost << (neg ? '-' : ' ')
                << std::setfill('0') << std::setw(4)
                << y << std::setw(2)
                << mo << std::setw(2)
                << d << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            std::string str_tmp = ost.str();
            d_expect = std::stod(str_tmp);
        }

        double d_my = dt.ToNumberFloat64();

        //std::cerr  << std::setprecision(10) << "std::fabs((" << d_expect << ") - (" << d_my << "))="  << std::fabs(d_expect - d_my) <<  std::endl;

        ASSERT_TRUE( std::fabs(d_my - d_expect ) < 0.0000001)
                << "i: " << i << std::endl
                << "my_d: " << d_my << std::endl
                << "d_expect: " << d_expect << std::endl
                << "y: " << y << std::endl
                << "mo: " << mo << std::endl
                << "d: " << d << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "MyDateTime.GetYear(): " << dt.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << std::endl;
    }
}

TEST(MyDateTime, TestMyDateTimeFromNumberFloat64){

    typedef struct {
        double input;
        bool ret;
        std::string expect;
        datatype::MyDateTime::StWarn st;
    } UseCases;


    UseCases cases [] = {
            {20191111111111.9, true, "2019-11-11 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111111111.9, true, "0201-11-11 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201111111111.9, true, "2020-11-11 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111111111.9, true, "2002-01-11 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111111.9, true, "2000-20-11 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201111111.9, true, "2000-02-01 11:11:11.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111111.9, true, "2011-11-11 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011111.9, false, "0000-00-00 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED},
            {201111.9, true, "2020-11-11 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20111.9, true, "2002-01-11 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2011.9, true, "2000-20-11 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201.9, true, "2000-02-01 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20.9, false, "0000-00-00 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {2.9, false, "0000-00-00 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {0.9, true, "0000-00-00 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},

            {99999999999999.9, true, "9999-99-99 99:99:99.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {100000000000000.9, false, "0000-00-00 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_OUT_OF_RANGE },
            {10000102000000.9, true, "1000-01-02 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {19690101000000.9, true, "1969-01-01 00:00:00.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {991231235959.9, true, "1999-12-31 23:59:59.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {691231235959.9, true, "2069-12-31 23:59:59.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {370119031407.9, true, "2037-01-19 03:14:07.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {380120031407.9, true, "2038-01-20 03:14:07.900000", datatype::MyDateTime::MYDATETIME_WARN_NONE},
            {11111111111.9, true, "2001-11-11 11:11:11.900000",  datatype::MyDateTime::MYDATETIME_WARN_NONE},

            {2019111111111.8, true, "0201-91-11 11:11:11.800000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201911111111.11, true, "2020-19-11 11:11:11.110000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20191111111.111, true, "2002-01-91 11:11:11.111000", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2019111111.1111, true, "2000-20-19 11:11:11.111100", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201911111.11111, true, "2000-02-01 91:11:11.111110", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20191111.111111, true, "2019-11-11 00:00:00.111111", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2019111.1111111, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED},
            {201911.11111111, true, "2020-19-11 00:00:00.111111", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20191.111111111, true, "2002-01-91 00:00:00.111111", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {2019.1111111111, true, "2000-20-19 00:00:00.111111", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {201.91111111111, true, "2000-02-01 00:00:00.911111", datatype::MyDateTime::MYDATETIME_WARN_NONE },
            {20.191111111111, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {2.0191111111111, false, "0000-00-00 00:00:00.000000", datatype::MyDateTime::MYDATETIME_WARN_TRUNCATED },
            {.20191111111111, true, "0000-00-00 00:00:00.201911", datatype::MyDateTime::MYDATETIME_WARN_NONE },
    };

    for ( const auto & cs : cases ) {
        datatype::MyDateTime dt;
        datatype::MyDateTime::StWarn st;

        ASSERT_TRUE(dt.FromNumberFloat64(cs.input, st) == cs.ret)
                << "input: "<< cs.input;
        ASSERT_TRUE(cs.st == st)
                << "input:" << cs.input << std::endl
                << "cs.st:" << cs.st << std::endl
                << "ret.st:" << st;


        datatype::MyDateTime dt2;
        datatype::MyDateTime::StWarn st2;

        ASSERT_TRUE(dt2.FromString(cs.expect, st2) == true)
                << "input: "<< cs.expect;
        ASSERT_TRUE( datatype::MyDateTime::StWarn::MYDATETIME_WARN_NONE == st2)
                << "input:" << cs.expect<< std::endl
                << "st2:" << st2 << std::endl;

        ASSERT_TRUE( (dt.GetYear() == dt2.GetYear())
                && ( dt.GetMonth() == dt2.GetMonth() )
                && ( dt.GetDay() == dt2.GetDay())
                && ( dt.GetHour() == dt2.GetHour())
                && ( dt.GetMinute() == dt2.GetMinute())
                && ( dt.GetSecond() == dt2.GetSecond()))
                << "input: " << cs.input << std::endl
                << "expect: " << cs.expect << std::endl
                << "MyDateTime.ToString(): " << dt.ToString() << std::endl
                << "MyDateTime.GetYear(): " << dt.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << "MyDateTime.GetNeg(): " << dt.GetNeg() << std::endl
                << "MyDateTime.ToString(): " << dt2.ToString() << std::endl
                << "MyDateTime.GetYear(): " << dt2.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt2.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt2.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt2.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt2.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt2.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt2.GetMicroSecond() << std::endl
                << "MyDateTime.GetNeg(): " << dt2.GetNeg() << std::endl;
    }

}

TEST(MyDateTime, TestMyDateTimeToNumberString) {

    auto func_data = [](const int32_t begin, const int32_t end) -> int32_t {
        std::random_device r;
        std::default_random_engine e1(r());
        std::uniform_int_distribution<int32_t> uniform_dist(begin, end);
        int mean = uniform_dist(e1);
        return mean;
    };

    for (auto i = 0; i < 10000; i++) {
        uint32_t y = func_data(1000, 9999);
        uint32_t mo = func_data(0, 12);
        uint32_t d = func_data(0, 31);
        uint32_t h = func_data(0, 23);
        uint32_t mi = func_data(0, 59);
        uint32_t s = func_data(0, 59);
        uint32_t ms = func_data(0, 999999);
        bool neg = func_data(0, 100000) % 2 == 0;

        datatype::MyDateTime dt;
        dt.SetYear(y);
        dt.SetMonth(mo);
        dt.SetDay(d);
        dt.SetHour(h);
        dt.SetMinute(mi);
        dt.SetSecond(s);
        dt.SetMicroSecond(ms);
        dt.SetNeg(neg);

        std::string str_expect;
        {
            std::ostringstream ost;
            ost << (neg ? '-' : ' ')
                << std::setfill('0') << std::setw(4)
                << y << std::setw(2)
                << mo << std::setw(2)
                << d << std::setw(2)
                << h << std::setw(2)
                << mi << std::setw(2)
                << s << "." << std::setw(6)
                << ms;

            str_expect = ost.str();
        }

        std::string str_my = dt.ToNumberString();

        //std::cerr  << std::setprecision(10) << "expect:" << str_expect << ", my:"<< str_my << std::endl;

        ASSERT_TRUE( str_expect == str_my)
                << "i: " << i << std::endl
                << "str_my: " << str_my << std::endl
                << "str_expect: " << str_expect << std::endl
                << "y: " << y << std::endl
                << "mo: " << mo << std::endl
                << "d: " << d << std::endl
                << "h: " << h << std::endl
                << "mi: " << mi << std::endl
                << "s: " << s << std::endl
                << "ms: " << ms << std::endl
                << "MyDateTime.GetYear(): " << dt.GetYear() << std::endl
                << "MyDateTime.GetMonth(): " << dt.GetMonth() << std::endl
                << "MyDateTime.GetDay(): " << dt.GetDay() << std::endl
                << "MyDateTime.GetHour(): " << dt.GetHour() << std::endl
                << "MyDateTime.GetMinute(): " << dt.GetMinute() << std::endl
                << "MyDateTime.GetSecond(): " << dt.GetSecond() << std::endl
                << "MyDateTime.GetMicroSecond(): " << dt.GetMicroSecond() << std::endl
                << std::endl;
    }
}

} /* namespace  */
