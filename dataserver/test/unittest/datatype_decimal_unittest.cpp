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

#include "common/data_type/my_decimal.h"
#include <string>
#include <vector>

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace {

using namespace jim;
using namespace jim::datatype;

TEST(MyDecimal, TestNewDecFromInt) {
    typedef struct {
        int64_t  input;
        std::string output;
    } UseCase;

    UseCase cases[] = {
            {-9223372036854775805, "-9223372036854775805"},
            {-4294967296, "-4294967296"},
            {-2147483648, "-2147483648"},
            {-53241, "-53241"},
            {-1, "-1"},
            {0, "0"},
            {1, "1"},
            {53241, "53241"},
            {2147483648, "2147483648"},
            {4294967296, "4294967296"},
            {9223372036854775805L, "9223372036854775805"},
            {9223372036854775807L, "9223372036854775807"},
    };

    for (const auto & cs : cases) {
        MyDecimal* d = NewDecFromInt(cs.input);
        std::string str = d->ToString();
        EXPECT_TRUE(cs.output == str) << "{"<< cs.input << ",\t"<< cs.output << ", \t" << str << "}";
    }
}

TEST(MyDecimal, TestNewDecFromUint){
    typedef struct {
        uint64_t  input;
        std::string output;
    } UseCase;

    UseCase cases[] = {
            {0, "0"},
            {1, "1"},
            {53241, "53241"},
            {2147483648, "2147483648"},
            {4294967296, "4294967296"},
            {9223372036854775805UL, "9223372036854775805"},
            {9223372036854775808UL, "9223372036854775808"},
            {18446744073709551610UL, "18446744073709551610"},
            {18446744073709551615UL, "18446744073709551615"},
    };

    for (const auto & cs : cases) {
        MyDecimal* d = NewDecFromUint(cs.input);
        std::string str = d->ToString();
        EXPECT_TRUE(cs.output == str) << "{" << cs.input << ",\t" << cs.output << ",\t" << str << "}";
    }
}

TEST(MyDecimal, TestMyDecimalToInt){
    typedef struct {
        std::string input;
        int64_t output;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"0", 0, E_DEC_OK},
            {"1", 1, E_DEC_OK},
            {"2147483648", 2147483648, E_DEC_OK},
            {"4294967296", 4294967296, E_DEC_OK},
            {"-9223372036854775807", -9223372036854775807, E_DEC_OK},
            {"-9223372036854775808", static_cast<int64_t>(-9223372036854775808UL), E_DEC_OK},
            {"9223372036854775807", 9223372036854775807, E_DEC_OK},
            {"18446744073709551610", 9223372036854775807, E_DEC_OVERFLOW},
            {"-9223372036854775809", static_cast<int64_t>(-9223372036854775808UL), E_DEC_OVERFLOW},
            {"1.14", 1, E_DEC_TRUNCATED},
            {"-1.14", -1, E_DEC_TRUNCATED},
    };

    for (const auto & cs : cases) {
        MyDecimal d;
        d.FromString(cs.input);
        int64_t val = 0;
        int32_t error = 0;
        d.ToInt(val, error);
        EXPECT_TRUE((cs.output == val) && (cs.error == error) )
            << "{ ["<< cs.input<< ",\t" << cs.output << ",\t" << cs.error << "] != ["
            << val << ",\t" << error << "]}";
    }
}

TEST(MyDecimal, TestMyDecimalToUint) {
    typedef struct {
        std::string input;
        uint64_t output;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"0", 0, E_DEC_OK},
            {"1", 1, E_DEC_OK},
            {"2147483648", 2147483648, E_DEC_OK},
            {"4294967296", 4294967296, E_DEC_OK},
            {"9223372036854775807", 9223372036854775807, E_DEC_OK},
            {"9223372036854775808", 9223372036854775808UL, E_DEC_OK},
            {"9223372036854775807", 9223372036854775807, E_DEC_OK},
            {"18446744073709551610", 18446744073709551610UL, E_DEC_OK},
            {"18446744073709551615", 18446744073709551615UL, E_DEC_OK},
            {"18446744073709551616", 18446744073709551615UL, E_DEC_OVERFLOW},
            {"1.14", 1, E_DEC_TRUNCATED},
            {"-1.14", 0, E_DEC_OVERFLOW},
            {"1111111111111111111111111.111", 18446744073709551615UL, E_DEC_OVERFLOW},
            {"9999999999999999999999999.999", 18446744073709551615UL, E_DEC_OVERFLOW},
    };

    for (const auto & cs : cases) {
        MyDecimal d;
        d.FromString(cs.input);
        uint64_t val = 0;
        int32_t error = 0;
        d.ToUint(val, error);
        EXPECT_TRUE((cs.output == val) && (cs.error == error) )
            << "{ ["<< cs.input<< ",\t" << cs.output << ",\t" << cs.error << "] != ["
            << val << ",\t" << error << "]}";
    }
}


TEST(MyDecimal, TestNewDecFromFloat) {
    // todo
    ;
}

TEST(MyDecimal, TestNewDecimalToFloat){
    // todo
    ;
}

TEST(MyDecimal, TestMyDecimalToHashKey) {
    typedef struct {
        std::vector<std::string> numbers;
    } UseCase;

    UseCase cases[] = {
            {{"1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000"}},
            {{"-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000"}},
            {{".1", "0.1", "000000.1", ".10000", "0000.10000", "000000000000000000.1"}},
            {{"0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000"}},
            {{".123456789123456789", ".1234567891234567890", ".12345678912345678900", ".123456789123456789000", ".1234567891234567890000", "0.123456789123456789",
                      ".1234567891234567890000000000", "0000000.123456789123456789000"}},
            {{"12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.", "12345.00", "12345.000000000", "000012345.0000"}},
            {{"123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000"}},
            {{"123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000"}},
    };

    for (const auto & cs : cases) {
        std::vector<std::string> keys;
        for (const auto & num : cs.numbers) {
            MyDecimal d;
            int32_t error = d.FromString(num);
            ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString return :" << error ;
            std::string key;
            d.ToHashKey(key, error);
            keys.push_back(key);
        }

        for (auto it=keys.begin(); it < keys.end(); ++it) {
            ASSERT_TRUE(keys.front()==*it);
        }
    }

    typedef struct {
        std::vector<std::string> hashNumbers;
        std::vector<std::string> binNumbers;
    } UseCaseHashBin;

    UseCaseHashBin hash_bin[] = {
            {
                {"1.1", "1.1000", "1.1000000", "1.10000000000", "01.1", "0001.1", "001.1000000"},
                {"1.1", "0001.1", "01.1"}
            },
            {
                {"-1.1", "-1.1000", "-1.1000000", "-1.10000000000", "-01.1", "-0001.1", "-001.1000000"},
                {"-1.1", "-0001.1", "-01.1"}
            },
            {
                {".1", "0.1", "000000.1", ".10000", "0000.10000", "000000000000000000.1"},
                {".1", "0.1", "000000.1", "00.1"}
            },
            {
                {"0", "0000", ".0", ".00000", "00000.00000", "-0", "-0000", "-.0", "-.00000", "-00000.00000"},
                {"0", "0000", "00", "-0", "-00", "-000000"}
            },
            {
                {".123456789123456789", ".1234567891234567890", ".12345678912345678900", ".123456789123456789000", ".1234567891234567890000", "0.123456789123456789", ".1234567891234567890000000000", "0000000.123456789123456789000"},
                {".123456789123456789", "0.123456789123456789", "0000.123456789123456789", "0000000.123456789123456789"}
            },
            {
                {"12345", "012345", "0012345", "0000012345", "0000000012345", "00000000000012345", "12345.", "12345.00", "12345.000000000", "000012345.0000"},
                {"12345", "012345", "000012345", "000000000000012345"}
            },
            {
                {"123E5", "12300000", "00123E5", "000000123E5", "12300000.00000000"},
                {"12300000", "123E5", "00123E5", "0000000000123E5"}
            },
            {
                {"123E-2", "1.23", "00000001.23", "1.2300000000000000", "000000001.23000000000000"},
                {"123E-2", "1.23", "000001.23", "0000000000001.23"}
            },
    };

    for (const auto & ch: hash_bin) {
        std::vector<std::string> keys;

        for (const auto & hash_num : ch.hashNumbers) {
            MyDecimal d;
            int32_t error = d.FromString(hash_num);
            ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString return :" << error ;

            std::string key;
            d.ToHashKey(key, error);
            ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::ToHashKey return :" << error ;

            keys.push_back(key);
        }

        for ( const auto & bin_num : ch.binNumbers) {
            MyDecimal d;
            int32_t error = d.FromString(bin_num);
            ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << bin_num << ") =" << error;

            int32_t prec = 0;
            int32_t frac = 0;
            d.PrecisionAndFrac( prec, frac);

            std::string key;
            d.ToBin(prec, frac, key, error);
            ASSERT_TRUE( (error == E_DEC_OK) || (error == E_DEC_TRUNCATED) ) << "call MyDecimal::ToBin(" << prec << ", " << frac << "," << "key" << "," << error<< "); bin_num: " << bin_num;
            keys.push_back(key);
        }

        for( auto it = keys.begin(); it != keys.end(); ++it) {
            ASSERT_TRUE(keys.front()==*it) << "keys.front()" << ", " << "*it";
        }
    }
}

TEST(MyDecimal, TestMyDecimalRemoveTrailingZeros) {
    typedef std::vector<std::string> UseCase;

    UseCase cases = {
            "0", "0.0", ".0", ".00000000", "0.0000", "0000", "0000.0", "0000.000",
            "-0", "-0.0", "-.0", "-.00000000", "-0.0000", "-0000", "-0000.0", "-0000.000",
            "123123123", "213123.", "21312.000", "21321.123", "213.1230000", "213123.000123000",
            "-123123123", "-213123.", "-21312.000", "-21321.123", "-213.1230000", "-213123.000123000",
            "123E5", "12300E-5", "0.00100E1", "0.001230E-3",
            "123987654321.123456789000", "000000000123", "123456789.987654321", "999.999000",
    };

    for (const auto & cs : cases ) {
        MyDecimal d;
        int32_t error = d.FromString(cs);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs <<") =" << error ;

        int32_t digitsFracExp = 0;
        std::string str = d.ToString();
        size_t pos_point = str.find(".");

        if ( pos_point != std::string::npos) {
            size_t i = str.length()-1;
            while( i > pos_point) {
                if ( str.at(i) != '0') {
                    break;
                }
                i--;
            }

            digitsFracExp = i - pos_point;
        }

        int32_t prec = 0, frac = 0;
        d.RemoveTrailingZeros(prec, frac);
        ASSERT_TRUE(frac == digitsFracExp) << "[" << frac << "!=" << digitsFracExp << "], number:" << cs ;
    }
}

TEST(MyDecimal, TestMyDecimalShift) {
    typedef struct {
        std::string input;
        int32_t shift;
        std::string output;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"123.123",                                           1,    "1231.23",                               E_DEC_OK},
            {"123457189.123123456789000",                         1,    "1234571891.23123456789",                E_DEC_OK},
            {"123457189.123123456789000",                         8,    "12345718912312345.6789",                E_DEC_OK},
            {"123457189.123123456789000",                         9,    "123457189123123456.789",                E_DEC_OK},
            {"123457189.123123456789000",                         10,   "1234571891231234567.89",                E_DEC_OK},
            {"123457189.123123456789000",                         17,   "12345718912312345678900000",            E_DEC_OK},
            {"123457189.123123456789000",                         18,   "123457189123123456789000000",           E_DEC_OK},
            {"123457189.123123456789000",                         19,   "1234571891231234567890000000",          E_DEC_OK},
            {"123457189.123123456789000",                         26,   "12345718912312345678900000000000000",   E_DEC_OK},
            {"123457189.123123456789000",                         27,   "123457189123123456789000000000000000",  E_DEC_OK},
            {"123457189.123123456789000",                         28,   "1234571891231234567890000000000000000", E_DEC_OK},
            {"000000000000000000000000123457189.123123456789000", 26,   "12345718912312345678900000000000000",   E_DEC_OK},
            {"00000000123457189.123123456789000",                 27,   "123457189123123456789000000000000000",  E_DEC_OK},
            {"00000000000000000123457189.123123456789000",        28,   "1234571891231234567890000000000000000", E_DEC_OK},
            {"123",                                               1,    "1230",                                  E_DEC_OK},
            {"123",                                               10,   "1230000000000",                         E_DEC_OK},
            {".123",                                              1,    "1.23",                                  E_DEC_OK},
            {".123",                                              10,   "1230000000",                            E_DEC_OK},
            {".123",                                              14,   "12300000000000",                        E_DEC_OK},
            {"000.000",                                           1000, "0",                                     E_DEC_OK},
            {"000.",                                              1000, "0",                                     E_DEC_OK},
            {".000",                                              1000, "0",                                     E_DEC_OK},
            {"1",                                                 1000, "1",                                     E_DEC_OVERFLOW},
            {"123.123",                                           -1,   "12.3123",                               E_DEC_OK},
            {"123987654321.123456789000",                         -1,   "12398765432.1123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -2,   "1239876543.21123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -3,   "123987654.321123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -8,   "1239.87654321123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -9,   "123.987654321123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -10,  "12.3987654321123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -11,  "1.23987654321123456789",                E_DEC_OK},
            {"123987654321.123456789000",                         -12,  "0.123987654321123456789",               E_DEC_OK},
            {"123987654321.123456789000",                         -13,  "0.0123987654321123456789",              E_DEC_OK},
            {"123987654321.123456789000",                         -14,  "0.00123987654321123456789",             E_DEC_OK},
            {"00000087654321.123456789000",                       -14,  "0.00000087654321123456789",             E_DEC_OK},
    };

//#undef WORD_BUF_LEN
//#define WORD_BUF_LEN  (MAX_WORD_BUF_LEN)

    for (const auto &cs : cases) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input << ") =" << error;

        MyDecimal d_bak(d);

        error = d.Shift(cs.shift);
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::Shift(" << cs.shift << ") =" << cs.error << "; param input = " << cs.input;

        std::string str = d.ToString();
        ASSERT_TRUE(str == cs.output) << "[" << str << "!=" << cs.output << "]; param input = " << cs.input;
    }
/*
    UseCase cases2[] = {
            {"123.123",             -2,  "1.23123",              E_DEC_OK},
            {"123.123",             -3,  "0.123123",             E_DEC_OK},
            {"123.123",             -6,  "0.000123123",          E_DEC_OK},
            {"123.123",             -7,  "0.0000123123",         E_DEC_OK},
            {"123.123",             -15, "0.000000000000123123", E_DEC_OK},
            {"123.123",             -16, "0.000000000000012312", E_DEC_TRUNCATED},
            {"123.123",             -17, "0.000000000000001231", E_DEC_TRUNCATED},
            {"123.123",             -18, "0.000000000000000123", E_DEC_TRUNCATED},
            {"123.123",             -19, "0.000000000000000012", E_DEC_TRUNCATED},
            {"123.123",             -20, "0.000000000000000001", E_DEC_TRUNCATED},
            {"123.123",             -21, "0",                    E_DEC_TRUNCATED},
            {".000000000123",       -1,  "0.0000000000123",      E_DEC_OK},
            {".000000000123",       -6,  "0.000000000000000123", E_DEC_OK},
            {".000000000123",       -7,  "0.000000000000000012", E_DEC_TRUNCATED},
            {".000000000123",       -8,  "0.000000000000000001", E_DEC_TRUNCATED},
            {".000000000123",       -9,  "0",                    E_DEC_TRUNCATED},
            {".000000000123",       1,   "0.00000000123",        E_DEC_OK},
            {".000000000123",       8,   "0.0123",               E_DEC_OK},
            {".000000000123",       9,   "0.123",                E_DEC_OK},
            {".000000000123",       10,  "1.23",                 E_DEC_OK},
            {".000000000123",       17,  "12300000",             E_DEC_OK},
            {".000000000123",       18,  "123000000",            E_DEC_OK},
            {".000000000123",       19,  "1230000000",           E_DEC_OK},
            {".000000000123",       20,  "12300000000",          E_DEC_OK},
            {".000000000123",       21,  "123000000000",         E_DEC_OK},
            {".000000000123",       22,  "1230000000000",        E_DEC_OK},
            {".000000000123",       23,  "12300000000000",       E_DEC_OK},
            {".000000000123",       24,  "123000000000000",      E_DEC_OK},
            {".000000000123",       25,  "1230000000000000",     E_DEC_OK},
            {".000000000123",       26,  "12300000000000000",    E_DEC_OK},
            {".000000000123",       27,  "123000000000000000",   E_DEC_OK},
            {".000000000123",       28,  "0.000000000123",       E_DEC_OVERFLOW},
            {"123456789.987654321", -1,  "12345678.998765432",   E_DEC_TRUNCATED},
            {"123456789.987654321", -2,  "1234567.899876543",    E_DEC_TRUNCATED},
            {"123456789.987654321", -8,  "1.234567900",          E_DEC_TRUNCATED},
            {"123456789.987654321", -9,  "0.123456789987654321", E_DEC_OK},
            {"123456789.987654321", -10, "0.012345678998765432", E_DEC_TRUNCATED},
            {"123456789.987654321", -17, "0.000000001234567900", E_DEC_TRUNCATED},
            {"123456789.987654321", -18, "0.000000000123456790", E_DEC_TRUNCATED},
            {"123456789.987654321", -19, "0.000000000012345679", E_DEC_TRUNCATED},
            {"123456789.987654321", -26, "0.000000000000000001", E_DEC_TRUNCATED},
            {"123456789.987654321", -27, "0",                    E_DEC_TRUNCATED},
            {"123456789.987654321", 1,   "1234567900",           E_DEC_TRUNCATED},
            {"123456789.987654321", 2,   "12345678999",          E_DEC_TRUNCATED},
            {"123456789.987654321", 4,   "1234567899877",        E_DEC_TRUNCATED},
            {"123456789.987654321", 8,   "12345678998765432",    E_DEC_TRUNCATED},
            {"123456789.987654321", 9,   "123456789987654321",   E_DEC_OK},
            {"123456789.987654321", 10,  "123456789.987654321",  E_DEC_OVERFLOW},
            {"123456789.987654321", 0,   "123456789.987654321",  E_DEC_OK},
    };

#undef WORD_BUF_LEN
#define WORD_BUF_LEN  (2)

    for (const auto &cs : cases2) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        EXPECT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input << ") =" << error;

        MyDecimal d_bak(d);

        error = d.Shift(cs.shift);
        EXPECT_TRUE(error == E_DEC_OK) << "call MyDecimal::Shift(" << cs.shift << ") =" << error << "; param input = " << cs.input;

        std::string str = d.ToString();
        EXPECT_TRUE(str == cs.output) << "[" << str << "!=" << cs.output << "]; param input = " << cs.input << ", shiftc =" << cs.shift;
    }
*/
}

TEST(MyDecimal, TestMyDecimalRound) {
    typedef struct {
        std::string input;
        int32_t scale;
        std::string output;
        int32_t error;
    } UseCase;

    UseCase cases_half [] = {
            {"123456789.987654321", 1, "123456790.0", E_DEC_OK},
            {"15.1", 0, "15", E_DEC_OK},
            {"15.5", 0, "16", E_DEC_OK},
            {"15.9", 0, "16", E_DEC_OK},
            {"-15.1", 0, "-15", E_DEC_OK},
            {"-15.5", 0, "-16", E_DEC_OK},
            {"-15.9", 0, "-16", E_DEC_OK},
            {"15.1", 1, "15.1", E_DEC_OK},
            {"-15.1", 1, "-15.1", E_DEC_OK},
            {"15.17", 1, "15.2", E_DEC_OK},
            {"15.4", -1, "20", E_DEC_OK},
            {"-15.4", -1, "-20", E_DEC_OK},
            {"5.4", -1, "10", E_DEC_OK},
            {".999", 0, "1", E_DEC_OK},
            {"999999999", -9, "1000000000", E_DEC_OK}
    };

    for ( const auto &cs : cases_half ) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input <<") =" << error ;

        MyDecimal round;
        d.Round(&round, cs.scale, ModeHalfEven, error );
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::Round(" << &round << ", " << cs.scale << "," << ModeHalfEven << error << "); param input = " << cs.input;
        std::string str = round.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input << "; scale=" << cs.scale;
    }

    UseCase cases_truncate [] = {
            {"123456789.987654321", 1, "123456789.9", E_DEC_OK},
            {"15.1", 0, "15", E_DEC_OK},
            {"15.5", 0, "15", E_DEC_OK},
            {"15.9", 0, "15", E_DEC_OK},
            {"-15.1", 0, "-15", E_DEC_OK},
            {"-15.5", 0, "-15", E_DEC_OK},
            {"-15.9", 0, "-15", E_DEC_OK},
            {"15.1", 1, "15.1", E_DEC_OK},
            {"-15.1", 1, "-15.1", E_DEC_OK},
            {"15.17", 1, "15.1", E_DEC_OK},
            {"15.4", -1, "10", E_DEC_OK},
            {"-15.4", -1, "-10", E_DEC_OK},
            {"5.4", -1, "0", E_DEC_OK},
            {".999", 0, "0", E_DEC_OK},
            {"999999999", -9, "0", E_DEC_OK},
    };

    for ( const auto &cs : cases_truncate ) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input <<") =" << error ;

        MyDecimal round;
        d.Round(&round, cs.scale, ModeTruncate, error );
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::Round(" << &round << ", " << cs.scale << "," << ModeTruncate << error << "); param input = " << cs.input;
        std::string str = round.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input << "; scale=" << cs.scale;
    }

    UseCase cases_ceil [] = {
            {"123456789.987654321", 1, "123456790.0", E_DEC_OK},
            {"15.1", 0, "16", E_DEC_OK},
            {"15.5", 0, "16", E_DEC_OK},
            {"15.9", 0, "16", E_DEC_OK},
            //TODO:fix me
            {"-15.1", 0, "-16", E_DEC_OK},
            {"-15.5", 0, "-16", E_DEC_OK},
            {"-15.9", 0, "-16", E_DEC_OK},
            {"15.1", 1, "15.1", E_DEC_OK},
            {"-15.1", 1, "-15.1", E_DEC_OK},
            {"15.17", 1, "15.2", E_DEC_OK},
            {"15.4", -1, "20", E_DEC_OK},
            {"-15.4", -1, "-20", E_DEC_OK},
            {"5.4", -1, "10", E_DEC_OK},
            {".999", 0, "1", E_DEC_OK},
            {"999999999", -9, "1000000000", E_DEC_OK},
    };

    for ( const auto &cs : cases_ceil ) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input <<") =" << error ;

        MyDecimal round;
        d.Round(&round, cs.scale, ModeCeiling, error );
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::Round(" << &round << ", " << cs.scale << "," << ModeCeiling << error << "); param input = " << cs.input;
        std::string str = round.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input << "; scale=" << cs.scale;
    }
}

TEST(MyDecimal, TestMyDecimalFromString) {
    typedef struct {
        std::string input;
        std::string output;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"12345", "12345", E_DEC_OK},
            {"12345.", "12345", E_DEC_OK},
            {"123.45.", "123.45", E_DEC_OK},
            {"-123.45.", "-123.45", E_DEC_OK},
            {".00012345000098765", "0.00012345000098765", E_DEC_OK},
            {".12345000098765", "0.12345000098765", E_DEC_OK},
            {"-.000000012345000098765", "-0.000000012345000098765", E_DEC_OK},
            {"1234500009876.5", "1234500009876.5", E_DEC_OK},
            {"123E5", "12300000", E_DEC_OK},
            {"123E-2", "1.23", E_DEC_OK},
            {"1e1073741823", "999999999999999999999999999999999999999999999999999999999999999999999999999999999", E_DEC_OVERFLOW},
            {"-1e1073741823", "-999999999999999999999999999999999999999999999999999999999999999999999999999999999", E_DEC_OVERFLOW},
            {"1e18446744073709551620", "0", E_DEC_BAD_NUM},
            {"1e", "1", E_DEC_TRUNCATED},
            {"1e001", "10", E_DEC_OK},
            {"1e00", "1", E_DEC_OK},
            {"1eabc", "1", E_DEC_TRUNCATED},
            {"1e 1dddd ", "10", E_DEC_TRUNCATED},
            {"1e - 1", "1", E_DEC_TRUNCATED},
            {"1e -1", "0.1", E_DEC_OK},
    };

    for (const auto & cs : cases) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::FromString(" << cs.input <<") =" << error << "; param param = " << cs.error;

        std::string str = d.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input ;
    }

    // WORD_BUF_LEN = 1;
    UseCase cases_2 [] = {
            {"123450000098765", "98765", E_DEC_OVERFLOW},
            {"123450.000098765", "123450", E_DEC_TRUNCATED},
    };

    for (const auto & cs : cases) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == cs.error) << "call MyDecimal::FromString(" << cs.input <<") =" << error ;

        std::string str = d.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input ;
    }
}

TEST(MyDecimal, TestDecimalToString) {
    typedef struct {
        std::string input;
        std::string output;
    } UseCase;

    UseCase cases[] = {
            {"123.123", "123.123"},
            {"123.1230", "123.1230"},
            {"00123.123", "123.123"},
    };

    for (const auto & cs : cases ) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input <<") =" << error;

        std::string str = d.ToString();
        ASSERT_TRUE(str==cs.output) << "[" << str << "!=" << cs.output << "]; param input="<<cs.input;
    }
}

TEST(MyDecimal, TestMyDecimalClone) {
    std::vector<std::string> cases = {
            ".0",
            ".123",
            "123.123",
            "123.",
            "123",
            "123.1230",
            "-123.1230",
            "00123.123",
    };

    for (const auto & cs : cases ) {
        MyDecimal d;
        int32_t error = d.FromString(cs);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs<<") =" << error;

        MyDecimal* cl = d.Clone();

        ASSERT_TRUE(cl->GetDigitsInt() == d.GetDigitsInt()) << "[" << cl->GetDigitsInt() << "!=" << d.GetDigitsInt()<< "]";
        ASSERT_TRUE(cl->GetDigitsFrac() == d.GetDigitsFrac()) << "[" << cl->GetDigitsFrac() << "!=" << d.GetDigitsFrac()<< "]";
        ASSERT_TRUE(cl->GetNegative() == d.GetNegative()) << "[" << cl->GetNegative() << "!=" << d.GetNegative()<< "]";

        for ( int i = 0; i < MAX_WORD_BUF_LEN; ++i) {
            ASSERT_TRUE(cl->GetWordDigit(i) == d.GetWordDigit(i)) << "[" << cl->GetWordDigit(i) << "!=" << d.GetWordDigit(i)<< "]";
        }

        ASSERT_TRUE(cl->ToString() == d.ToString()) << "[" << cl->ToString() << "!=" << d.ToString()<< "]";
    }
}

TEST(MyDecimal, TestMyDecimalFromBinToBin) {
    typedef struct {
        std::string input;
        int32_t precision;
        int32_t frac;
        std::string output;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"-10.55", 4, 2, "-10.55", E_DEC_OK},
            {"0.0123456789012345678912345", 30, 25, "0.0123456789012345678912345", E_DEC_OK},
            {"12345", 5, 0, "12345", E_DEC_OK},
            {"12345", 10, 3, "12345.000", E_DEC_OK},
            {"123.45", 10, 3, "123.450", E_DEC_OK},
            {"-123.45", 20, 10, "-123.4500000000", E_DEC_OK},
            {".00012345000098765", 15, 14, "0.00012345000098", E_DEC_TRUNCATED},
            {".00012345000098765", 22, 20, "0.00012345000098765000", E_DEC_OK},
            {".12345000098765", 30, 20, "0.12345000098765000000", E_DEC_OK},
            {"-.000000012345000098765", 30, 20, "-0.00000001234500009876", E_DEC_TRUNCATED},
            {"1234500009876.5", 30, 5, "1234500009876.50000", E_DEC_OK},
            {"111111111.11", 10, 2, "11111111.11", E_DEC_OVERFLOW},
            {"000000000.01", 7, 3, "0.010", E_DEC_OK},
            {"123.4", 10, 2, "123.40", E_DEC_OK},
            {"1000", 3, 0, "0", E_DEC_OVERFLOW},
    };

    for (const auto & cs : cases ) {
        MyDecimal d;
        int32_t error = d.FromString(cs.input);
        EXPECT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.input <<") =" << error;

        std::string bin;
        d.ToBin(cs.precision, cs.frac, bin, error);
        EXPECT_TRUE(error == cs.error) << "call MyDecimal::ToBin(" << cs.precision << ", " << cs.frac << ", " << "bin" << "," << error << "); param input=" << cs.input;
        int32_t bin_size = 0;

        std::string bak_bin(bin);

        MyDecimal d2;

        d2.FromBin(bin, cs.precision, cs.frac, bin_size, error);
        EXPECT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromBin(" << "bin" <<  "," << cs.precision << ", " << cs.frac << ", " << bin_size << "," << error << "); param input=" << cs.input;

        ASSERT_TRUE(bin.size() == bak_bin.size()) << bin.size() << "!=" << bak_bin.size();

        std::string str = d2.ToString();
        EXPECT_TRUE(str == cs.output) << "[" << str << "!=" << cs.output << "]; param input = " << cs.input;
    }

    MyDecimal d;

    d.FromInt(1);

    typedef struct {
        int32_t prec;
        int32_t frac;
    } UseErrCase;

    UseErrCase cases_err [] = {
            {82, 1},
            {-1, 1},
            {10, 31},
            {10, -1},
    };

    std::string bin;
    int32_t bin_size = 0;
    int32_t error = 0;

    for (const auto & cs : cases_err ) {
        d.ToBin(cs.prec, cs.frac, bin, error);
        ASSERT_TRUE(error == E_DEC_BAD_NUM);
    }
}

TEST(MyDecimal, TestMyDecimalCompare) {
    typedef struct {
        std::string left;
        std::string right;
        int32_t cmp;
    } UseCase;

    UseCase cases[] = {
            {"12", "13", -1},
            {"13", "12", 1},
            {"-10", "10", -1},
            {"10", "-10", 1},
            {"-12", "-13", 1},
            {"0", "12", -1},
            {"-10", "0", -1},
            {"4", "4", 0},
            {"-1.1", "-1.2", 1},
            {"1.2", "1.1", 1},
            {"1.1", "1.2", -1},
    };

    for (const auto & cs : cases ) {
        MyDecimal left;
        int32_t error = left.FromString(cs.left);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.left<<") =" << error;

        MyDecimal right;
        error = right.FromString(cs.right);
        ASSERT_TRUE(error == E_DEC_OK) << "call MyDecimal::FromString(" << cs.right<<") =" << error;

        int32_t ret = left.Compare(right);
        ASSERT_TRUE( ret == cs.cmp) << "[" << ret << "(left.Compare(right)!="  << cs.cmp <<"]";
    }
}

TEST(MyDecimal, TestMyDecimalMaxDecimal) {
    typedef struct {
        int32_t prec;
        int32_t frac;
        std::string num;
    } UseCase;

    UseCase cases[] = {
            {1, 1, "0.9"},
            {1, 0, "9"},
            {2, 1, "9.9"},
            {4, 2, "99.99"},
            {6, 3, "999.999"},
            {8, 4, "9999.9999"},
            {10, 5, "99999.99999"},
            {12, 6, "999999.999999"},
            {14, 7, "9999999.9999999"},
            {16, 8, "99999999.99999999"},
            {18, 9, "999999999.999999999"},
            {20, 10, "9999999999.9999999999"},
            {20, 20, "0.99999999999999999999"},
            {20, 0, "99999999999999999999"},
            {40, 20, "99999999999999999999.99999999999999999999"},
    };

    for (const auto & cs : cases ) {
        MyDecimal d;
        MaxMyDecimal( &d, cs.prec, cs.frac);
        std::string str = d.ToString();
        ASSERT_TRUE( str == cs.num ) << "[" << str << "!="  << cs.num<<"]; param prec = " << cs.prec << ", frac=" << cs.frac;
    }
}

TEST(MyDecimal, TestMyDecimalADD) {
    typedef struct {
        std::string first;
        std::string second;
        std::string result;
        int32_t error;
    } UseCase;

    UseCase cases [] = {
            {".00012345000098765", "123.45", "123.45012345000098765", E_DEC_OK},
            {".1", ".45", "0.55", E_DEC_OK},
            {"1234500009876.5", ".00012345000098765", "1234500009876.50012345000098765", E_DEC_OK},
            {"9999909999999.5", ".555", "9999910000000.055", E_DEC_OK},
            {"99999999", "1", "100000000", E_DEC_OK},
            {"989999999", "1", "990000000", E_DEC_OK},
            {"999999999", "1", "1000000000", E_DEC_OK},
            {"12345", "123.45", "12468.45", E_DEC_OK},
            {"-12345", "-123.45", "-12468.45", E_DEC_OK},
            {"-12345", "123.45", "-12221.55", E_DEC_OK},
            {"12345", "-123.45", "12221.55", E_DEC_OK},
            {"123.45", "-12345", "-12221.55", E_DEC_OK},
            {"-123.45", "12345", "12221.55", E_DEC_OK},
            {"5", "-6.0", "-1.0", E_DEC_OK},
            {"2" + std::string(71,'1'), std::string(81, '8'), "8888888890" + std::string(71, '9'), E_DEC_OK},
            {"-1234.1234", "1234.1234", "0.0000", E_DEC_OK},
    };

    for (const auto & cs : cases ) {
        MyDecimal* first = NewDecFromString(cs.first);
        MyDecimal* second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalAdd( &result, first, second);
        EXPECT_TRUE(error == cs.error ) << "call DecimalAdd =" << error << "; param first=" << cs.first <<",second=" << cs.second;


        std::string str = result.ToString();
        EXPECT_TRUE( str == cs.result) << "[" << str << "!="  << cs.result <<"]; param first= " << cs.first << ", second=" << cs.second;
    }
}

TEST( MyDecimal, TestMyDecimalSub ) {
    typedef struct {
        std::string first;
        std::string second;
        std::string result;
        int32_t error;
    } UseCase;

    UseCase cases [] = {
            {".00012345000098765", "123.45", "-123.44987654999901235", E_DEC_OK},
            {"1234500009876.5", ".00012345000098765", "1234500009876.49987654999901235", E_DEC_OK},
            {"9999900000000.5", ".555", "9999899999999.945", E_DEC_OK},
            {"1111.5551", "1111.555", "0.0001", E_DEC_OK},
            {".555", ".555", "0.000", E_DEC_OK},
            {"10000000", "1", "9999999", E_DEC_OK},
            {"1000001000", ".1", "1000000999.9", E_DEC_OK},
            {"1000000000", ".1", "999999999.9", E_DEC_OK},
            {"12345", "123.45", "12221.55", E_DEC_OK},
            {"-12345", "-123.45", "-12221.55", E_DEC_OK},
            {"123.45", "12345", "-12221.55", E_DEC_OK},
            {"-123.45", "-12345", "12221.55", E_DEC_OK},
            {"-12345", "123.45", "-12468.45", E_DEC_OK},
            {"12345", "-123.45", "12468.45", E_DEC_OK},
            {"12.12", "12.12", "0.00", E_DEC_OK},
    };

    for (const auto & cs : cases ) {
        MyDecimal* first = NewDecFromString(cs.first);
        MyDecimal* second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalSub(&result, first, second);
        EXPECT_TRUE(error == cs.error ) << "call DecimalSub=" << error << "; param first=" << cs.first <<",second=" << cs.second;

        std::string str = result.ToString();
        EXPECT_TRUE( str == cs.result ) << "[" << str << "!="  << cs.result <<"]; param first= " << cs.first << ", second=" << cs.second;
    }
}

TEST( MyDecimal, TestMyDecimalMul ) {
    typedef struct {
        std::string first;
        std::string second;
        std::string result;
        int32_t error;
    } UseCase;

    UseCase cases [] = {
            {"12", "10", "120", E_DEC_OK},
            {"-123.456", "98765.4321", "-12193185.1853376", E_DEC_OK},
            {"-123456000000", "98765432100000", "-12193185185337600000000000", E_DEC_OK},
            {"123456", "987654321", "121931851853376", E_DEC_OK},
            {"123456", "9876543210", "1219318518533760", E_DEC_OK},
            {"123", "0.01", "1.23", E_DEC_OK},
            {"123", "0", "0", E_DEC_OK},
            {"-0.0000000000000000000000000000000000000000000000000017382578996420603", "-13890436710184412000000000000000000000000000000000000000000000000000000000000", "0.000000000000000000000000000000", E_DEC_TRUNCATED},
            {"1" + std::string(60, '0'), "1" + std::string(60, '0'), "0", E_DEC_OVERFLOW},
            {"0.5999991229316", "0.918755041726043", "0.5512522192246113614062276588", E_DEC_OK},
            {"0.5999991229317", "0.918755041726042", "0.5512522192247026369112773314", E_DEC_OK},
            {"0.000", "-1", "0.000", E_DEC_OK},
    };

    for (const auto & cs : cases ) {
        MyDecimal* first = NewDecFromString(cs.first);
        MyDecimal* second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalMul(&result, first, second);
        EXPECT_TRUE(error == cs.error ) << "call DecimalMul=" << error << "; param first=" << cs.first <<",second=" << cs.second;

        std::string str = result.String();
        EXPECT_TRUE( str == cs.result ) << "[" << str << "!="  << cs.result <<"]; param first= " << cs.first << ", second=" << cs.second;
    }
}

TEST( MyDecimal, TestMyDecimalDivMod) {
    typedef struct {
        std::string first;
        std::string second;
        std::string result;
        int32_t error;
    } UseCase;

    UseCase cases[] = {
            {"120",                         "10",                 "12.000000000",                           E_DEC_OK},
            {"123",                         "0.01",               "12300.000000000",                        E_DEC_OK},
            {"120",                         "100000000000.00000", "0.000000001200000000",                   E_DEC_OK},
            {"123",                         "0",                  "",                                       E_DEC_DIV_ZERO},
            {"0",                           "0",                  "",                                       E_DEC_DIV_ZERO},
            {"-12193185.1853376",           "98765.4321",         "-123.456000000000000000",                E_DEC_OK},
            {"121931851853376",             "987654321",          "123456.000000000",                       E_DEC_OK},
            {"0",                           "987",                "0.00000",                                E_DEC_OK},
            {"1",                           "3",                  "0.333333333",                            E_DEC_OK},
            {"1.000000000000",              "3",                  "0.333333333333333333",                   E_DEC_OK},
            {"1",                           "1",                  "1.000000000",                            E_DEC_OK},
            {"0.0123456789012345678912345", "9999999999",         "0.000000000001234567890246913578148141", E_DEC_OK},
            {"10.333000000",                "12.34500",           "0.837019036046982584042122316",          E_DEC_OK},
            {"10.000000000060",             "2",                  "5.000000000030000000",                   E_DEC_OK},
            {"51",                          "0.003430",           "14868.804664723032069970",               E_DEC_OK},
    };

    for (const auto &cs : cases) {
        MyDecimal *first = NewDecFromString(cs.first);
        MyDecimal *second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalDiv(&result, first, second, 5);
        EXPECT_TRUE(error == cs.error) << "call DecimaliDiv=" << error << "; param first=" << cs.first << ",second="
                                       << cs.second;
        if ( error != E_DEC_DIV_ZERO) {
            std::string str = result.ToString();
            EXPECT_TRUE(str == cs.result)
                                << "[" << str << "!=" << cs.result << "]; param first= " << cs.first << ", second="
                                << cs.second;
        }
    }

    UseCase cases_mod[] = {
            {"234",                                    "10",       "4",            E_DEC_OK},
            {"234.567",                                "10.555",   "2.357",        E_DEC_OK},
            {"-234.567",                               "10.555",   "-2.357",       E_DEC_OK},
            {"234.567",                                "-10.555",  "2.357",        E_DEC_OK},
            {"99999999999999999999999999999999999999", "3",        "0",            E_DEC_OK},
            {"51",                                     "0.003430", "0.002760",     E_DEC_OK},
            {"0.0000000001",                           "1.0",      "0.0000000001", E_DEC_OK},
            {"0.000",                                  "0.1",      "0.000",        E_DEC_OK},
    };

    for (const auto &cs : cases_mod) {
        MyDecimal *first = NewDecFromString(cs.first);
        MyDecimal *second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalMod(&result, first, second);
        EXPECT_TRUE(error == cs.error) << "call DecimalMod=" << error << "; param first=" << cs.first << ",second="
                                       << cs.second;
        if ( error != E_DEC_DIV_ZERO) {
            std::string str = result.ToString();
            EXPECT_TRUE(str == cs.result)
                                << "[" << str << "!=" << cs.result << "]; param first= " << cs.first << ", second="
                                << cs.second;
        }
   }

    UseCase cases_div2[] = {
            {"1",     "1",        "1.0000",     E_DEC_OK},
            {"1.00",  "1",        "1.000000",   E_DEC_OK},
            {"1",     "1.000",    "1.0000",     E_DEC_OK},
            {"2",     "3",        "0.6667",     E_DEC_OK},
            {"51",    "0.003430", "14868.8047", E_DEC_OK},
            {"0.000", "0.1",      "0.0000000",  E_DEC_OK},
    };

    for (const auto &cs : cases_div2) {
        MyDecimal *first = NewDecFromString(cs.first);
        MyDecimal *second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalDiv(&result, first, second, DIV_FRAC_INCR);
        EXPECT_TRUE(error == cs.error) << "call DecimalDiv=" << error << "; param first=" << cs.first << ",second="
                                       << cs.second;

        if ( error == E_DEC_DIV_ZERO) {
            std::string str = result.ToString();
            EXPECT_TRUE(str == cs.result)
                                << "[" << str << "!=" << cs.result << "]; param first= " << cs.first << ", second="
                                << cs.second;
        }
    }

    UseCase cases_mod2[] = {
            {"1",    "2.0",      "1.0",      E_DEC_OK},
            {"1.0",  "2",        "1.0",      E_DEC_OK},
            {"2.23", "3",        "2.23",     E_DEC_OK},
            {"51",   "0.003430", "0.002760", E_DEC_OK},
    };

    for (const auto &cs : cases_mod2) {
        MyDecimal *first = NewDecFromString(cs.first);
        MyDecimal *second = NewDecFromString(cs.second);

        MyDecimal result;
        int32_t error = DecimalMod(&result, first, second);
        EXPECT_TRUE(error == cs.error) << "call DecimalMod=" << error << "; param first=" << cs.first << ",second="
                                       << cs.second;
        if ( error == E_DEC_DIV_ZERO ) {
            std::string str = result.ToString();
            EXPECT_TRUE(str == cs.result)
                                << "[" << str << "!=" << cs.result << "]; param first= " << cs.first << ", second="
                                << cs.second;
        }
    }
}

TEST(MyDecimal, TestMyDecimalNewMaxOrMinDec) {
    typedef struct {
        bool neg;
        int32_t prec;
        int32_t frac;
        std::string result;
    } UseCase;

    UseCase cases [] = {
            {true, 2, 1, "-9.9"},
            {false, 1, 1, "0.9"},
            {true, 1, 0, "-9"},
            {false, 0, 0, "0"},
            {false, 4, 2, "99.99"},
    };

    for (const auto & cs : cases ) {
        MyDecimal* d = NewMaxOrMinDec(cs.prec, cs.frac, cs.neg);
        ASSERT_TRUE(d->String() == cs.result );
    }
}

} /* namespace  */
