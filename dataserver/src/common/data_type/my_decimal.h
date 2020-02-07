// Copyright 2018 TiKV Project Authors.
// Portions Copyright 2019 The JIMDB Authors.
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

#include <string>
#include <cassert>

namespace jim {
namespace datatype{

const uint64_t MAX_UINT = UINT64_MAX;
const uint64_t UINT_CUT_OFF = MAX_UINT/static_cast<uint64_t>(10) + 1;
const uint64_t INT_CUT_OFF = static_cast<uint64_t>(INT64_MAX) + 1;

const int32_t MAX_DECIMAL_SCALE = 30;
const int32_t MAX_DECIMAL_WIDTH = 65;

static const int32_t TEN0 = 1;
static const int32_t TEN1 = 10;
static const int32_t TEN2 = 100;
static const int32_t TEN3 = 1000;
static const int32_t TEN4 = 10000;
static const int32_t TEN5 = 100000;
static const int32_t TEN6 = 1000000;
static const int32_t TEN7 = 10000000;
static const int32_t TEN8 = 100000000;
static const int32_t TEN9 = 1000000000;

static const int32_t powers10[10] = {
        TEN0, TEN1, TEN2, TEN3, TEN4, TEN5, TEN6, TEN7, TEN8, TEN9
};
static const int32_t dig2bytes[10] = {
        0, 1, 1, 2, 2, 3, 3, 4, 4, 4
};
static const int32_t FRAC_MAX[] = {
        900000000,
        990000000,
        999000000,
        999900000,
        999990000,
        999999000,
        999999900,
        999999990
};

static const int32_t MAX_WORD_BUF_LEN = 9;
static const int32_t DIGITS_PER_WORD = 9;
static const int32_t WORD_SIZE = 4;
static const int32_t DIG_MASK = TEN8;
static const int32_t WORD_BASE = TEN9;
static const int32_t WORD_MAX = WORD_BASE - 1;
static const int32_t NOT_FIXED_DEC = 31;

static const int32_t DIV_FRAC_INCR = 4;

typedef enum { ModeHalfEven, ModeTruncate, ModeCeiling} RoundMode;

static const int8_t WORD_BUF_LEN = MAX_WORD_BUF_LEN;

typedef enum {
    E_DEC_OK = 0,
    E_DEC_TRUNCATED,
    E_DEC_OVERFLOW,
    E_DEC_DIV_ZERO,
    E_DEC_BAD_NUM,
    E_DEC_OOM,
    E_DEC_ERROR,
    E_DEC_FATAL_ERROR
} E_DEC_TYPE;

class MyDecimal {
public:
    MyDecimal();
    MyDecimal( const int8_t digitsInt, const int8_t digitsFrac, bool negative);
    MyDecimal( const MyDecimal & other);
    MyDecimal& operator = (const MyDecimal & other);
    ~MyDecimal();

    bool GetNegative() const;
    void SetNegative(bool negative);

    int8_t GetDigitsFrac() const;
    void SetDigitsFrac(int8_t digitsFrac);
    int8_t GetDigitsInt() const;
    void SetDigitsInt(int8_t digitsInt);
    int8_t GetResultFrac() const;
    void SetResultFrac(int8_t resultFrac);

    int8_t GetHexDigitsFrac() const;
    void SetHexDigitsFrac(const int8_t hexDigitsFrac);
    int8_t GetHexDigitsInt() const;
    void SetHexDigitsInt(const int8_t hexDigitsInt);

    int32_t GetWordDigit(const int i) const;
    void SetWordDigit(const int i, const int32_t val);

    MyDecimal* Clone() const;
    void Swap(MyDecimal & other);

    std::string String();
    int32_t StringSize() const;

    void RemoveLeadingZeros( int32_t & wordIdx, int32_t & digitsInt) const;
    void RemoveTrailingZeros( int32_t & lastWordIdx, int32_t & digitsFrac) const;

    std::string ToString() const;
    int32_t FromString( const std::string & str);

    int32_t Shift( int32_t shift);

    void DigitBounds( int32_t & start, int32_t & end);

    void DoMiniLeftShift( int32_t shift, int32_t beg, int32_t end);
    void DoMiniRightShift( int32_t shift, int32_t beg, int32_t end);

    void Round( MyDecimal* to, int32_t frac, RoundMode roundMode, int32_t & error);

    MyDecimal& FromInt( const int64_t val );
    MyDecimal& FromUint( const uint64_t val );

    void ToInt( int64_t &  val, int32_t & error) const;
    void ToUint( uint64_t & val, int32_t & error) const;

    MyDecimal& FromFloat64( double f64 );
    void ToFloat64( double & f64, int32_t & error) const ;

    void ToBin( const int32_t precision, const int32_t frac, std::string & bin, int32_t & error) const;
    void ToHashKey(std::string & hash_key, int32_t & error) const;

    void PrecisionAndFrac( int32_t & precision, int32_t & frac) const;

    bool IsZero() const;
    void FromBin( std::string bin, const int32_t precision, const int32_t frac, int32_t & binSize, int32_t & error );

    // Compare compares one decimal to another, returns -1/0/1.
    int32_t Compare( const MyDecimal & other) const;

public:
    MyDecimal & ResetZero();
    MyDecimal & ResetMax();
    MyDecimal & ResetMin();

private:
    typedef int32_t decimal_digit_t;

    int8_t hexDigitsInt_;
    int8_t hexDigitsFrac_;

    int8_t digitsInt_;
    int8_t digitsFrac_;
    int8_t resultFrac_;
    bool negative_;

    decimal_digit_t word_buf_[WORD_BUF_LEN];
};

void MaxMyDecimal( MyDecimal * to, const int32_t precision, int32_t frac);
int32_t DecimalBinSize( const int32_t precision, const int32_t frac);
int32_t DoSub(MyDecimal* to, const MyDecimal* lt, const MyDecimal* rt, int32_t & cmp);
int32_t DoAdd( MyDecimal* to, const MyDecimal* lt, const MyDecimal* rt);
int32_t DecimalSub( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2);
int32_t DecimalAdd( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2);
int32_t DecimalMul( MyDecimal* to , const MyDecimal* from1, const MyDecimal* from2);
int32_t DoDivMod( MyDecimal* mod, MyDecimal* to, const MyDecimal * from1, const MyDecimal* from2, int32_t fracIncr);
int32_t DecimalDiv( MyDecimal* to,const  MyDecimal* from1, const MyDecimal* from2, const int32_t fracIncr);
int32_t DecimalMod( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2);
int32_t DecimalPeak( std::string b, int32_t & dec_bin_len);

MyDecimal* NewDecFromInt(int64_t i);
MyDecimal* NewDecFromUint(uint64_t i);

MyDecimal* NewDecFromString(std::string s);
MyDecimal* NewMaxOrMinDec( int32_t prec, int32_t frac, bool negative );

} // namespace dataytpe
}  // namespace jim
