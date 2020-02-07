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

#include "my_decimal.h"
#include <vector>
#include <algorithm>
#include <utility>

namespace jim {
namespace datatype{


#define MyMax(a, b) ((a)>(b)? (a):(b))
#define MyMin(a, b) ((a)<(b)? (a):(b))

inline bool IsSpace( uint8_t c)
{
    return c == ' ' || c == '\t';
}

inline bool IsDigit( uint8_t c)
{
    return c >= '0' && c <= '9';
}

std::string trim ( std::string str)
{
    if ( str.empty() ) {
        return str;
    }

    str.erase( 0, str.find_first_not_of(" "));
    str.erase( str.find_last_not_of(" ")+1);
    return str;
}

int32_t StrToInt( const std::string & str, int64_t & val)
{
    int32_t error = 0;
    std::string tmpStr = trim(str);
    if ( tmpStr.length() == 0 ) {
        val = 0;
        error = E_DEC_TRUNCATED;
        return error;
    }

    bool negative = false;
    unsigned int i = 0;

    if ( tmpStr[i] == '-') {
        negative = true;
        i++;
    } else if ( str[i] == '+') {
        i++;
    }

    bool hasNum  = false;

    uint64_t r = 0;

    for ( ; i < tmpStr.length(); i++ ) {
        if ( !IsDigit(tmpStr[i])) {
            error = E_DEC_TRUNCATED;
            break;
        }
        hasNum = true;

        if ( r >= UINT_CUT_OFF) {
            r = 0;
            error = E_DEC_BAD_NUM;
            break;
        }

        r = r * 10;

        uint64_t r1 = r + static_cast<uint64_t>(tmpStr[i] - '0');
        if ( r1 < r || r1 > MAX_UINT) {
            r = 0;
            error = E_DEC_BAD_NUM;
            break;
        }
        r = r1;
    }

    if ( !hasNum ) {
        error = E_DEC_TRUNCATED;
    }

    if ( !negative && r >= INT_CUT_OFF ) {
        val = INT64_MAX;
        error = E_DEC_BAD_NUM;
        return error;
    }

    if ( negative && r > INT_CUT_OFF) {
        val = INT64_MIN;
        error = E_DEC_BAD_NUM;
        return error;
    }

    if ( negative ) {
        r = -r;
    }

    val = static_cast<int64_t>(r);

    return error;
}

inline int32_t add( const int32_t a, const int32_t b, int32_t & carry )
{
    int32_t sum = a + b + carry;

    if (sum >= WORD_BASE) {
        carry = 1;
        sum -= WORD_BASE;
    } else {
        carry = 0;
    }
    return sum ;
}

inline int32_t add2( const int32_t a, const int32_t b, int32_t & carry )
{
    int64_t sum = int64_t(a) + int64_t(b) + int64_t(carry);
    if ( sum >= WORD_BASE ) {
        carry = 1;
        sum -= WORD_BASE;
    } else {
        carry = 0;
    }

    if ( sum >= WORD_BASE ) {
        sum -= WORD_BASE;
        carry++;
    }

    return static_cast<int32_t>(sum);
}

inline int32_t sub( const int32_t a, const int32_t b , int32_t & carry )
{
    int32_t diff = a - b - carry;
    if ( diff < 0 ) {
        carry = 1;
        diff += WORD_BASE;
    } else {
        carry = 0;
    }

    return diff;
}

inline int32_t sub2( const int32_t a, const int32_t b , int32_t & carry )
{
    int32_t diff = a - b - carry;
    if ( diff < 0 ) {
        carry = 1;
        diff += WORD_BASE;
    } else {
        carry = 0;
    }

    if ( diff < 0 ) {
        diff += WORD_BASE;
        carry++;
    }

    return diff;
}

void FixWordCntError(int32_t & wordsInt, int32_t & wordsFrac, int32_t & error)
{
    if ( wordsInt + wordsFrac > WORD_BUF_LEN ) {
        if ( wordsInt > WORD_BUF_LEN ) {
            wordsInt = WORD_BUF_LEN;
            wordsFrac = 0;
            error = E_DEC_OVERFLOW;
            return ;
        } else {
            // wordsInt = wordsInt;
            wordsFrac = WORD_BUF_LEN - wordsInt;

            error = E_DEC_TRUNCATED;
            return;
        }
    }

    error = E_DEC_OK;
}

int32_t CountLeadingZeroes( int32_t i, int32_t word)
{
    int32_t leading = 0;

    while ( word < powers10[i] && i >= 0) {
        i--;
        leading++;
    }
    return leading;
}

int32_t CountTrailingZeroes(int32_t i, int32_t word)
{
    int32_t trailing = 0;

    while ( (word % powers10[i]) == 0) {
        i++;
        trailing++;
    }
    return trailing;
}

int32_t DigitsToWords( int32_t digits )
{
    int32_t word = 0;
    word = (digits + DIGITS_PER_WORD - 1) / DIGITS_PER_WORD;
    return word;
}

const int32_t MY_DECIMAL_SIZE = 40;

MyDecimal ZeroMyDecimalWithFrac(int8_t frac)
{
    MyDecimal zero(0, frac, false);
    return zero;
}

int32_t DecimalBinSize( const int32_t precision, const int32_t frac)
{
    int32_t digitsInt = precision - frac;
    int32_t wordsInt = digitsInt / DIGITS_PER_WORD;
    int32_t wordsFrac = frac / DIGITS_PER_WORD;
    int32_t xInt = digitsInt - wordsInt * DIGITS_PER_WORD;
    int32_t xFrac = frac - wordsFrac * DIGITS_PER_WORD;

    return wordsInt * WORD_SIZE + dig2bytes[xInt] + wordsFrac * WORD_SIZE + dig2bytes[xFrac];
}

int32_t ReadWord( const std::string b, const int32_t size)
{
    int32_t x = 0;

    assert(b.length() >= static_cast<uint32_t>(size));

    switch(size) {
        case 1: {
            x = static_cast<int32_t>(static_cast<int8_t>(b[0]));
            break;
        }
        case 2: {
            x = (static_cast<int32_t>(static_cast<int8_t>(b[0])) << 8)
                + (static_cast<int32_t>(static_cast<uint8_t>(b[1])));
            break;
        }
        case 3: {
            if ( (b[0] & 128) > 0) {
                x = (static_cast<int32_t>( static_cast<uint32_t>(255) << 24)
                     | (static_cast<uint32_t>(static_cast<uint8_t>(b[0])) << 16)
                     | (static_cast<uint32_t>(static_cast<uint8_t>(b[1])) << 8)
                     | (static_cast<uint32_t>(static_cast<uint8_t>(b[2])))
                );
            } else {
                x = (static_cast<int32_t> ( static_cast<uint32_t>(static_cast<uint8_t>(b[0])) << 16)
                     | (static_cast<uint32_t>(static_cast<uint8_t>(b[1])) << 8)
                     | (static_cast<uint32_t>(static_cast<uint8_t>(b[2])))
                );

            }
            break;
        }
        case 4: {
            x = static_cast<int32_t>(static_cast<uint8_t>(b[3]))
                + (static_cast<int32_t>(static_cast<uint8_t>(b[2])) << 8)
                + (static_cast<int32_t>(static_cast<uint8_t>(b[1])) << 16)
                + (static_cast<int32_t>(static_cast<int8_t>(b[0])) << 24);
            break;
        }
        default: {
            // never to here.
        }
    }

    return x;

}

void WriteWord( std::string & b, const int32_t word, const int32_t size)
{
    uint32_t x = static_cast<uint32_t>(word);
    assert(b.length() >= static_cast<uint32_t>(size));

    switch( size ) {
        case 1: {
            b[0] = static_cast<uint8_t>(x);
            break;
        }
        case 2: {
            b[0] = static_cast<uint8_t>(x >> 8 );
            b[1] = static_cast<uint8_t>(x);
            break;
        }
        case 3: {
            b[0] = static_cast<uint8_t>(x >> 16 );
            b[1] = static_cast<uint8_t>(x >> 8);
            b[2] = static_cast<uint8_t>(x);
            break;
        }
        case 4: {
            b[0] = static_cast<uint8_t>(x >> 24 );
            b[1] = static_cast<uint8_t>(x >> 16);
            b[2] = static_cast<uint8_t>(x >> 8);
            b[3] = static_cast<uint8_t>(x);
            break;
        }
        default : {
            break;
        }
    }
}

MyDecimal MaxMyDecimal( const int32_t precision, const int32_t frac)
{
    int32_t digitsInt = precision - frac;
    int32_t digitsFrac = frac;

    MyDecimal d(static_cast<int8_t>(digitsInt), static_cast<int8_t>(digitsFrac), false);
    d.ResetMax();

    return d;
}

int32_t DecimalAdd( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2)
{
    int32_t cmp = 0, error = 0;
    to->SetResultFrac(MyMax(from1->GetResultFrac(), from2->GetResultFrac()));
    if ( from1->GetNegative() == from2->GetNegative()) {
        error = DoAdd(to, from1, from2);
        return error;
    }

    error = DoSub(to, from1, from2, cmp);
    return error;
}

int32_t DecimalSub( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2)
{
    int32_t cmp = 0, error = 0;
    to->SetResultFrac(MyMax(from1->GetResultFrac(), from2->GetResultFrac()));
    if ( from1->GetNegative() == from2->GetNegative()) {
        error = DoSub(to, from1, from2, cmp);
        return error;
    }

    error = DoAdd(to, from1, from2);
    return error;
}

int32_t DoSub(MyDecimal * to, const MyDecimal * lt, const MyDecimal * rt, int32_t & cmp)
{
    int32_t error = E_DEC_OK;
    MyDecimal from1(*lt);
    MyDecimal from2(*rt);

    int32_t wordsInt1   = DigitsToWords(static_cast<int32_t>(from1.GetDigitsInt()));
    int32_t wordsFrac1  = DigitsToWords(static_cast<int32_t>(from1.GetDigitsFrac()));
    int32_t wordsInt2   = DigitsToWords(static_cast<int32_t>(from2.GetDigitsInt()));
    int32_t wordsFrac2  = DigitsToWords(static_cast<int32_t>(from2.GetDigitsFrac()));
    int32_t wordsFracTo = MyMax(wordsFrac1, wordsFrac2);

    int32_t start1 = 0;
    int32_t stop1  = wordsInt1;
    int32_t idx1   = 0;
    int32_t start2 = 0;
    int32_t stop2  = wordsInt2;
    int32_t idx2   = 0;
    if (from1.GetWordDigit(idx1) == 0) {
        while (idx1 < stop1 && from1.GetWordDigit(idx1) == 0) {
            idx1++;
        }
        start1 = idx1;
        wordsInt1 = stop1 - idx1;
    }
    if (from2.GetWordDigit(idx2) == 0) {
        while( (idx2<stop2) && (from2.GetWordDigit(idx2)==0) ) {
            idx2++;
        }
        start2 = idx2;
        wordsInt2 = stop2 - idx2;
    }

    int32_t carry = 0;
    if (wordsInt2 > wordsInt1) {
        carry = 1;
    } else if (wordsInt2 == wordsInt1) {
        int32_t end1 = stop1 + wordsFrac1 - 1;
        int32_t end2 = stop2 + wordsFrac2 - 1;
        while ( (idx1<=end1) && (from1.GetWordDigit(end1)==0) ){
            end1--;
        }
        while( (idx2<=end2) && (from2.GetWordDigit(end2)==0) ){
            end2--;
        }
        wordsFrac1 = end1 - stop1 + 1;
        wordsFrac2 = end2 - stop2 + 1;
        while ( (idx1<=end1) && (idx2<=end2) && (from1.GetWordDigit(idx1)==from2.GetWordDigit(idx2))) {
            idx1++;
            idx2++;
        }
        if (idx1 <= end1) {
            if ( (idx2 <= end2) && (from2.GetWordDigit(idx2)>from1.GetWordDigit(idx1)) ) {
                carry = 1;
            } else {
                carry = 0;
            }
        } else {
            if (idx2 <= end2) {
                carry = 1;
            } else {
                if (to == nullptr) {
                    cmp = 0;
                    error = E_DEC_OK;
                    return error;
                }

                *to = ZeroMyDecimalWithFrac(to->GetResultFrac());
                cmp = 0;
                error = E_DEC_OK;
                return error;
            }
        }
    }

    if (to == nullptr) {
        if ((carry>0) == from1.GetNegative()) { // from2 is negative too.
            cmp = 1;
            error = E_DEC_OK;
            return error;
        }

        cmp = -1;
        error = E_DEC_OK;
        return error;
    }

    to->SetNegative(from1.GetNegative());

    // ensure that always idx1 > idx2 (and wordsInt1 >= wordsInt2)
    if (carry > 0) {

        from1.Swap(from2);
        std::swap(start1, start2);
        std::swap(wordsInt1, wordsInt2);
        std::swap(wordsFrac1, wordsFrac2);
        to->SetNegative( !to->GetNegative());
    }

    FixWordCntError(wordsInt1, wordsFracTo, error);

    int32_t idxTo = wordsInt1 + wordsFracTo;

    to->SetDigitsFrac(from1.GetDigitsFrac());
    if (to->GetDigitsFrac() < from2.GetDigitsFrac()) {
        to->SetDigitsFrac(from2.GetDigitsFrac());
    }
    to->SetDigitsInt(static_cast<int8_t>(wordsInt1 * DIGITS_PER_WORD));
    if (error != E_DEC_OK){
        if ( to->GetDigitsFrac() > (wordsFracTo * DIGITS_PER_WORD)) {
            to->SetDigitsFrac(static_cast<int8_t>(wordsFracTo * DIGITS_PER_WORD));
        }
        if (wordsFrac1 > wordsFracTo) {
            wordsFrac1 = wordsFracTo;
        }
        if (wordsFrac2 > wordsFracTo) {
            wordsFrac2 = wordsFracTo;
        }
        if (wordsInt2 > wordsInt1) {
            wordsInt2 = wordsInt1;
        }
    }
    carry = 0;

    // part 1 - max(frac) ... min (frac)
    if (wordsFrac1 > wordsFrac2) {
        idx1 = start1 + wordsInt1 + wordsFrac1;
        stop1 = start1 + wordsInt1 + wordsFrac2;
        idx2 = start2 + wordsInt2 + wordsFrac2;
        while(wordsFracTo > wordsFrac1) {
            wordsFracTo--;
            idxTo--;
            to->SetWordDigit(idxTo, 0);
        }
        while (idx1 > stop1) {
            idxTo--;
            idx1--;
            to->SetWordDigit(idxTo, from1.GetWordDigit(idx1));
        }
    } else {
        idx1 = start1 + wordsInt1 + wordsFrac1;
        idx2 = start2 + wordsInt2 + wordsFrac2;
        stop2 = start2 + wordsInt2 + wordsFrac1;
        while (wordsFracTo > wordsFrac2) {
            wordsFracTo--;
            idxTo--;
            to->SetWordDigit(idxTo, 0);
        }
        while (idx2 > stop2) {
            idxTo--;
            idx2--;
            to->SetWordDigit(idxTo, sub(0, from2.GetWordDigit(idx2), carry));
        }
    }

    // part 2 - min(frac) ... wordsInt2
    while(idx2 > start2) {
        idxTo--;
        idx1--;
        idx2--;
        to->SetWordDigit(idxTo, sub(from1.GetWordDigit(idx1), from2.GetWordDigit(idx2), carry));
    }

    // part 3 - wordsInt2 ... wordsInt1
    while(carry > 0 && idx1 > start1) {
        idxTo--;
        idx1--;
        to->SetWordDigit(idxTo, sub(from1.GetWordDigit(idx1), 0, carry));
    }

    while(idx1 > start1) {
        idxTo--;
        idx1--;
        to->SetWordDigit(idxTo, from1.GetWordDigit(idx1));
    }
    while(idxTo > 0) {
        idxTo--;
        to->SetWordDigit(idxTo, 0);
    }
    cmp = 0;

    return error;
}

int32_t DoAdd( MyDecimal* to, const MyDecimal* lt, const MyDecimal* rt)
{
    int32_t error = 0;
    MyDecimal from1(*lt);
    MyDecimal from2(*rt);
    int32_t wordsInt1 = DigitsToWords(static_cast<int32_t>(from1.GetDigitsInt()));
    int32_t wordsFrac1 = DigitsToWords(static_cast<int32_t>(from1.GetDigitsFrac()));
    int32_t wordsInt2 = DigitsToWords(static_cast<int32_t>(from2.GetDigitsInt()));
    int32_t wordsFrac2 = DigitsToWords(static_cast<int32_t>(from2.GetDigitsFrac()));
    int32_t wordsIntTo = MyMax(wordsInt1, wordsInt2);
    int32_t wordsFracTo = MyMax(wordsFrac1, wordsFrac2);

    int32_t x = 0;

    if ( wordsInt1 > wordsInt2 ) {
        x = from1.GetWordDigit(0);
    } else if ( wordsInt2 > wordsInt1) {
        x = from2.GetWordDigit(0);
    } else {
        x = from1.GetWordDigit(0) + from2.GetWordDigit(0);
    }

    if ( x > WORD_MAX - 1) {
        wordsIntTo++;
        to->SetWordDigit(0, 0);
    }

    FixWordCntError(wordsIntTo, wordsFracTo, error);
    if ( error == E_DEC_OVERFLOW ) {
        *to = MaxMyDecimal( WORD_BUF_LEN * DIGITS_PER_WORD, 0);
        return error ;
    }

    int32_t idxTo = wordsIntTo + wordsFracTo;
    to->SetNegative(from1.GetNegative());
    to->SetDigitsInt(static_cast<int32_t>(wordsIntTo * DIGITS_PER_WORD));
    to->SetDigitsFrac(MyMax(from1.GetDigitsFrac(), from2.GetDigitsFrac()));

    if ( error != E_DEC_OK) {
        if ( to->GetDigitsFrac() > wordsFracTo * DIGITS_PER_WORD ) {
            to->SetDigitsFrac(static_cast<int8_t>(wordsFracTo * DIGITS_PER_WORD));
        }

        if (wordsFrac1 > wordsFracTo) {
            wordsFrac1 = wordsFracTo;
        }

        if ( wordsFrac2 > wordsFracTo ) {
            wordsFrac2 = wordsFracTo;
        }

        if ( wordsInt1 > wordsIntTo ) {
            wordsInt1 = wordsIntTo;
        }

        if ( wordsInt2 > wordsIntTo ) {
            wordsInt2 = wordsIntTo;
        }
    }


    MyDecimal dec1(from1), dec2(from2);
    int32_t idx1 = 0, idx2 = 0, stop = 0, stop2 = 0;

    if ( wordsFrac1 > wordsFrac2 ) {
        idx1 = wordsInt1 + wordsFrac1;
        stop = wordsInt1 + wordsFrac2;
        idx2 = wordsInt2 + wordsFrac2;

        if ( wordsInt1 > wordsInt2 ) {
            stop2 = wordsInt1 - wordsInt2;
        }
    } else {
        idx1 = wordsInt2 + wordsFrac2;
        stop = wordsInt2 + wordsFrac1;
        idx2 = wordsInt1 + wordsFrac1;

        if (wordsInt2 > wordsInt1) {
            stop2 = wordsInt2 - wordsInt1;
        }
        dec1 = from2;
        dec2 = from1;
    }

    while (idx1 > stop) {
        idxTo--;
        idx1--;
        to->SetWordDigit(idxTo, dec1.GetWordDigit(idx1));
    }

    int32_t carry = 0;
    while (idx1 > stop2) {
        idx1--;
        idx2--;
        idxTo--;
        to->SetWordDigit(idxTo, add(dec1.GetWordDigit(idx1), dec2.GetWordDigit(idx2), carry));
    }

    stop = 0;
    if ( wordsInt1 > wordsInt2) {
        idx1 = wordsInt1 - wordsInt2;
        dec1 = from1;
        dec2 = from2;
    } else {
        idx1 = wordsInt2 - wordsInt1;
        dec1 = from2;
        dec2 = from1;
    }

    while (idx1 > stop) {
        idxTo--;
        idx1--;
        to->SetWordDigit(idxTo, add(dec1.GetWordDigit(idx1), 0, carry));
    }

    if ( carry > 0 ) {
        idxTo--;
        to->SetWordDigit(idxTo, 1);
    }
    return error;
}

void MaxMyDecimal( MyDecimal * to, const int32_t precision, int32_t frac)
{
    int32_t digitsInt = precision - frac;
    to->SetNegative(false);
    to->SetDigitsInt(static_cast<int8_t>(digitsInt));
    int32_t idx = 0;

    if ( digitsInt > 0 ) {
        int32_t firstWordDigits = digitsInt % DIGITS_PER_WORD;
        if ( firstWordDigits > 0 ) {
            to->SetWordDigit(idx, powers10[firstWordDigits]-1);
            idx++;
        }

        for ( digitsInt /= DIGITS_PER_WORD; digitsInt > 0; digitsInt-- ) {
            to->SetWordDigit(idx, WORD_MAX);
            idx++;
        }
    }

    to->SetDigitsFrac(static_cast<int8_t>(frac));
    if (frac > 0) {
        int32_t lastDigits = frac % DIGITS_PER_WORD;

        for ( frac /= DIGITS_PER_WORD; frac > 0; frac-- ) {
            to->SetWordDigit(idx, WORD_MAX);
            idx++;
        }

        if ( lastDigits > 0) {
            to->SetWordDigit(idx, FRAC_MAX[lastDigits-1]);
        }
    }
}

int32_t DecimalMul( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2)
{
    int32_t error = 0;
    int32_t wordsInt1 = DigitsToWords(static_cast<int32_t>(from1->GetDigitsInt()));
    int32_t wordsFrac1 = DigitsToWords(static_cast<int32_t>(from1->GetDigitsFrac()));
    int32_t wordsInt2 = DigitsToWords(static_cast<int32_t>(from2->GetDigitsInt()));
    int32_t wordsFrac2 = DigitsToWords(static_cast<int32_t>(from2->GetDigitsFrac()));
    int32_t wordsIntTo = DigitsToWords(static_cast<int32_t>(from1->GetDigitsInt() + from2->GetDigitsInt()));
    int32_t wordsFracTo = wordsFrac1 + wordsFrac2;
    int32_t idx1 = wordsInt1;
    int32_t idx2 = wordsInt2;
    int32_t idxTo = 0;
    int32_t tmp1 = wordsIntTo;
    int32_t tmp2 = wordsFracTo;

    to->SetResultFrac(MyMin(static_cast<int32_t>(from1->GetResultFrac() + from2->GetResultFrac()), MAX_DECIMAL_SCALE));
    FixWordCntError(wordsIntTo, wordsFracTo, error);
    to->SetNegative(from1->GetNegative() != from2->GetNegative());
    to->SetDigitsFrac(from1->GetDigitsFrac() + from2->GetDigitsFrac()) ;
    if ( to->GetDigitsFrac() > NOT_FIXED_DEC) {
        to->SetDigitsFrac(NOT_FIXED_DEC);
    }

    to->SetDigitsInt(wordsIntTo * DIGITS_PER_WORD);
    if (error == E_DEC_OVERFLOW) {
        return error;
    }


    if (error != E_DEC_OK){
        if (to->GetDigitsFrac() > (wordsFracTo * DIGITS_PER_WORD)) {
            to->SetDigitsFrac(static_cast<int8_t>(wordsFracTo * DIGITS_PER_WORD));
        }

        if (to->GetDigitsInt() > (wordsIntTo*DIGITS_PER_WORD)) {
            to->SetDigitsInt(static_cast<int8_t>(wordsIntTo * DIGITS_PER_WORD));
        }

        if (tmp1 > wordsIntTo) {
            tmp1 -= wordsIntTo;
            tmp2 = tmp1 >> 1;
            wordsInt2 -= (tmp1 - tmp2);
            wordsFrac1 = 0;
            wordsFrac2 = 0;
        } else {
            tmp2 -= wordsFracTo;
            tmp1 = tmp2 >> 1;
            if (wordsFrac1 <= wordsFrac2) {
                wordsFrac1 -= tmp1;
                wordsFrac2 -= (tmp2 - tmp1);
            } else {
                wordsFrac2 -= tmp1;
                wordsFrac1 -= (tmp2 - tmp1);
            }
        }
    }

    int32_t startTo = wordsIntTo + wordsFracTo - 1;
    int32_t start2 = idx2 + wordsFrac2 - 1;
    int32_t stop1 = idx1 - wordsInt1;
    int32_t stop2 = idx2 - wordsInt2;
    to->ResetZero();

    for (idx1 += wordsFrac1 - 1; idx1 >= stop1; idx1--) {
        int32_t carry = 0;
        idxTo = startTo;
        idx2 = start2;
        while (idx2 >= stop2) {
            int32_t hi, lo;
            int64_t p = static_cast<int64_t>(from1->GetWordDigit(idx1)) * static_cast<int64_t>(from2->GetWordDigit(idx2));
            hi = p / WORD_BASE;
            lo = static_cast<int32_t>(p - hi*WORD_BASE);
            to->SetWordDigit(idxTo, add2(to->GetWordDigit(idxTo), lo, carry));
            carry += hi;
            idx2--;
            idxTo--;
        }
        if (carry > 0) {
            if (idxTo < 0) {
                error = E_DEC_OVERFLOW;
                return error;
            }
            to->SetWordDigit(idxTo, add2(to->GetWordDigit(idxTo), 0, carry));
        }
        for (idxTo--; carry > 0; idxTo--) {
            if (idxTo < 0) {
                error = E_DEC_OVERFLOW;
                return error;
            }
            to->SetWordDigit(idxTo, add(to->GetWordDigit(idxTo), 0, carry));
        }
        startTo--;
    }

    // Now we have to check for -0.000 case
    if (to->GetNegative()) {
        int32_t idx = 0;
        int32_t end = wordsIntTo + wordsFracTo;
        while (1) {
            if (to->GetWordDigit(idx) != 0) {
                break;
            }
            idx++;
            // We got decimal zero
            if (idx == end) {
                *to = ZeroMyDecimalWithFrac(to->GetResultFrac());
                break;
            }
        }
    }

    idxTo = 0;
    int32_t dToMove = wordsIntTo + DigitsToWords(to->GetDigitsFrac());
    while(to->GetWordDigit(idxTo) == 0 && (to->GetDigitsInt() > DIGITS_PER_WORD)) {
        idxTo++;
        to->SetDigitsInt( to->GetDigitsInt()-DIGITS_PER_WORD);
        dToMove--;
    }
    if (idxTo > 0) {
        int32_t curIdx = 0;
        while(dToMove > 0) {
            to->SetWordDigit(curIdx,to->GetWordDigit(idxTo));
            curIdx++;
            idxTo++;
            dToMove--;
        }
    }
    return error;
}

int32_t DecimalDiv( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2, int32_t fracIncr)
{
    int32_t error = 0;
    to->SetResultFrac(MyMin(from1->GetResultFrac() + fracIncr, MAX_DECIMAL_SCALE));
    error = DoDivMod(to, nullptr, from1, from2, fracIncr);
    return error;
}

int32_t DecimalMod( MyDecimal* to, const MyDecimal* from1, const MyDecimal* from2)
{
    int32_t error = 0;
    to->SetResultFrac(MyMax(from1->GetResultFrac(), from2->GetResultFrac()));
    error = DoDivMod( nullptr, to, from1, from2, 0);
    return error;
}

int32_t DoDivMod( MyDecimal* to, MyDecimal* mod, const MyDecimal* from1, const MyDecimal* from2, int32_t fracIncr)
{
    int32_t error = 0;

    int32_t frac1 = DigitsToWords(static_cast<int32_t>(from1->GetDigitsFrac())) * DIGITS_PER_WORD;
    int32_t prec1 = static_cast<int32_t>(from1->GetDigitsInt()) + frac1;

    int32_t frac2 = DigitsToWords(static_cast<int32_t>(from2->GetDigitsFrac())) * DIGITS_PER_WORD;
    int32_t prec2 = static_cast<int32_t>(from2->GetDigitsInt()) + frac2;

    if (mod != nullptr) {
        to = mod;
    }

    int32_t i = ((prec2 - 1) % DIGITS_PER_WORD) + 1;
    int32_t idx2 = 0;

    while ((prec2 > 0) && (from2->GetWordDigit(idx2) == 0)) {
        prec2 -= i;
        i = DIGITS_PER_WORD;
        idx2++;
    }

    if (prec2 <= 0) {
        error = E_DEC_DIV_ZERO;
        return error;
    }

    prec2 -= CountLeadingZeroes((prec2 - 1) % DIGITS_PER_WORD, from2->GetWordDigit(idx2));
    i = ((prec1 - 1) % DIGITS_PER_WORD) + 1;
    int32_t idx1 = 0;

    while (prec1 > 0 && from1->GetWordDigit(idx1) == 0) {
        prec1 -= i;
        i = DIGITS_PER_WORD;
        idx1++;
    }

    if (prec1 <= 0) {
        *to = ZeroMyDecimalWithFrac(to->GetResultFrac());
        error = E_DEC_OK;
        return error;
    }

    prec1 -= CountLeadingZeroes((prec1 - 1) % DIGITS_PER_WORD, from1->GetWordDigit(idx1));

    fracIncr -= (frac1 - static_cast<int32_t>(from1->GetDigitsFrac()) + frac2 - static_cast<int32_t>(from2->GetDigitsFrac()));
    if (fracIncr < 0) {
        fracIncr = 0;
    }

    int32_t digitsIntTo = (prec1 - frac1) - (prec2 - frac2);
    if (from1->GetWordDigit(idx1) >= from2->GetWordDigit(idx2)) {
        digitsIntTo++;
    }

    int32_t wordsIntTo = 0;
    if (digitsIntTo < 0) {
        digitsIntTo /= DIGITS_PER_WORD;
        wordsIntTo = 0;
    } else {
        wordsIntTo = DigitsToWords(digitsIntTo);
    }

    int32_t wordsFracTo = 0;
    if (mod != nullptr) {

        to->SetNegative(from1->GetNegative());
        to->SetDigitsFrac(MyMax(from1->GetDigitsFrac(), from2->GetDigitsFrac()));
    } else {

        wordsFracTo = DigitsToWords(frac1 + frac2 + fracIncr);
        FixWordCntError(wordsIntTo, wordsFracTo, error);
        to->SetNegative(from1->GetNegative() != from2->GetNegative());
        to->SetDigitsInt(static_cast<int8_t>(wordsIntTo * DIGITS_PER_WORD));
        to->SetDigitsFrac(static_cast<int8_t>(wordsFracTo * DIGITS_PER_WORD));
    }

    int8_t idxTo = 0;
    int32_t stopTo = wordsIntTo + wordsFracTo;

    if (mod == nullptr) {
        while (digitsIntTo < 0 && idxTo < WORD_BUF_LEN) {
            to->SetWordDigit(idxTo, 0);
            idxTo++;
            digitsIntTo++;
        }
    }

    i = DigitsToWords(prec1);
    int32_t len1 = i + DigitsToWords(2 * frac2 + fracIncr + 1) + 1;
    if (len1 < 3) {
        len1 = 3;
    }

    std::vector<int32_t> tmp1(len1, 0);
    for ( int32_t it = idx1; it < idx1 + i; it++) {
        tmp1[it-idx1] = from1->GetWordDigit(it);
    }

    int32_t start1 = 0;
    int32_t stop1 = 0;
    int32_t start2 = idx2;
    int32_t stop2 = idx2 + DigitsToWords(prec2) - 1;

    while (from2->GetWordDigit(stop2) == 0 && stop2 >= start2) {
        stop2--;
    }

    int32_t len2 = stop2 - start2;
    stop2++;

    int32_t normFactor = WORD_BASE / static_cast<int64_t>(from2->GetWordDigit(start2) + 1);
    int32_t norm2 = static_cast<int32_t>(normFactor * static_cast<int64_t>(from2->GetWordDigit(start2)));

    if (len2 > 0) {
        norm2 += static_cast<int32_t>( normFactor * static_cast<int64_t>(from2->GetWordDigit(start2 + 1)) / WORD_BASE);
    }

    int32_t dcarry = 0;

    if (tmp1[start1] < from2->GetWordDigit(start2)) {
        dcarry = tmp1[start1];
        start1++;
    }

    int64_t guess = 0;

    for (; idxTo < stopTo; idxTo++) {
        if (dcarry == 0 && (tmp1[start1] < from2->GetWordDigit(start2))) {
            guess = 0;
        } else {
            int64_t x = static_cast<int64_t>(tmp1[start1]) + static_cast<int64_t>(dcarry) * WORD_BASE;
            int64_t y = static_cast<int64_t>(tmp1[start1 + 1]);

            guess = (normFactor * x + normFactor * y / WORD_BASE) / static_cast<int64_t>(norm2);
            if (guess > WORD_BASE) {
                guess = WORD_BASE - 1;
            }

            if (len2 > 0) {
                if ((static_cast<int64_t>(from2->GetWordDigit(start2 + 1)) * guess) >
                    ((x - guess * static_cast<int64_t>(from2->GetWordDigit(start2))) * WORD_BASE + y)
                        ) {
                    guess--;
                }

                if ((static_cast<int64_t>(from2->GetWordDigit(start2 + 1)) * guess) >
                    ((x - guess * static_cast<int64_t>(from2->GetWordDigit(start2))) * WORD_BASE + y)
                        ) {
                    guess--;
                }
            }

            idx2 = stop2;
            idx1 = start1 + len2;

            int32_t carry = 0;

            for (carry = 0; idx2 > start2; idx1--) {
                int32_t hi, lo;
                idx2--;
                x = guess * static_cast<int64_t>(from2->GetWordDigit(idx2));
                hi = static_cast<int32_t>( x / WORD_BASE);
                lo = static_cast<int32_t>( x - static_cast<int64_t>(hi) * WORD_BASE);

                tmp1[idx1] = sub2(tmp1[idx1], lo, carry);
                carry += hi;
            }

            if (dcarry < carry) {
                carry = 1;
            } else {
                carry = 0;
            }

            if (carry > 0) {
                guess--;
                idx2 = stop2;
                idx1 = start1 + len2;

                for (carry = 0; idx2 > start2; idx1--) {
                    idx2--;
                    tmp1[idx1] = add(tmp1[idx1], from2->GetWordDigit(idx2), carry);
                }
            }
        }

        if (mod == nullptr) {
            to->SetWordDigit(idxTo, static_cast<int32_t>(guess));
        }
        dcarry = tmp1[start1];
        start1++;
    }

    if (mod != nullptr) {
        if ( dcarry != 0 ) {
            start1--;
            tmp1[start1] = dcarry;
        }

        idxTo = 0;

        digitsIntTo = prec1 - frac1 - start1 * DIGITS_PER_WORD;

        if (digitsIntTo < 0) {
            wordsIntTo = digitsIntTo / DIGITS_PER_WORD;
        } else {
            wordsIntTo = DigitsToWords(digitsIntTo);
        }

        wordsFracTo = DigitsToWords(static_cast<int32_t>(to->GetDigitsFrac()));
        error = E_DEC_OK;

        if ((wordsIntTo == 0) && (wordsFracTo == 0)) {
            to->ResetZero();
            return error;
        }
        if (wordsIntTo <= 0) {
            if (-wordsIntTo >= WORD_BUF_LEN) {
                to->ResetZero();
                error = E_DEC_TRUNCATED;
                return error;
            }

            stop1 = start1 + wordsIntTo + wordsFracTo;
            wordsFracTo += wordsIntTo;
            to->SetDigitsInt(0);

            while (wordsIntTo < 0) {
                to->SetWordDigit(idxTo, 0);
                idxTo++;
                wordsIntTo++;
            }
        } else {
            if (wordsIntTo > WORD_BUF_LEN) {
                to->SetDigitsInt(static_cast<int8_t>(DIGITS_PER_WORD * WORD_BUF_LEN));
                to->SetDigitsFrac(0);
                error = E_DEC_OVERFLOW;
                return error;
            }

            stop1 = start1 + wordsIntTo + wordsFracTo;
            to->SetDigitsInt( static_cast<int8_t>(MyMin(wordsIntTo* DIGITS_PER_WORD, static_cast<int32_t>(from2->GetDigitsInt()))));
        }

        if ((wordsIntTo + wordsFracTo) > WORD_BUF_LEN) {
            stop1 -= wordsIntTo + wordsFracTo - WORD_BUF_LEN;
            wordsFracTo = WORD_BUF_LEN - wordsIntTo;
            to->SetDigitsFrac(static_cast<int8_t>( wordsFracTo * DIGITS_PER_WORD));
            error = E_DEC_TRUNCATED;
        }

        while(start1 < stop1) {
            to->SetWordDigit(static_cast<int>(idxTo), tmp1[start1]);
            idxTo++;
            start1++;
        }
    }

    int32_t tmpIdxTo = 0;
    to->RemoveLeadingZeros(tmpIdxTo, digitsIntTo);

    idxTo = tmpIdxTo;
    to->SetDigitsInt(static_cast<int8_t>(digitsIntTo));
    if (idxTo != 0 ) {
        for ( int32_t ii = idxTo; ii < WORD_BUF_LEN; ii++) {
            to->SetWordDigit(ii-idxTo, to->GetWordDigit(ii));
        }
    }
    return error;
}

int32_t DecimalPeak( std::string b, int32_t & dec_bin_len)
{
    int32_t error = 0;

    if ( b.length() < 3 ){
        dec_bin_len = 0;
        error = E_DEC_BAD_NUM;
        return error;
    }

    int32_t precision = static_cast<int32_t>(b[0]);
    int32_t frac = static_cast<int32_t>(b[1]);

    dec_bin_len = DecimalBinSize(precision, frac) + 2;
    error = E_DEC_OK;
    return error;
}

MyDecimal* NewDecFromInt(int64_t i)
{
    MyDecimal * dec = new MyDecimal();
    dec->FromInt(i);
    return dec;
}

MyDecimal* NewDecFromUint(uint64_t i)
{
    MyDecimal * dec = new MyDecimal();
    dec->FromUint(i);

    return dec;
}

//MyDecimal* NewDecFromFloat( double f64)
//{
//    MyDecimal* dec = new MyDecimal();
//
//    return *dec;
//}
//
MyDecimal* NewDecFromString(std::string s)
{
    MyDecimal* dec = new MyDecimal();
    int32_t error = dec->FromString(s);
    return dec;
}

MyDecimal* NewMaxOrMinDec( int32_t prec, int32_t frac, bool negative )
{
    std::string str;
    str.resize(prec+2);
    for ( size_t i = 0; i < str.length(); i++) {
        str[i] = '9';
    }

    if (negative) {
        str[0] = '-';
    } else {
        str[0] = '+';
    }

    str[1+prec-frac] = '.';

    MyDecimal * dec = new MyDecimal();
    int32_t error = dec->FromString(str);
    return dec;
}


MyDecimal::MyDecimal()
{
    this->digitsInt_ = 1;
    this->digitsFrac_ = 0;
    this->resultFrac_ = 0;
    this->hexDigitsInt_ = this->digitsInt_;
    this->hexDigitsFrac_ = this->digitsFrac_;

    this->negative_ = false;

    for ( int i = 0; i < WORD_BUF_LEN; i++) {
        this->word_buf_[i] = 0;
    }
}

MyDecimal::MyDecimal( const int8_t digitsInt, const int8_t digitsFrac, bool negative)
{
    this->digitsInt_ = digitsInt;
    this->digitsFrac_ = digitsFrac;
    this->resultFrac_ = digitsFrac;
    this->hexDigitsInt_ = this->digitsInt_;
    this->hexDigitsFrac_ = this->digitsFrac_;
    this->negative_ = negative;

    for ( int i = 0; i < WORD_BUF_LEN; i++) {
        this->word_buf_[i] = 0;
    }
}

MyDecimal::MyDecimal( const MyDecimal & other)
{
    this->digitsInt_ = other.digitsInt_;
    this->digitsFrac_ = other.digitsFrac_;
    this->resultFrac_ = other.resultFrac_;
    this->hexDigitsInt_ = other.hexDigitsInt_;
    this->hexDigitsFrac_ = other.hexDigitsFrac_;
    this->negative_ = other.negative_;

    for ( int i = 0; i < WORD_BUF_LEN; i++) {
        this->word_buf_[i] = other.word_buf_[i];
    }
}

MyDecimal & MyDecimal::operator = ( const MyDecimal & other)
{
    if ( this == &other) {
        return *this;
    }

    this->digitsInt_ = other.digitsInt_;
    this->digitsFrac_ = other.digitsFrac_;
    this->resultFrac_ = other.resultFrac_;
    this->hexDigitsInt_ = other.hexDigitsInt_;
    this->hexDigitsFrac_ = other.hexDigitsFrac_;
    this->negative_ = other.negative_;

    for ( int i = 0; i < WORD_BUF_LEN; i++) {
        this->word_buf_[i] = other.word_buf_[i];
    }

    return *this;
}

MyDecimal::~MyDecimal()
{

}

bool MyDecimal::GetNegative() const
{
    return this->negative_;
}

void MyDecimal::SetNegative(bool negative)
{
    this->negative_ = negative;
}

int8_t MyDecimal::GetDigitsFrac() const
{
    return this->digitsFrac_;
}

void MyDecimal::SetDigitsFrac(int8_t digitsFrac)
{
    this->digitsFrac_ = digitsFrac;
}

int8_t MyDecimal::GetDigitsInt() const
{
    return this->digitsInt_;
}

void MyDecimal::SetDigitsInt(int8_t digitsInt)
{
    this->digitsInt_ = digitsInt;
}

int8_t MyDecimal::GetResultFrac() const
{
    return this->resultFrac_;
}

void MyDecimal::SetResultFrac(int8_t resultFrac)
{
    this->resultFrac_ = resultFrac;
}

int8_t MyDecimal::GetHexDigitsFrac() const
{
    return this->hexDigitsFrac_;
}

void MyDecimal::SetHexDigitsFrac(const int8_t hexDigitsFrac)
{
    this->hexDigitsFrac_ = hexDigitsFrac;
}

int8_t MyDecimal::GetHexDigitsInt() const
{
    return this->hexDigitsInt_;
}

void MyDecimal::SetHexDigitsInt(const int8_t hexDigitsInt)
{
    this->hexDigitsInt_ = hexDigitsInt;
}

int32_t MyDecimal::GetWordDigit(const int i) const
{
    assert( i>=0 && i < WORD_BUF_LEN );

    return this->word_buf_[i];
}

void MyDecimal::SetWordDigit(const int i, const int32_t val)
{
    assert( i>=0 && i < WORD_BUF_LEN );
    this->word_buf_[i] = val;
    return ;
}

MyDecimal* MyDecimal::Clone() const
{
    return new MyDecimal(*this);
}

void MyDecimal::Swap(MyDecimal & other)
{
    std::swap(this->digitsInt_, other.digitsInt_);
    std::swap(this->digitsFrac_, other.digitsFrac_);
    std::swap(this->resultFrac_, other.resultFrac_);
    std::swap(this->hexDigitsInt_, other.hexDigitsInt_);
    std::swap(this->hexDigitsFrac_, other.hexDigitsFrac_);
    std::swap(this->negative_, other.negative_);
    std::swap(this->word_buf_, other.word_buf_);
}

std::string MyDecimal::String()
{
    MyDecimal tmp(*this);
    int32_t error;
    tmp.Round(&tmp, static_cast<int32_t>(tmp.resultFrac_), ModeHalfEven, error);
    return tmp.ToString();
}

int32_t MyDecimal::StringSize() const
{
    return this->digitsInt_ + this->digitsFrac_ + 3;
}

void MyDecimal::RemoveLeadingZeros( int32_t & wordIdx, int32_t & digitsInt) const
{
    digitsInt = static_cast<int32_t>(this->digitsInt_);
    int32_t i = ( ( digitsInt- 1) % DIGITS_PER_WORD) + 1;

    while ( digitsInt > 0 && this->word_buf_[wordIdx] == 0 ) {
        digitsInt -= i;
        i = DIGITS_PER_WORD;
        wordIdx++;
    }

    if ( digitsInt > 0) {
        digitsInt -= CountLeadingZeroes((digitsInt - 1)%DIGITS_PER_WORD, this->word_buf_[wordIdx]);
    } else {
        digitsInt = 0;
    }

    return ;
}

void MyDecimal::RemoveTrailingZeros(int32_t & lastWordIdx, int32_t & digitsFrac) const
{
    digitsFrac = static_cast<int32_t>(this->digitsFrac_);

    int32_t i = ( (digitsFrac - 1) % DIGITS_PER_WORD ) + 1;

    lastWordIdx = DigitsToWords( this->digitsInt_) + DigitsToWords(this->digitsFrac_);

    while ( digitsFrac > 0 && this->word_buf_[lastWordIdx - 1] == 0) {
        digitsFrac -= i;
        i = DIGITS_PER_WORD;
        lastWordIdx--;
    }

    if ( digitsFrac > 0 ) {
        digitsFrac -= CountTrailingZeroes(DIGITS_PER_WORD - ((digitsFrac -1 )%DIGITS_PER_WORD), this->word_buf_[lastWordIdx - 1]);
    } else {
        digitsFrac = 0;
    }

    return ;
}

std::string MyDecimal::ToString() const
{
    std::string str;
    str.reserve( this->StringSize());

    int32_t digitsFrac = this->digitsFrac_;
    int32_t wordStartIdx = 0, digitsInt = 0;
    RemoveLeadingZeros( wordStartIdx, digitsInt);
    if ( (digitsInt + digitsFrac) == 0) {
        digitsInt = 1;
        wordStartIdx = 0;
    }

    int32_t digitsIntLen = digitsInt;
    if ( digitsIntLen == 0 ) {
        digitsIntLen = 1;
    }

    int32_t digitsFracLen = digitsFrac;
    int32_t length = digitsIntLen + digitsFracLen;
    if ( this->negative_ ) {
        length++;
    }

    if ( digitsFrac > 0) {
        length++;
    }

    str.resize(length, 0);

    int32_t strIdx = 0;

    if ( this->negative_ ) {
        str[strIdx] = '-';
        strIdx++;
    }

    int32_t fill = 0;

    if ( digitsFrac > 0 ) {
        int32_t fracIdx = strIdx + digitsIntLen;
        fill = digitsFracLen - digitsFrac;
        int32_t wordIdx = wordStartIdx + DigitsToWords(digitsInt);

        str[fracIdx] = '.';
        fracIdx++;

        for ( ; digitsFrac > 0; digitsFrac -= DIGITS_PER_WORD ) {
            int32_t x = this->word_buf_[wordIdx];
            wordIdx++;

            for ( int32_t i = MyMin(digitsFrac, DIGITS_PER_WORD); i > 0; i--) {
                int32_t y = x / DIG_MASK;
                str[fracIdx] = static_cast<uint8_t>(y) + '0';
                fracIdx++;

                x -= y * DIG_MASK;
                x *= 10;
            }
        }

        for ( ; fill > 0; fill-- ) {
            str[fracIdx] = '0';
            fracIdx++;
        }
    }

    fill = digitsIntLen - digitsInt;
    if ( digitsInt == 0 ) {
        fill--;
    }

    for ( ; fill > 0; fill-- ) {
        str[strIdx] = '0';
        strIdx++;
    }

    if (digitsInt > 0) {
        strIdx += digitsInt;
        int32_t wordIdx = wordStartIdx + DigitsToWords(digitsInt);
        for ( ; digitsInt > 0; digitsInt -= DIGITS_PER_WORD) {
            wordIdx--;
            int32_t x = this->word_buf_[wordIdx];
            for ( int32_t i = MyMin( digitsInt, DIGITS_PER_WORD); i > 0; i-- ) {
                int32_t y = x / 10;
                strIdx--;
                str[strIdx] = '0' + static_cast<uint8_t>(x-y*10);
                x = y;
            }
        }
    } else {
        str[strIdx] = '0';
    }

    return str;
}

int32_t MyDecimal::FromString( const std::string & str)
{
    std::string tmpStr = str;
    int32_t error = E_DEC_OK;

    for ( size_t i = 0; i < str.length(); i++ ) {
        if ( !IsSpace(str[i])) {
            tmpStr = str.substr(i);
            break;
        }
    }

    if ( tmpStr.length() == 0) {
        // add zero MyDecimal
        this->ResetZero();
        error = E_DEC_BAD_NUM;
        return error;
    }

    switch ( tmpStr[0] ) {
        case '-' : {
            this->negative_ = true;
            tmpStr = tmpStr.substr(1);
            break;
        }
        case '+' : {
            tmpStr = tmpStr.substr(1);
            break;
        }
    }

    uint32_t strIdx = 0;
    while ( strIdx < tmpStr.length() && IsDigit(tmpStr[strIdx])) {
        strIdx++;
    }

    uint32_t digitsInt = strIdx;
    int32_t digitsFrac = 0;
    uint32_t endIdx = 0;

    if ( strIdx < tmpStr.length() && tmpStr[strIdx] == '.' ) {
        endIdx = strIdx + 1;

        while ( endIdx < tmpStr.length() && IsDigit(tmpStr[endIdx])) {
            endIdx++;
        }
        digitsFrac = endIdx - strIdx - 1;
    } else {
        digitsFrac = 0;
        endIdx = strIdx;
    }

    if ( digitsInt + digitsFrac == 0) {
        this->ResetZero();
        error = E_DEC_BAD_NUM;
        return error;
    }

    int32_t wordsInt = DigitsToWords(digitsInt);
    int32_t wordsFrac = DigitsToWords(digitsFrac);
    FixWordCntError(wordsInt, wordsFrac, error);
    if ( error != E_DEC_OK) {
        digitsFrac = wordsFrac * DIGITS_PER_WORD;
        if ( error == E_DEC_OVERFLOW ) {
            digitsInt = wordsInt * DIGITS_PER_WORD;
        }
    }

    this->digitsInt_ = static_cast<int8_t>(digitsInt);
    this->digitsFrac_ = static_cast<int8_t>(digitsFrac);

    int32_t wordIdx = wordsInt;
    int32_t strIdxTmp = strIdx;

    int32_t word = 0;
    int32_t innerIdx = 0;

    while ( digitsInt > 0) {
        digitsInt--;
        strIdx--;
        word += static_cast<int32_t>(tmpStr[strIdx] - '0') * powers10[innerIdx];
        innerIdx++;

        if ( innerIdx == DIGITS_PER_WORD ) {
            wordIdx--;
            this->word_buf_[wordIdx] = word;
            word = 0;
            innerIdx = 0;
        }
    }

    if ( innerIdx != 0 ) {
        wordIdx--;
        this->word_buf_[wordIdx] = word;
    }

    wordIdx = wordsInt;
    strIdx = strIdxTmp;
    word = 0;
    innerIdx = 0;

    while( digitsFrac > 0 ) {
        digitsFrac--;
        strIdx++;
        word = static_cast<int32_t>(tmpStr[strIdx] - '0') + word * 10;
        innerIdx++;

        if ( innerIdx == DIGITS_PER_WORD ) {
            this->word_buf_[wordIdx] = word;
            wordIdx++;
            word = 0;
            innerIdx = 0;
        }
    }

    if ( innerIdx != 0 ) {
        this->word_buf_[wordIdx] = word * powers10[ DIGITS_PER_WORD - innerIdx ];
    }

    if ( endIdx + 1 <=  tmpStr.length() && ( tmpStr[endIdx] == 'e' || tmpStr[endIdx] == 'E') ) {
        int64_t exponent = 0;
        error = StrToInt(tmpStr.substr(endIdx+1), exponent);

        if ( error != E_DEC_OK ) {
            if ( error != E_DEC_TRUNCATED ) {
                this->ResetZero();
            }
        }

        if ( exponent > INT32_MAX / 2) {
            auto negative  = this->negative_;
            this->digitsInt_ = WORD_BUF_LEN * DIGITS_PER_WORD;
            this->digitsFrac_ = 0;
            this->ResetMax();
            this->negative_ = negative;
            error = E_DEC_OVERFLOW;
        }

        if ( exponent < INT32_MIN / 2 && error != E_DEC_OVERFLOW ) {
            this->ResetZero();
        }

        if ( error != E_DEC_OVERFLOW ) {
            int32_t shift_error = this->Shift(exponent);
            if ( shift_error != E_DEC_OK ) {
                if ( shift_error == E_DEC_OVERFLOW ) {
                    auto negative  = this->negative_;
                    this->digitsInt_ = WORD_BUF_LEN * DIGITS_PER_WORD;
                    this->digitsFrac_ = 0;
                    this->ResetMax();
                    this->negative_ = negative;
                }
                error = shift_error;
            }
        }
    }

    bool allZero = true;
    for ( int i = 0; i < WORD_BUF_LEN; i++ ) {
        if ( this->word_buf_[i] != 0 ) {
            allZero = false;
            break;
        }
    }

    if ( allZero ) {
        this->negative_ = false;
    }

    this->resultFrac_ = this->digitsFrac_;
    this->hexDigitsInt_ = this->digitsInt_;
    this->hexDigitsFrac_ = this->digitsFrac_;

    return error;
}


int32_t MyDecimal::Shift( int32_t shift)
{
    int32_t error = E_DEC_OK;
    if ( shift == 0 ) {
        return error;
    }

    int32_t digitBegin = 0;
    int32_t digitEnd = 0;
    int32_t point = DigitsToWords(this->digitsInt_) * DIGITS_PER_WORD;
    int32_t newPoint = point + shift;
    int32_t digitsInt = 0, digitsFrac = 0;
    int32_t newFront = 0;

    this->DigitBounds(digitBegin, digitEnd);
    if ( digitBegin == digitEnd ) {
        this->digitsInt_ = 0;
        this->digitsFrac_ = 0;
        this->resultFrac_ = 0;
        this->ResetZero();
        return error;
    }

    digitsInt = newPoint - digitBegin;
    if ( digitsInt < 0 ) {
        digitsInt = 0;
    }

    digitsFrac = digitEnd - newPoint;
    if ( digitsFrac < 0) {
        digitsFrac = 0;
    }

    int32_t wordsInt = DigitsToWords(digitsInt);
    int32_t wordsFrac = DigitsToWords(digitsFrac);
    int32_t newLen = wordsInt + wordsFrac;

    if ( newLen > WORD_BUF_LEN ) {
        int32_t lack = newLen - WORD_BUF_LEN;
        if ( wordsFrac < lack ) {
            error = E_DEC_OVERFLOW;
            return error;
        }

        error = E_DEC_TRUNCATED;
        wordsFrac -= lack;

        int32_t diff = digitsFrac - wordsFrac * DIGITS_PER_WORD;
        this->Round(this, digitEnd - point - diff, ModeHalfEven, error);
        if ( error != E_DEC_OK ) {
            return error;
        }

        digitEnd -= diff;
        digitsFrac = wordsFrac * DIGITS_PER_WORD;

        if ( digitEnd <= digitBegin ) {
            this->digitsInt_ = 0;
            this->digitsFrac_ = 0;
            this->resultFrac_ = 0;
            this->ResetZero();
            error = E_DEC_TRUNCATED;
            return error;
        }
    }

    if ( (shift % DIGITS_PER_WORD) != 0 ) {
        int32_t lMiniShift = 0, rMiniShift = 0, miniShift = 0;
        bool doLeft;

        if ( shift > 0) {
            lMiniShift = shift % DIGITS_PER_WORD;
            rMiniShift = DIGITS_PER_WORD - lMiniShift;
            doLeft = lMiniShift <= digitBegin;
        } else {
            rMiniShift = (-shift) % DIGITS_PER_WORD;
            lMiniShift = DIGITS_PER_WORD - rMiniShift;
            doLeft = (DIGITS_PER_WORD * WORD_BUF_LEN - digitEnd) < rMiniShift;
        }

        if (doLeft) {
            this->DoMiniLeftShift(lMiniShift, digitBegin, digitEnd);
            miniShift = -lMiniShift;
        } else {
            this->DoMiniRightShift(rMiniShift, digitBegin, digitEnd);
            miniShift = rMiniShift;
        }

        newPoint += miniShift;

        if ( (shift+miniShift==0) && ((newPoint-digitsInt)<DIGITS_PER_WORD)) {
            this->digitsInt_ = static_cast<int8_t>(digitsInt);
            this->digitsFrac_ = static_cast<int8_t>(digitsFrac);
            return error;
        }

        digitBegin += miniShift;
        digitEnd += miniShift;
    }

    newFront = newPoint - digitsInt;

    if ( newFront >= DIGITS_PER_WORD || newFront < 0 ) {
        int32_t wordShift = 0;

        if (newFront > 0) {
            wordShift = newFront / DIGITS_PER_WORD;
            int32_t to = digitBegin / DIGITS_PER_WORD - wordShift;
            int32_t barier = (digitEnd-1) / DIGITS_PER_WORD - wordShift;

            for ( ; to <= barier; to++ ) {
                this->word_buf_[to] = this->word_buf_[to+wordShift];
            }

            for ( barier += wordShift; to < barier; to++) {
                this->word_buf_[to] = 0;
            }
            wordShift = -wordShift;

        } else {
            wordShift = (1-newFront)/DIGITS_PER_WORD;
            int32_t to = (digitEnd-1)/DIGITS_PER_WORD + wordShift;
            int32_t barier = digitBegin/DIGITS_PER_WORD + wordShift;

            for ( ; to >= barier; to--) {
                this->word_buf_[to] = this->word_buf_[to-wordShift];
            }

            for (barier -= wordShift; to >= barier; to--) {
                this->word_buf_[to] = 0;
            }
        }

        int32_t digitShift = wordShift * DIGITS_PER_WORD;
        digitBegin += digitShift;
        digitEnd += digitShift;
        newPoint += digitShift;
    }

    int32_t wordIdxBegin = digitBegin / DIGITS_PER_WORD;
    int32_t wordIdxEnd = (digitEnd - 1) / DIGITS_PER_WORD;
    int32_t wordIdxNewPoint = 0;

    if ( newPoint != 0 ) {
        wordIdxNewPoint = (newPoint-1)/DIGITS_PER_WORD;
    }

    if ( wordIdxNewPoint > wordIdxEnd) {
        while ( wordIdxNewPoint > wordIdxEnd) {
            this->word_buf_[wordIdxNewPoint] = 0;
            wordIdxNewPoint--;
        }
    } else {
        for ( ; wordIdxNewPoint < wordIdxBegin; wordIdxNewPoint++ ) {
            this->word_buf_[wordIdxNewPoint] = 0;
        }
    }

    this->digitsInt_ = static_cast<int8_t>(digitsInt);
    this->digitsFrac_ = static_cast<int8_t>(digitsFrac);

    return error;
}

void MyDecimal::DigitBounds( int32_t & start, int32_t & end)
{
    int32_t i = 0;
    int32_t bufBeg = 0;
    int32_t bufLen = DigitsToWords(this->digitsInt_) + DigitsToWords(this->digitsFrac_);
    int32_t bufEnd = bufLen - 1;

    while ( bufBeg < bufLen && this->word_buf_[bufBeg] == 0 ) {
        bufBeg++;
    }

    if ( bufBeg >= bufLen ) {
        start = 0;
        end = 0;
        return ;
    }

    if ( bufBeg == 0 && this->digitsInt_ > 0) {
        i = (static_cast<int32_t>(this->digitsInt_) - 1 ) % DIGITS_PER_WORD;
        start = DIGITS_PER_WORD - i - 1;
    } else {
        i = DIGITS_PER_WORD - 1;
        start = bufBeg * DIGITS_PER_WORD;
    }

    if ( bufBeg < bufLen ) {
        start += CountLeadingZeroes(i, this->word_buf_[bufBeg]);
    }

    if ( bufEnd > bufBeg && this->word_buf_[bufEnd] == 0 ) {
        bufEnd--;
    }

    if ( bufEnd == bufLen - 1 && this->digitsFrac_ > 0 ) {
        i = (static_cast<int32_t>(this->digitsFrac_) - 1) % DIGITS_PER_WORD + 1;
        end = bufEnd * DIGITS_PER_WORD + i;
        i = DIGITS_PER_WORD - i + 1;
    } else {
        end = ( bufEnd + 1 ) * DIGITS_PER_WORD;
        i = 1;
    }

    end -= CountTrailingZeroes(i, this->word_buf_[bufEnd]);

    return ;
}

void MyDecimal::DoMiniLeftShift( int32_t shift, int32_t beg, int32_t end)
{
    int32_t bufFrom = beg / DIGITS_PER_WORD;
    int32_t bufEnd = (end - 1) / DIGITS_PER_WORD;
    int32_t cShift = DIGITS_PER_WORD - shift;

    if ( beg % DIGITS_PER_WORD < shift ) {
        this->word_buf_[bufFrom-1] = this->word_buf_[bufFrom] / powers10[cShift];
    }

    while( bufFrom < bufEnd ) {
        this->word_buf_[bufFrom] = ( this->word_buf_[bufFrom] % powers10[cShift]) * powers10[shift]
                + this->word_buf_[bufFrom + 1]/powers10[cShift];
        bufFrom++;
    }

    this->word_buf_[bufFrom] = (this->word_buf_[bufFrom] % powers10[cShift]) * powers10[shift];
}

void MyDecimal::DoMiniRightShift( int32_t shift, int32_t beg, int32_t end)
{
    int32_t bufFrom = ( end - 1 ) / DIGITS_PER_WORD;
    int32_t bufEnd = beg / DIGITS_PER_WORD;
    int32_t cShift = DIGITS_PER_WORD - shift;

    if ( DIGITS_PER_WORD - ((end-1) % DIGITS_PER_WORD+1) < shift ) {
        this->word_buf_[bufFrom+1] = (this->word_buf_[bufFrom] % powers10[shift]) * powers10[cShift];
    }

    while (bufFrom > bufEnd) {
        this->word_buf_[bufFrom] = this->word_buf_[bufFrom]/powers10[shift]
                + (this->word_buf_[bufFrom-1]%powers10[shift]) * powers10[cShift] ;
        bufFrom--;
    }
    this->word_buf_[bufFrom] = this->word_buf_[bufFrom] / powers10[shift];
}

void MyDecimal::Round( MyDecimal * to, int32_t frac, RoundMode roundMode, int32_t & error)
{
    int32_t wordsFracTo = (frac + 1 ) / DIGITS_PER_WORD;
    if  ( frac > 0 ) {
        wordsFracTo = DigitsToWords(frac);
    }

    int32_t wordsFrac = DigitsToWords(static_cast<int32_t>(this->digitsFrac_));
    int32_t wordsInt = DigitsToWords(static_cast<int32_t>(this->digitsInt_));

    /* TODO - fix this code as it won't work for CEILING mode.*/

    if ( (wordsInt + wordsFracTo) > WORD_BUF_LEN) {
        wordsFracTo = WORD_BUF_LEN - wordsInt;
        frac = wordsFracTo * DIGITS_PER_WORD;
        error = E_DEC_TRUNCATED;
    }

    if ( this->digitsInt_ + frac < 0) {
        to->ResetZero();
        error = E_DEC_OK;
        return ;
    }

    if ( to != this) {
        for ( int i = 0; i < WORD_BUF_LEN; i++) {
            to->word_buf_[i] = this->word_buf_[i];
        }
        to->negative_ = this->negative_;
        to->digitsInt_ = MyMin( wordsInt, static_cast<int32_t>(WORD_BUF_LEN)) * DIGITS_PER_WORD;
    }

    if ( wordsFracTo > wordsFrac ) {
        int32_t idx = wordsInt + wordsFrac;
        while ( wordsFracTo > wordsFrac ) {
            wordsFracTo--;
            to->word_buf_[idx] = 0;
            idx++;
        }
        to->digitsFrac_ = static_cast<int8_t>(frac);
        to->resultFrac_ = to->digitsFrac_;
        error = E_DEC_OK;
        return ;
    }

    if ( frac >= this->digitsFrac_ ) {
        to->digitsFrac_ = static_cast<int8_t>(frac);
        to->resultFrac_ = to->digitsFrac_;
        error = E_DEC_OK;
        return ;
    }

    int32_t toIdx = wordsInt + wordsFracTo - 1;
    if ( frac == wordsFracTo * DIGITS_PER_WORD ) {
        bool doInc = false;

        switch(roundMode) {
            // Notice: No support for ceiling mode now.
            case ModeCeiling: {

                int32_t idx = toIdx + (wordsFrac - wordsFracTo);
                while ( idx > toIdx ) {
                    if ( this->word_buf_[idx] != 0 ) {
                        doInc = true;
                        break;
                    }
                    idx--;
                }
                break;
            }
            case ModeHalfEven: {
                int32_t digAfterScale = this->word_buf_[toIdx + 1] / DIG_MASK;
                doInc = (digAfterScale > 5) || (digAfterScale == 5 );
                break;
            }
            case ModeTruncate: {
                doInc = false;
                break;
            }
            default: {
                // never to here.
            }
        }

        if ( doInc ) {
            if ( toIdx >= 0 ) {
                to->word_buf_[toIdx]++;
            } else {
                toIdx++;
                to->word_buf_[toIdx] = WORD_BASE;
            }
        } else if ((wordsInt + wordsFracTo)==0) {
            to->ResetZero();
            error = E_DEC_OK;
            return ;
        }

    } else {
        /*TODO - fix this code as it won't word for CEILING mode. */
        int32_t pos = wordsFracTo * DIGITS_PER_WORD - frac - 1;
        int32_t shiftedNumber = to->word_buf_[toIdx] / powers10[pos];
        int32_t digAfterScale = shiftedNumber % 10;

        //if ((digAfterScale > 5) || ((roundMode == ModeHalfEven)&&(digAfterScale == 5 ) )) {
        if (((roundMode == ModeHalfEven)&&(digAfterScale >= 5 )) || ( roundMode == ModeCeiling )) {
            shiftedNumber += 10;
        }

        to->word_buf_[toIdx] = powers10[pos] * (shiftedNumber - digAfterScale);
    }

    if ( wordsFracTo < wordsFrac ) {
        int32_t idx = wordsInt + wordsFracTo;
        if ( frac == 0 && wordsInt == 0 ) {
            idx = 1;
        }
        while ( idx < WORD_BUF_LEN) {
            to->word_buf_[idx] = 0;
            idx++;
        }
    }

    int32_t carry = 0;
    if ( to->word_buf_[toIdx] >= WORD_BASE) {
        carry = 1;
        to->word_buf_[toIdx] -= WORD_BASE;
        while ((carry == 1) && (toIdx > 0)) {
            toIdx--;
            to->word_buf_[toIdx] = add(to->word_buf_[toIdx], 0, carry);
        }

        if ( carry > 0 ) {
            if ( wordsInt + wordsFracTo >= WORD_BUF_LEN) {
                wordsFracTo--;
                frac = wordsFracTo * DIGITS_PER_WORD;
                error = E_DEC_TRUNCATED;
            }

            toIdx = wordsInt + MyMax(wordsFracTo, 0);
            for ( ; toIdx > 0; toIdx--) {
                if ( toIdx < WORD_BUF_LEN) {
                    to->word_buf_[toIdx] = to->word_buf_[toIdx-1];
                } else {
                    error = E_DEC_OVERFLOW;
                }
            }
            to->word_buf_[toIdx] = 1;

            if ( static_cast<int32_t>(to->digitsInt_) < DIGITS_PER_WORD * WORD_BUF_LEN){
                to->digitsInt_++;
            } else {
                error = E_DEC_OVERFLOW;
            }
        }
    } else {
        while(1) {
            if (to->word_buf_[toIdx] != 0 ) {
                break;
            }

            if (toIdx == 0 ) {
                int32_t idx = wordsFracTo + 1;
                to->digitsInt_ = 1;
                to->digitsFrac_ = static_cast<int8_t>(MyMax(frac, 0));
                to->negative_ = false;

                while (toIdx < idx) {
                    to->word_buf_[toIdx] = 0;
                    toIdx++;
                }

                to->resultFrac_ = to->digitsFrac_;
                error = E_DEC_OK;
                return ;
            }
            toIdx--;
        }
    }

    int32_t firstDig = to->digitsInt_ % 9;
    if ( firstDig > 0 && to->word_buf_[toIdx] >= powers10[firstDig]) {
        to->digitsInt_++;
    }

    if ( frac < 0) {
        frac = 0;
    }

    to->digitsFrac_ = static_cast<int8_t>(frac);
    to->resultFrac_ = to->digitsFrac_;
    error = E_DEC_OK;
    return ;
}

MyDecimal& MyDecimal::FromInt( const int64_t val )
{
    uint64_t uVal = 0;
    if ( val < 0) {
        this->negative_ = true;
        uVal = static_cast<uint64_t>(-val);
    } else {
        uVal = static_cast<uint64_t>(val);
    }

    return this->FromUint(uVal);
}

MyDecimal& MyDecimal::FromUint( const uint64_t val )
{
    uint64_t x = val;
    int32_t wordIdx = 1;
    while ( x >= WORD_BASE ) {
        wordIdx++;
        x /= WORD_BASE;
    }

    this->digitsFrac_ = 0;
    this->digitsInt_ = static_cast<int8_t>(wordIdx * DIGITS_PER_WORD);
    x = val;

    while (wordIdx > 0) {
        wordIdx--;
        int64_t y = x / WORD_BASE;
        this->word_buf_[wordIdx] = static_cast<int32_t>(x - y * WORD_BASE);
        x = y;
    }

    this->hexDigitsInt_ = this->digitsInt_;
    this->hexDigitsFrac_ = this->digitsFrac_;

    return *this;
}

void MyDecimal::ToInt( int64_t &  val, int32_t & error) const
{
    int64_t x = 0;
    int32_t wordIdx = 0;

    for ( int64_t i = this->digitsInt_; i > 0; i -= DIGITS_PER_WORD ) {
        int64_t y = x;

        x = x * WORD_BASE - static_cast<int64_t>(this->word_buf_[wordIdx]);
        wordIdx++;

        if ( y < INT64_MIN/WORD_BASE || x > y ) {
            if ( this->negative_ ) {
                val =INT64_MIN;
                error = E_DEC_OVERFLOW;
                return ;
            }

            val = INT64_MAX;
            error = E_DEC_OVERFLOW;
            return ;
        }
    }

    if ( !this->negative_ && x == INT64_MIN ) {
        val = INT64_MAX;
        error = E_DEC_OVERFLOW;
        return ;
    }

    if ( !this->negative_) {
        x = -x;
    }

    for ( int32_t i = this->digitsFrac_; i > 0; i -= DIGITS_PER_WORD ) {
        if ( this->word_buf_[wordIdx] != 0 ) {
            val = x;
            error = E_DEC_TRUNCATED;
            return ;
        }
        wordIdx++;
    }
    val = x;
    error = E_DEC_OK;
    return ;
}

void MyDecimal::ToUint( uint64_t & val, int32_t & error) const
{
    if ( this->negative_ ) {
        val = 0;
        error = E_DEC_OVERFLOW;
        return ;
    }

    uint64_t x = 0;
    int32_t wordIdx = 0;

    for ( int32_t i = this->digitsInt_; i > 0; i -= DIGITS_PER_WORD ) {
        uint64_t y = x;
        x = x * WORD_BASE + static_cast<uint64_t>(this->word_buf_[wordIdx]);
        wordIdx++;
        if ( y > UINT64_MAX/WORD_BASE || x < y) {
            val = UINT64_MAX;
            error = E_DEC_OVERFLOW;
            return ;
        }
    }

    for (  int32_t i = this->digitsFrac_; i > 0; i -= DIGITS_PER_WORD) {
        if ( this->word_buf_[wordIdx] != 0 ) {
            val = x;
            error = E_DEC_TRUNCATED;
            return ;
        }
        wordIdx++;
    }

    val = x;
    error = E_DEC_OK;

    return ;
}

MyDecimal& MyDecimal::FromFloat64( double f64 )
{
    std::string str = std::to_string(f64);
    this->FromString(str);
    return *this;
}

void MyDecimal::ToFloat64( double & f, int32_t & error) const
{
    std::string str = this->ToString();

    f = std::stod(str);
    error = E_DEC_OK;
    return ;
}

void MyDecimal::ToBin( const int32_t precision, const int32_t frac, std::string & bin, int32_t & error) const
{
    if ( (precision > DIGITS_PER_WORD * MAX_WORD_BUF_LEN)
        || ( precision < 0)
        || (frac > MAX_DECIMAL_SCALE)
        || (frac < 0) ) {

        bin.clear() ;
        error = E_DEC_BAD_NUM;
        return ;
    }

    int32_t mask = 0;

    if (this->negative_) {
        mask = -1;
    }

    int32_t digitsInt = precision - frac;
    int32_t wordsInt = digitsInt / DIGITS_PER_WORD;
    int32_t leadingDigits = digitsInt - wordsInt * DIGITS_PER_WORD;
    int32_t wordsFrac = frac / DIGITS_PER_WORD;
    int32_t trailingDigits = frac - wordsFrac * DIGITS_PER_WORD;

    int32_t wordsFracFrom = static_cast<int32_t>(this->digitsFrac_) / DIGITS_PER_WORD;
    int32_t trailingDigitsFrom = static_cast<int32_t>(this->digitsFrac_) - wordsFracFrom * DIGITS_PER_WORD;
    int32_t intSize = wordsInt * WORD_SIZE + dig2bytes[leadingDigits];
    int32_t fracSize = wordsFrac * WORD_SIZE + dig2bytes[trailingDigits];
    int32_t fracSizeFrom = wordsFracFrom * WORD_SIZE + dig2bytes[trailingDigitsFrom];
    int32_t originIntSize = intSize;
    int32_t originFracSize = fracSize;

    bin.resize( intSize + fracSize, 0);

    int32_t binIdx = 0;
    int32_t wordIdxFrom = 0, digitsIntFrom = 0;
    this->RemoveLeadingZeros(wordIdxFrom, digitsIntFrom);
    if ( digitsIntFrom + fracSizeFrom == 0) {
        mask = 0;
        digitsInt = 1;
    }

    int32_t wordsIntFrom = digitsIntFrom / DIGITS_PER_WORD;
    int32_t leadingDigitsFrom = digitsIntFrom - wordsIntFrom * DIGITS_PER_WORD;
    int32_t iSizeFrom = wordsIntFrom * WORD_SIZE + dig2bytes[leadingDigitsFrom];

    if ( digitsInt < digitsIntFrom ) {
        wordIdxFrom += wordsIntFrom - wordsInt;
        if ( leadingDigitsFrom > 0 ) {
            wordIdxFrom++;
        }
        if ( leadingDigits > 0 ) {
            wordIdxFrom--;
        }

        wordsIntFrom = wordsInt;
        leadingDigitsFrom = leadingDigits;

        error = E_DEC_OVERFLOW;
    } else if ( intSize > iSizeFrom ) {
        while ( intSize > iSizeFrom ) {
            intSize--;
            bin[binIdx] = static_cast<uint8_t>(mask);
            binIdx++;
        }
    }

    if ( fracSize < fracSizeFrom ) {
        wordsFracFrom = wordsFrac;
        trailingDigitsFrom  = trailingDigits;
        error = E_DEC_TRUNCATED;
    } else if ( fracSize > fracSizeFrom && trailingDigitsFrom > 0) {
        if ( wordsFrac == wordsFracFrom ) {
            trailingDigitsFrom = trailingDigits;
            fracSize = fracSizeFrom;
        } else {
            wordsFracFrom++;
            trailingDigitsFrom = 0;
        }
    }

    if ( leadingDigitsFrom > 0 ) {
        int32_t i = dig2bytes[leadingDigitsFrom];
        int32_t x = ( this->word_buf_[wordIdxFrom] % powers10[leadingDigitsFrom]) ^ mask;
        wordIdxFrom++;
        std::string tmp( i, 0);
        WriteWord(tmp, x, i);
        for (const auto c : tmp) {
            bin[binIdx] = c;
            binIdx++;
        }
    }

    for ( int32_t stop = wordIdxFrom + wordsIntFrom + wordsFracFrom; wordIdxFrom < stop ; ) {
        int32_t x = this->word_buf_[wordIdxFrom] ^ mask;
        wordIdxFrom++;
        std::string  tmp(WORD_SIZE, 0);
        WriteWord(tmp, x, WORD_SIZE);
        for (const auto c : tmp) {
            bin[binIdx] = c;
            binIdx++;
        }
    }

    if ( trailingDigitsFrom > 0 ) {
        int32_t x = 0;
        int32_t i = dig2bytes[trailingDigitsFrom];
        int32_t lim = trailingDigits;
        if ( wordsFracFrom < wordsFrac ) {
            lim = DIGITS_PER_WORD;
        }

        while ( trailingDigitsFrom < lim && dig2bytes[trailingDigitsFrom] == i ) {
            trailingDigitsFrom++;
        }

        x = ( this->word_buf_[wordIdxFrom] / powers10[ DIGITS_PER_WORD - trailingDigitsFrom ]) ^ mask;
        std::string tmp(i, 0);
        WriteWord(tmp, x, i);
        for (const auto c : tmp) {
            bin[binIdx] = c;
            binIdx++;
        }
    }

    if ( fracSize > fracSizeFrom ) {
        int32_t binIdxEnd = originIntSize + originFracSize;

        while((fracSize>fracSizeFrom) && (binIdx<binIdxEnd)) {
            fracSize--;
            bin[binIdx] = static_cast<uint8_t>(mask);
            binIdx++;
        }
    }
    bin[0] ^= 0x80;

    return ;
}

void MyDecimal::ToHashKey(std::string & hash_key, int32_t & error) const
{
    int32_t wordIdx = 0, digitsInt = 0;
    this->RemoveLeadingZeros( wordIdx, digitsInt);

    int32_t lastWordIdx, digitsFrac;
    this->RemoveTrailingZeros( lastWordIdx, digitsFrac);

    int32_t prec = digitsInt + digitsFrac;
    if ( prec == 0 ) {
        prec = 1;
    }

    this->ToBin(prec, digitsFrac, hash_key, error);
    if ( error == E_DEC_TRUNCATED ) {
        error = E_DEC_OK;
    }

    return ;
}

void MyDecimal::PrecisionAndFrac( int32_t & precision, int32_t & frac) const
{
    frac = static_cast<int32_t>(this->digitsFrac_);
    int32_t wordIdx = 0, digitsInt = 0;
    this->RemoveLeadingZeros(wordIdx, digitsInt);
    precision = digitsInt + frac;
    if ( precision == 0 ) {
        precision = 1;
    }

    return ;
}

bool MyDecimal::IsZero() const
{
    bool isZero = true;

    for( int i = 0; i < WORD_BUF_LEN; i++) {
        if (this->word_buf_[i] != 0) {
            isZero = false;
            break;
        }
    }

    return isZero;
}

void MyDecimal::FromBin(std::string bin, const int32_t precision, const int32_t frac, int32_t & binSize, int32_t & error)
{
    if ( bin.length() == 0 ) {
        this->ResetZero();
        binSize = 0;
        error = E_DEC_BAD_NUM;
        return ;
    }

    int32_t digitsInt = precision - frac;
    int32_t wordsInt = digitsInt / DIGITS_PER_WORD;
    int32_t leadingDigits = digitsInt - wordsInt * DIGITS_PER_WORD;
    int32_t wordsFrac = frac / DIGITS_PER_WORD;
    int32_t trailingDigits = frac - wordsFrac * DIGITS_PER_WORD;
    int32_t wordsIntTo = wordsInt;

    if (leadingDigits > 0 ) {
        wordsIntTo++;
    }

    int32_t wordsFracTo = wordsFrac;
    if ( trailingDigits > 0 ) {
        wordsFracTo++;
    }

    int32_t binIdx = 0;
    int32_t mask = -1;

    if ( (bin[binIdx]&0x80) > 0 ) {
        mask = 0;
    }

    binSize = DecimalBinSize(precision, frac);
    std::string dCopy(bin);
    dCopy[0] ^= 0x80;
    bin = dCopy;

    int32_t oldWordsIntTo = wordsIntTo;
    FixWordCntError(wordsIntTo, wordsFracTo, error);
    if ( error != E_DEC_OK ) {
        if ( wordsIntTo < oldWordsIntTo ) {
            binIdx += dig2bytes[leadingDigits] + (wordsInt - wordsIntTo) * WORD_SIZE;
        } else {
            trailingDigits = 0;
            wordsFrac = wordsFracTo;
        }
    }

    this->negative_ = mask != 0;
    this->digitsInt_ = static_cast<int8_t>( wordsInt * DIGITS_PER_WORD + leadingDigits);
    this->digitsFrac_ = static_cast<int8_t>( wordsFrac * DIGITS_PER_WORD + trailingDigits);

    int32_t wordIdx = 0;
    if ( leadingDigits > 0 ) {
        int32_t i = dig2bytes[leadingDigits];
        int32_t x = ReadWord(bin.substr(binIdx, i), i);
        binIdx += i;

        this->word_buf_[wordIdx] = x^mask;

        if ( static_cast<int64_t>(this->word_buf_[wordIdx]) >= static_cast<int64_t>( powers10[leadingDigits+1])) {
            this->ResetZero();
            error = E_DEC_BAD_NUM;
            return ;
        }

        if ( wordIdx > 0 || this->word_buf_[wordIdx] != 0 ) {
            wordIdx++;
        } else {
            this->digitsInt_ -= static_cast<int8_t>(leadingDigits);
        }
    }

    for ( int32_t stop = binIdx + wordsInt * WORD_SIZE; binIdx < stop; binIdx += WORD_SIZE ) {
        this->word_buf_[wordIdx] = ReadWord(bin.substr(binIdx, 4), 4) ^ mask;
        if ( static_cast<uint32_t>(this->word_buf_[wordIdx]) > WORD_MAX) {
            this->ResetZero();
            error = E_DEC_BAD_NUM;
            return ;
        }

        if ( wordIdx > 0 || this->word_buf_[wordIdx] != 0) {
            wordIdx++;
        } else {
            this->digitsInt_ -= DIGITS_PER_WORD;
        }
    }

    for ( int32_t stop = binIdx + wordsFrac * WORD_SIZE;  binIdx < stop; binIdx += WORD_SIZE) {
        this->word_buf_[wordIdx] = ReadWord(bin.substr(binIdx, 4), 4) ^ mask;
        if ( static_cast<uint32_t>(this->word_buf_[wordIdx]) > WORD_MAX) {
            this->ResetZero();
            error = E_DEC_BAD_NUM;
            return ;
        }
        wordIdx++;
    }

    if ( trailingDigits > 0 ) {
        int32_t i = dig2bytes[trailingDigits];
        int32_t x = ReadWord(bin.substr(binIdx, i), i);
        this->word_buf_[wordIdx] = (x ^ mask) * powers10[DIGITS_PER_WORD - trailingDigits];
        if ( static_cast<uint32_t>(this->word_buf_[wordIdx]) > WORD_MAX) {
            this->ResetZero();
            error = E_DEC_BAD_NUM;
            return ;
        }
    }

    if ( this->digitsInt_ == 0 && this->digitsFrac_ == 0 ) {
        this->ResetZero();
    }

    this->resultFrac_ = static_cast<int8_t>(frac);
    error = E_DEC_OK;
    return ;
}

int32_t MyDecimal::Compare( const MyDecimal& to ) const
{
    int32_t cmp = 0;
    int32_t error = 0;
    if (this->negative_ == to.negative_ ) {
        error = DoSub(nullptr, this, &to, cmp);
        return cmp;
    }

    if (this->negative_) {
        return -1;
    }

    return 1;
}

MyDecimal& MyDecimal::ResetZero()
{
    /*
    this->digitsInt_ = 0;
    this->digitsFrac_ = 0;
    this->resultFrac_ = 0;
    */

    for ( int i = 0; i < WORD_BUF_LEN; i++) {
        this->word_buf_[i] = 0;
    }
    return *this;
}

MyDecimal& MyDecimal::ResetMax()
{
    this->negative_ = false;
    int idx = 0;

    int32_t digitsInt = this->digitsInt_;
    if ( digitsInt > 0 ) {
        int32_t firstWordDigits = digitsInt % DIGITS_PER_WORD;
        if ( firstWordDigits > 0) {
             this->word_buf_[idx] = powers10[firstWordDigits] - 1;
             idx++;
        }

        for ( digitsInt /= DIGITS_PER_WORD; digitsInt > 0; digitsInt-- ) {
            this->word_buf_[idx] = WORD_MAX;
            idx++;
        }
    }

    int32_t digitsFrac = this->digitsFrac_;
    if ( digitsFrac > 0) {
        int32_t lastDigits = digitsFrac % DIGITS_PER_WORD;
        for ( digitsFrac /= DIGITS_PER_WORD; digitsFrac > 0; digitsFrac-- ) {
            this->word_buf_[idx] = WORD_MAX;
            idx++;
        }

        if ( lastDigits > 0) {
            this->word_buf_[idx] = FRAC_MAX[lastDigits-1];

        }
    }

    return *this;
}

MyDecimal & MyDecimal::ResetMin()
{
    this->ResetMax().negative_ = true;
    return *this;
}

} // namespace dataytpe
}  // namespace jim
