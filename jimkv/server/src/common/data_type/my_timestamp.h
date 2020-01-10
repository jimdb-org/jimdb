// Copyright (C) 2004-2006 MySQL AB
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
//
// Copyright 2018 TiKV Project Authors.
// Portions Copyright 2019 The JimDB Authors.
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
#include <iostream>
#include <ostream>
#include <vector>

namespace jimkv {
namespace datatype{

class MyDateTime {
public:
    typedef enum {
        MYDATETIME_WARN_NONE = 0,
        MYDATETIME_WARN_TRUNCATED = 1,
        MYDATETIME_WARN_OUT_OF_RANGE = 2,
        MYDATETIME_WARN_INVALID_TIMESTAMP = 4,
        MYDATETIME_WARN_ZERO_DATE = 8,
        MYDATETIME_NOTE_TRUNCATED = 16,
        MYDATETIME_WARN_ZERO_IN_DATE = 32
    } StWarn;
public:
    MyDateTime();
    MyDateTime(const MyDateTime& dt);
    MyDateTime& operator=( const MyDateTime& dt);
    ~MyDateTime();

    MyDateTime& FromDateTime( const uint32_t y, const uint32_t m, const uint32_t d, const uint32_t h, const uint32_t mi, uint32_t s);

    uint32_t GetYear() const;
    void SetYear( const uint32_t y);
    bool CheckYear() const ;

    uint32_t GetMonth() const;
    void SetMonth( const uint32_t m);
    bool CheckMonth() const;

    uint32_t GetDay() const;
    void SetDay( const uint32_t d);
    bool CheckDay() const ;

    uint32_t GetHour() const;
    void SetHour( const uint32_t h);
    bool CheckHour() const ;

    uint32_t GetMinute() const;
    void SetMinute( const uint32_t m);
    bool CheckMinute() const;

    uint32_t GetSecond() const;
    void SetSecond( const uint32_t s);
    bool CheckSecond() const;

    uint64_t GetMicroSecond() const;
    void SetMicroSecond( const uint64_t ms);
    bool CheckMicroSecond() const;

    bool GetNeg() const;
    void SetNeg( bool n);

    int64_t ToPackInt64() const;
    void FromPackInt64(const int64_t dt);

    uint64_t ToNumberUint64() const ;
    bool FromNumberUint64(const uint64_t u, StWarn &st);

    double ToNumberFloat64() const ;
    bool FromNumberFloat64(const double f64, StWarn &st);

    std::string ToNumberString() const;

    std::string ToString() const;
    bool FromString(const std::string& str, StWarn &st);

    void SetZero();
    bool IsLeapYear() const ;
    bool CheckRange() const ;

public:
    bool operator == ( const MyDateTime & other) const;
    bool operator != ( const MyDateTime & other) const;
    bool operator < ( const MyDateTime & other) const;
    bool operator > ( const MyDateTime & other) const;
    bool operator <= ( const MyDateTime & other) const;
    bool operator >= ( const MyDateTime & other) const;

private:
    inline bool IsSpace( uint8_t c) {
        return c == ' ' || c == '\t';
    }
    inline bool IsDigit( uint8_t c) {
        return  '0' <= c && c <= '9';
    }

    inline std::string trim( const std::string & str){

        if ( str.empty()) {
            return "";
        }

        std::string str_tmp = str;

        str_tmp.erase( 0, str_tmp.find_first_not_of(" "));
        str_tmp.erase( str_tmp.find_last_not_of(" ")+1);
        return str_tmp;
    }

    int32_t AdjustYear( const int32_t y );
    int32_t GetFracIndex(const std::string& str);
    bool ParseDateTime(const std::string& format, std::vector<std::string>& seps);
    bool SplitDateTime(const std::string& format, std::vector<std::string>& seps, std::string& spStr);

private:
    friend std::ostream& operator<< ( std::ostream& os, const MyDateTime& dt);

private:
    uint32_t year_;
    uint32_t month_;
    uint32_t day_;
    uint32_t hour_;
    uint32_t minute_;
    uint32_t second_;
    uint64_t micro_second_; // microseconds
    bool neg_;

    static const int32_t days_in_month_[];
    static const uint32_t log10num[];
};

} // namespace dataytpe
}  // namespace jimkv
