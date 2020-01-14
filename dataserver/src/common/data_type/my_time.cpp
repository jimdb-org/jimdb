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

#include "my_time.h"
#include <algorithm>
#include <utility>
#include <sstream>
#include <iomanip>

namespace jim {
namespace datatype{

const std::string my_zero_time = "[-]00:000:00.000000";
// const std::string my_zero_time = "-00:000:00.000000";
// const std::string my_zero_time = "-000:000:00.000000";

#define TIME_MAX_HOUR 838
#define TIME_MAX_MINUTE 59
#define TIME_MAX_SECOND 59
#define TIME_MAX_VALUE (TIME_MAX_HOUR*10000 + TIME_MAX_MINUTE*100 + \
                        TIME_MAX_SECOND)

const uint32_t MyTime::log10num[] = {
        1,
        10,
        100,
        1000,
        10000,
        100000,
        1000000,
        10000000,
        100000000,
        1000000000 };


MyTime::MyTime(){
    this->hour_ = 0;
    this->minute_ = 0;
    this->second_ = 0;
    this->micro_second_ = 0;
    this->neg_ = false;
}

MyTime::MyTime(const MyTime& t){
    this->hour_ = t.hour_;
    this->minute_ = t.minute_;
    this->second_ = t.second_;
    this->micro_second_ = t.micro_second_;
    this->neg_ = t.neg_;
}

MyTime& MyTime::operator=( const MyTime& t){

    if ( this == &t ) {
        return *this;
    }

    this->hour_ = t.hour_;
    this->minute_ = t.minute_;
    this->second_ = t.second_;
    this->micro_second_ = t.micro_second_;
    this->neg_ = t.neg_;
    return *this;
}

MyTime::~MyTime(){
}

MyTime& MyTime::FromTime(const uint32_t h, const uint32_t m, const uint32_t s, bool neg) {
    this->hour_ = h;
    this->minute_ = m;
    this->second_ = s;
    this->neg_ = neg;
    return *this;
}

uint32_t MyTime::GetHour() const{
    return this->hour_;
}

void MyTime::SetHour( const uint32_t h){
    this->hour_ = h;
}

bool MyTime::CheckHour() const{
    return this->hour_ <= 838;
}

uint32_t MyTime::GetMinute() const{
    return this->minute_;
}

void MyTime::SetMinute( const uint32_t m){
    this->minute_ = m;
}

bool MyTime::CheckMinute() const{
    return this->minute_ <= 59;
}

uint32_t MyTime::GetSecond() const{
    return this->second_;
}

void MyTime::SetSecond( const uint32_t s){
    this->second_ = s;
}

bool MyTime::CheckSecond() const{
    return this->second_ < 59;
}

uint64_t MyTime::GetMicroSecond() const{
    return this->micro_second_;
}

void MyTime::SetMicroSecond( const uint64_t ms){
    this->micro_second_ = ms;
}

bool MyTime::CheckMicroSecond() const{
    return this->micro_second_ <= 999999;
}

bool MyTime::GetNeg() const{
    return this->neg_;
}

void MyTime::SetNeg( bool n){
    this->neg_ = n;
}

int64_t MyTime::ToPackInt64() const{

    int64_t t_s =  static_cast<int64_t>(this->hour_) * 3600 +
                static_cast<int64_t>(this->minute_) * 60 +
                static_cast<int64_t>(this->second_);

    int64_t t = t_s * 1000 * 1000 + this->micro_second_;

    if (this->neg_) {
        t = -1 * t;
    }

    return t;
}

void MyTime::FromPackInt64(const int64_t t){
    int64_t tmp = t;
    if (t<0) {
        this->neg_ = true;
        tmp = -t;
    }

    this->micro_second_ = tmp % 1000000;
    int64_t hms = tmp / 1000000;
    int64_t hm = hms / 60;
    this->second_ = hms % 60;
    this->minute_ = hm % 60;
    this->hour_ = hm / 60;
}

std::string MyTime::ToString() const{

    std::ostringstream ost;
    ost << (this->neg_ ? "-" : "")
        << std::setfill('0') << std::setw(2)
        << this->hour_ << ":" << std::setw(2)
        << this->minute_ << ":" <<std::setw(2)
        << this->second_ << "." << std::setw(6)
        << this->micro_second_ ;

    std::string str_ret = ost.str();

    return str_ret;
}

int64_t MyTime::ToNumberInt64() const {

    return  (this->neg_? -1:1) * (this->hour_ * 10000UL + this->minute_ * 100UL + this->second_);
}

bool MyTime::FromDateTimeNumberUint64( const uint64_t u, StWarn & st) {
    uint64_t ui = u;

    bool ret = true;
    const int32_t YY_PART_YEAR = 70;
    uint64_t ymd = 0;
    uint64_t hms = 0;

    st = MYTIME_WARN_NONE;

    if (ui == 0LL || ui >= 10000101000000LL)
    {
        if (ui > 99999999999999LL) /* 9999-99-99 99:99:99 */
        {
            st = MYTIME_WARN_OUT_OF_RANGE;
            ret = false;
            return ret;
        }
        goto ok;
    }
    if (ui < 101)
        goto err;
    if (ui <= (YY_PART_YEAR-1)*10000L+1231L)
    {
        ui = (ui+20000000L)*1000000L;                 /* YYMMDD, year: 2000-2069 */
        goto ok;
    }
    if (ui < (YY_PART_YEAR)*10000L+101L)
        goto err;
    if (ui <= 991231L)
    {
        ui= (ui+19000000L)*1000000L;                 /* YYMMDD, year: 1970-1999 */
        goto ok;
    }
    /*
      Though officially we support DATE values from 1000-01-01 only, one can
      easily insert a value like 1-1-1. So, for consistency reasons such dates
      are allowed when TIME_FUZZY_DATE is set.
    */
    if (ui < 10000101L /*&& !(flags & TIME_FUZZY_DATE)*/)
        goto err;
    if (ui <= 99991231L)
    {
        ui= ui*1000000L;
        goto ok;
    }
    if (ui < 101000000L)
        goto err;


    if (ui <= (YY_PART_YEAR-1)*10000000000LL+1231235959LL)
    {
        ui= ui+20000000000000LL;                   /* YYMMDDHHMMSS, 2000-2069 */
        goto ok;
    }
    if (ui <  YY_PART_YEAR*10000000000LL+ 101000000LL)
        goto err;
    if (ui <= 991231235959LL)
        ui= ui+19000000000000LL;		/* YYMMDDHHMMSS, 1970-1999 */

    ok:
    ymd = ui/1000000LL;
    hms = ui - ymd *1000000LL;

    this->hour_ = hms/10000L;
    this->minute_ = (hms%10000L)/100;
    this->second_ = (hms%10000L)%100;

    ret = true;
    st = MYTIME_WARN_NONE;
    return ret;

    err:
    ret = false;
    st = MYTIME_WARN_TRUNCATED;
    return ret ;
}

bool MyTime::FromNumberInt64(const int64_t u, StWarn &st) {

    int64_t nr = u;
    bool ret = true;

    st = MYTIME_WARN_NONE;

    if (nr > TIME_MAX_VALUE)
    {
        if (nr >= 10000000000LL) /* '0001-00-00 00-00-00' */
        {
            if (!FromDateTimeNumberUint64(nr, st)) {
                ret = false;
            } else {
                ret = true;
                return ret;
            }
        }

        SetMaxTime();
        st = MYTIME_WARN_OUT_OF_RANGE;
        ret = true;
        return ret;
    }
    else if (nr < -TIME_MAX_VALUE)
    {
        SetMaxTime();
        SetNeg(true);
        st = MYTIME_WARN_OUT_OF_RANGE;
        ret = true;
        return ret;
    }

    if (nr < 0) {

        this->neg_ = true;
        nr= -nr;
    }
    if (nr % 100 >= 60 || nr / 100 % 100 >= 60)
    {
        SetZero();
        st = MYTIME_WARN_OUT_OF_RANGE;
        ret = true;
        return ret;
    }

    this->hour_ = static_cast<uint32_t>(nr / 10000);
    this->minute_ = static_cast<uint32_t>((nr / 100) % 100);
    this->second_ = static_cast<uint32_t>(nr%100);
    this->micro_second_ = 0;

    ret = true;
    return ret;
}

double MyTime::ToNumberFloat64() const {
    std::ostringstream ost;
    ost << (this->neg_? '-':' ')
        << std::setfill('0') << std::setw(2)
        << this->hour_ << std::setw(2)
        << this->minute_ << std::setw(2)
        << this->second_
        << "." << std::setw(6)
        << this->micro_second_;

    return std::stod(ost.str());
}

bool MyTime::FromNumberFloat64(const double f64, StWarn &st) {

    bool neg = false;
    double tmp_f64 = f64;

    if ( tmp_f64 < 0){
        tmp_f64 = -tmp_f64;
        neg = true;
    }

    std::string str = std::to_string(tmp_f64);
    auto len = str.length();
    auto pos = str.find('.');
    std::string str_intg = str;
    std::string str_point = "";
    if ( pos != std::string::npos) {
        str_intg = str.substr(0, pos);
        int32_t tmp_l = len - pos -1;
        str_point = str.substr(pos+1, (tmp_l > 6 ? 6 : tmp_l));
    }

    int64_t intg = std::stol(str_intg);
    uint64_t point = std::stol(str_point);

    bool ret = this->FromNumberInt64( intg, st);

    if (ret) {
        this->SetMicroSecond(point);
        this->SetNeg(neg);
    } else {
        this->SetZero();
    }

    return ret;
}

std::string MyTime::ToNumberString() const {
    std::ostringstream ost;
    ost << (this->neg_? '-':' ')
        << std::setfill('0') << std::setw(2)
        << this->hour_ << std::setw(2)
        << this->minute_ << std::setw(2)
        << this->second_
        << "." << std::setw(6)
        << this->micro_second_;

    return ost.str();
}

bool MyTime::FromString(const std::string& str, StWarn &st){
    bool ret = true;

    st = MYTIME_WARN_NONE;

    if (str.empty()) {
        st = MYTIME_WARN_TRUNCATED ;
        this->SetZero();
        ret = false;
        return ret;
    }

    std::string str_tmp = trim(str);

    if (str_tmp.empty()) {
        st = MYTIME_WARN_TRUNCATED ;
        this->SetZero();
        ret = false;
        return ret;
    }

    int32_t pos = 0;
    bool neg = false;
    uint64_t h = 0, mi = 0, s = 0, ms = 0;
    int32_t str_tmp_len = str_tmp.length();

    if ( str_tmp[0] == '-') {
        neg = true;
        pos++;
    }

    if ( str_tmp_len == pos) {
        st = MYTIME_WARN_TRUNCATED;
        this->SetZero();
        ret = false;
        return ret;
    }

    str_tmp = str_tmp.substr(pos);
    pos = 0;
    str_tmp_len = str_tmp.length();


    if ( str_tmp_len >= 12) {
        ret = this->FromDateTimeStr(str_tmp);
        if ( ret ) {
            this->SetNeg(neg);
            return ret;
        }
    }

    ret = true;

    uint64_t possible_day_digits = 0;
    int32_t possible_day_pos = 0;
    int32_t possible_day_space_pos = 0;
    int32_t possible_other_pos = 0;

    for (  pos = 0; pos < str_tmp_len; pos++) {
        if (IsDigit(str_tmp[pos])) {
            possible_day_digits = 10*possible_day_digits + (str_tmp[pos] - '0') ;
            possible_day_pos++;
        } else {
            break;
        }
    }

    if ( possible_day_digits > UINT32_MAX) {
        return false;
    }

    for ( possible_day_space_pos = pos; pos < str_tmp_len; pos++) {

        if (IsSpace(str_tmp[pos])) {
            possible_day_space_pos++;
        } else {
            break;
        }
    }

    int32_t start = 0;
    std::vector<std::string> v_digit;
    std::string str_ms;
    bool hav_point = false;
    bool is_over = false;

    if ( possible_day_pos == 0 && possible_day_space_pos == 0 ) { // no digit and no space. day_pos == day_space_pos == pos == 0;
        if (str_tmp[pos] == '.') {

            start = pos+1;
            for ( pos = pos + 1; pos < str_tmp_len; pos++) {
                if ( (start == pos) && (!IsDigit(str_tmp[pos]))) {
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos]) && (IsDigit(str_tmp[pos-1])))) {
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    str_ms = str_tmp.substr(start, pos-start);
                    break;
                }
            }

            if ( (!is_over) && (start < pos)) {
                str_ms = str_tmp.substr(start);
                st = MYTIME_WARN_NONE;
            }

        } else if ( str_tmp[pos] == ':') {

            if ( (pos + 1) == str_tmp_len) {
                st = MYTIME_WARN_TRUNCATED;
                ret = true;
            } else {
                start = pos + 1;
                hav_point = false;
                for ( pos++; pos < str_tmp_len; pos++ ) {
                    if ( (pos == start) && (!IsDigit(str_tmp[pos]))) {
                        is_over = true;
                        st = MYTIME_WARN_TRUNCATED;
                        break;
                    }

                    if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                        if ( str_tmp[pos] == ':' ) {

                            if ( hav_point) {
                                str_ms = str_tmp.substr( start, (pos-start));
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            } else {
                                v_digit.push_back(str_tmp.substr(start, (pos-start)));
                                if ( v_digit.size() > 3 ) {
                                    is_over = true;
                                    st = MYTIME_WARN_TRUNCATED;
                                    break;
                                }
                                start = pos + 1;
                            }
                        } else if ( str_tmp[pos] == '.') {

                            if (hav_point) {
                                is_over = true;
                                str_ms = str_tmp.substr( start, (pos-start));
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            } else {
                                v_digit.push_back(str_tmp.substr(start, (pos-start)));
                                if ( v_digit.size() > 3 ) {
                                    is_over = true;
                                    st = MYTIME_WARN_TRUNCATED;
                                    break;
                                }
                                start = pos + 1;
                            }
                            hav_point = true;
                        } else {
                            if ( hav_point) {
                                str_ms =  str_tmp.substr( start, (pos-start));
                            } else {
                                v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            }
                            is_over = true;
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        }
                    }
                }

                if ( (!is_over) && (start < pos) ) {
                    if ( hav_point) {
                        str_ms =  str_tmp.substr( start, (pos-start));
                        start = pos + 1;
                    } else {
                        v_digit.push_back(str_tmp.substr(start, (pos-start)));
                        start = pos + 1;
                    }
                }

                switch( v_digit.size()) {
                    case 0 : {
                        s = possible_day_digits % 100;
                        mi = (possible_day_digits / 100) % 100;
                        h = possible_day_digits / 10000;
                        break;
                    }
                    case 1 : {
                        h = possible_day_digits;
                        mi = std::stoul(v_digit[0]);
                        break;
                    }
                    default: {
                        h = possible_day_digits;
                        mi = std::stoul(v_digit[0]);
                        s = std::stoul(v_digit[1]);
                        if (v_digit.size() > 2) {
                            st = MYTIME_WARN_TRUNCATED;
                        }
                        break;
                    }
                }
            }

        } else {
            s = possible_day_digits % 100;
            mi = (possible_day_digits / 100) % 100;
            h = possible_day_digits / 10000;
            st = MYTIME_WARN_TRUNCATED;
            ret = true;
        }

    } else if ( possible_day_pos > 0 && possible_day_space_pos == possible_day_pos ) { // (day_pos == day_space_pos == pos) > 0

        if ( (possible_day_pos) == str_tmp_len) { // only digit

            s = possible_day_digits % 100;
            mi = (possible_day_digits / 100) % 100;
            h = possible_day_digits / 10000;

        } else if ( possible_day_pos < str_tmp_len)  {
             if ( str_tmp[pos] == '.' ) { // hava ms
                 start = pos + 1;
                 is_over = false;
                 for ( pos=pos+1 ; pos < str_tmp_len; pos++) {
                     if ( (start == pos) && (!IsDigit(str_tmp[pos]))) {
                         st = MYTIME_WARN_TRUNCATED;
                         is_over = true;
                         break;
                     }

                     if ( (!IsDigit(str_tmp[pos]) && (IsDigit(str_tmp[pos-1])))) {

                         str_ms = str_tmp.substr(start, (pos-start));
                         st = MYTIME_WARN_TRUNCATED;
                         is_over = true;
                         break;
                     }
                 }

                 if ( (!is_over) && (start < pos)) {
                     str_ms = str_tmp.substr(start);
                 }

                 s = possible_day_digits % 100;
                 mi = (possible_day_digits / 100) % 100;
                 h = possible_day_digits / 10000;

             } else if ( str_tmp[pos] == ':') {
                 start = pos + 1;
                 for ( pos = pos + 1; pos < str_tmp_len; pos++ ) {
                     if (  (pos == start) && (!IsDigit(str_tmp[pos]))) {
                         is_over = true;
                         st = MYTIME_WARN_TRUNCATED;
                         break;
                     }

                     if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                         if ( str_tmp[pos] == ':' ) {

                             if ( hav_point) {
                                 str_ms =  str_tmp.substr(start, (pos-start));
                                 is_over = true;
                                 st = MYTIME_WARN_TRUNCATED;
                                 break;
                             } else {
                                 v_digit.push_back(str_tmp.substr(start, (pos-start)));
                                 if ( v_digit.size() > 3 ) {
                                     is_over = true;
                                     st = MYTIME_WARN_TRUNCATED;
                                     break;
                                 }
                                 start = pos + 1;
                             }
                         } else if ( str_tmp[pos] == '.') {

                             if (hav_point) {
                                 str_ms =  str_tmp.substr(start, (pos-start));
                                 is_over = true;
                                 st = MYTIME_WARN_TRUNCATED;
                                 break;
                             } else {
                                 v_digit.push_back(str_tmp.substr(start, (pos-start)));
                                 start = pos + 1;
                             }
                             hav_point = true;
                         } else {
                             if ( hav_point) {
                                 ms =  std::stoi(str_tmp.substr( start, ((pos-start) >= 6 ? 6 : (pos-start))));
                                 start = pos + 1;
                             } else {
                                 v_digit.push_back(str_tmp.substr(start, (pos-start)));
                                 start = pos + 1;
                             }
                             is_over = true;
                             st = MYTIME_WARN_TRUNCATED;
                             break;
                         }
                     }
                 }

                 if ( (!is_over) && (start < pos)) {
                     if ( hav_point) {
                         str_ms = str_tmp.substr( start, (pos-start));
                         start = pos + 1;
                     } else {
                         v_digit.push_back(str_tmp.substr(start));
                         start = pos + 1;
                     }
                 }

                 switch( v_digit.size()) {
                     case 0 : {
                         s = possible_day_digits % 100;
                         mi = (possible_day_digits / 100) % 100;
                         h = possible_day_digits / 10000;
                         break;
                     }
                     case 1 : {
                         h = possible_day_digits;
                         mi = std::stoi(v_digit[0]);
                         break;
                     }
                     default : {
                         h = possible_day_digits;
                         mi = std::stoi(v_digit[0]);
                         s = std::stoi(v_digit[1]);
                         if (v_digit.size() > 2) {
                             st = MYTIME_WARN_TRUNCATED;
                         }
                         break;
                     }
                 }
             } else {
                 s = possible_day_digits % 100;
                 mi = (possible_day_digits / 100) % 100;
                 h = possible_day_digits / 10000;
                 st = MYTIME_WARN_TRUNCATED;
             }

        } else {
            // never to here;
           ret = true;
        }

    } else if ( (possible_day_pos == 0) && (possible_day_space_pos > 0) ) {

        if (str_tmp[pos] == '.') {
            start = pos+1;
            is_over = true;
            for ( pos = pos + 1; pos < str_tmp_len; pos++) {
                if ( (start == pos) && (!IsDigit(str_tmp[pos]))) {
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos]) && (IsDigit(str_tmp[pos-1])))) {
                    str_ms = str_tmp.substr(start, (pos-start));
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    break;
                }
            }

            if ( (!is_over) && (start < pos)) {
                str_ms = str_tmp.substr(start);
            }

        } else if ( str_tmp[pos] == ':')  {

            start = pos + 1;
            is_over = false;
            for ( pos = pos + 1; pos < str_tmp_len; pos++ ) {
                if (  (pos == start) && (!IsDigit(str_tmp[pos]))) {
                    is_over = true;
                    st = MYTIME_WARN_TRUNCATED;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                    if ( str_tmp[pos] == ':' ) {

                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                            is_over = true;
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                    } else if ( str_tmp[pos] == '.') {

                        if (hav_point) {
                            is_over = true;
                            str_ms = str_tmp.substr( start, (pos-start));
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                        hav_point = true;
                    } else {
                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                        }
                        is_over = true;
                        st = MYTIME_WARN_TRUNCATED;
                        break;
                    }
                }
            }

            if ( (!is_over) && (start < pos)) {
                if ( hav_point) {
                    str_ms = str_tmp.substr( start, (pos-start));
                    start = pos + 1;
                } else {
                    v_digit.push_back(str_tmp.substr(start, (pos-start)));
                    start = pos + 1;
                }
            }

            switch( v_digit.size()) {
                case 0 : {
                    s = possible_day_digits % 100;
                    mi = (possible_day_digits / 100) % 100;
                    h = possible_day_digits / 10000;
                    break;
                }
                case 1 : {
                    mi = possible_day_digits;
                    s = std::stoi(v_digit[0]);
                    break;
                }
                default: {
                    mi = possible_day_digits;
                    s = std::stoi(v_digit[0]);
                    //s = std::stoi(v_digit[1]);
                    if (v_digit.size() > 2) {
                        st = MYTIME_WARN_TRUNCATED;
                    }
                    break;
                }
            }
        } else if ( IsDigit(str_tmp[pos])) {

            start = pos;
            is_over = false;
            for ( ; pos < str_tmp_len; pos++ ) {
                if ( (pos == start) && (!IsDigit(str_tmp[pos]))) {
                    is_over = true;
                    st = MYTIME_WARN_TRUNCATED;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                    if ( str_tmp[pos] == ':' ) {

                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                            is_over = true;
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                    } else if ( str_tmp[pos] == '.') {

                        if (hav_point) {
                            is_over = true;
                            str_ms = str_tmp.substr( start, (pos-start));
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                        hav_point = true;
                    } else {
                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                        }
                        is_over = true;
                        st = MYTIME_WARN_TRUNCATED;
                        break;
                    }
                }
            }

            if ( (!is_over) && (start < pos) ) {
                if ( hav_point) {
                    str_ms = str_tmp.substr( start, (pos-start));
                    start = pos + 1;
                } else {
                    v_digit.push_back(str_tmp.substr(start, (pos-start)));
                    start = pos + 1;
                }
            }

            switch( v_digit.size()) {
                case 0 : {
                    s = possible_day_digits % 100;
                    mi = (possible_day_digits / 100) % 100;
                    h = possible_day_digits / 10000;
                    break;
                }
                case 1 : {
                    h = possible_day_digits;
                    mi = std::stoi(v_digit[0]);
                    break;
                }
                default: {
                    h = possible_day_digits;
                    mi = std::stoi(v_digit[0]);
                    s = std::stoi(v_digit[1]);
                    if (v_digit.size() > 3) {
                        st = MYTIME_WARN_TRUNCATED;
                    }
                    break;
                }
            }
        } else {
            s = possible_day_digits % 100;
            mi = (possible_day_digits / 100) % 100;
            h = possible_day_digits / 10000;
            st = MYTIME_WARN_TRUNCATED;
        }

    } else if ( (possible_day_pos > 0) && (possible_day_space_pos > 0) && (possible_day_space_pos > possible_day_pos) ) {

        if (str_tmp[pos] == '.') {

            start = pos+1;
            is_over = true;
            for ( pos = pos + 1; pos < str_tmp_len; pos++) {
                if ( (start == pos) && (!IsDigit(str_tmp[pos]))) {
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos]) && (IsDigit(str_tmp[pos-1])))) {
                    str_ms = str_tmp.substr(start, (pos-start));
                    st = MYTIME_WARN_TRUNCATED;
                    is_over = true;
                    break;
                }
            }

            if ( (!is_over) && (start < pos)) {
                str_ms = str_tmp.substr(start);
            }

            s = possible_day_digits % 100;
            mi = (possible_day_digits / 100) % 100;
            h = possible_day_digits / 10000;

        } else if ( str_tmp[pos] == ':')  {

            start = pos+1;
            is_over = false;
            for ( pos++; pos < str_tmp_len; pos++ ) {
                if (  (pos == start) && (!IsDigit(str_tmp[pos]))) {
                    is_over = true;
                    st = MYTIME_WARN_TRUNCATED;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                    if ( str_tmp[pos] == ':' ) {

                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                            is_over = true;
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                    } else if ( str_tmp[pos] == '.') {
                        if (hav_point) {

                            str_ms = str_tmp.substr( start, (pos-start));
                            st = MYTIME_WARN_TRUNCATED;
                            is_over = true;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                        hav_point = true;
                    } else {
                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                        }
                        is_over = true;
                        st = MYTIME_WARN_TRUNCATED;
                        break;
                    }
                }
            }

            if ( (!is_over) && (start < pos) ) {
                if ( hav_point) {
                    str_ms = str_tmp.substr( start, (pos-start));
                    start = pos + 1;
                } else {
                    v_digit.push_back(str_tmp.substr(start, (pos-start)));
                    start = pos + 1;
                }
            }

            switch( v_digit.size()) {
                case 0 : {
                    s = possible_day_digits % 100;
                    mi = (possible_day_digits / 100) % 100;
                    h = possible_day_digits / 10000;
                    break;
                }
                case 1 : {
                    h = possible_day_digits;
                    mi = std::stoi(v_digit[0]);
                    break;
                }
                default: {
                    h = possible_day_digits;
                    mi = std::stoi(v_digit[0]);
                    s = std::stoi(v_digit[1]);
                    if (v_digit.size() > 3) {
                        st = MYTIME_WARN_TRUNCATED;
                    }
                    break;
                }
            }
        } else if ( IsDigit(str_tmp[pos])) {

            start = pos;
            is_over = false;
            bool f = ((str_tmp_len - pos -1) > 0);
            for ( ; pos < str_tmp_len; pos++ ) {
                if ( (pos == start) && (!IsDigit(str_tmp[pos]))) {
                    is_over = true;
                    st = MYTIME_WARN_TRUNCATED;
                    break;
                }

                if ( (!IsDigit(str_tmp[pos])) && (IsDigit(str_tmp[pos-1]))) {
                    if ( str_tmp[pos] == ':' ) {

                        if ( hav_point) {
                            str_ms = str_tmp.substr( start, (pos-start));
                            is_over = true;
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                    } else if ( str_tmp[pos] == '.') {

                        if (hav_point) {
                            is_over = true;
                            str_ms = str_tmp.substr( start, (pos-start));
                            st = MYTIME_WARN_TRUNCATED;
                            break;
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                            if ( v_digit.size() > 3 ) {
                                is_over = true;
                                st = MYTIME_WARN_TRUNCATED;
                                break;
                            }
                            start = pos + 1;
                        }
                        hav_point = true;
                    } else {
                        if ( hav_point) {
                            str_ms =  std::stoi(str_tmp.substr( start, ((pos-start) >= 6 ? 6:(pos-start))));
                        } else {
                            v_digit.push_back(str_tmp.substr(start, (pos-start)));
                        }
                        is_over = true;
                        st = MYTIME_WARN_TRUNCATED;
                        break;
                    }
                }
            }

            if ( (!is_over) && (start < pos)) {
                if ( hav_point) {
                    str_ms = str_tmp.substr( start, (pos-start));
                    start = pos + 1;
                } else {
                    v_digit.push_back(str_tmp.substr(start, (pos-start)));
                    start = pos + 1;
                }
            }

            switch( v_digit.size()) {
                case 0 : {
                    s = possible_day_digits % 100;
                    mi = (possible_day_digits / 100) % 100;
                    h = possible_day_digits / 10000;
                    break;
                }
                case 1 : {
                    if (f) {
                        h = possible_day_digits*24 + std::stoi(v_digit[0]);
                    } else {
                        s = possible_day_digits;
                    }
                    break;
                }
                case 2 : {
                    h = possible_day_digits*24 + std::stoi(v_digit[0]);
                    mi = std::stoi(v_digit[1]);
                    break;
                }
                default : {
                    h = possible_day_digits*24 + std::stoi(v_digit[0]);
                    mi = std::stoi(v_digit[1]);
                    s = std::stoi(v_digit[2]);
                    if (v_digit.size() > 3) {
                        st = MYTIME_WARN_TRUNCATED;
                    }

                    break;
                }
            }
        } else {
            s = possible_day_digits % 100;
            mi = (possible_day_digits / 100) % 100;
            h = possible_day_digits / 10000;
        }
    }

    int l_ms = str_ms.length();
    uint64_t ms_tmp = 0;

    if (!str_ms.empty()) {
        ms_tmp = static_cast<uint64_t>(std::stoul(str_ms));
        while (ms_tmp > 1000 * 1000) {
            ms_tmp /= 10;
        }

        int32_t fix_len = ((l_ms >= 6) ? 0 : (6 - l_ms));
        ms = ms_tmp * log10num[fix_len];
    }

    if (l_ms > 6) {
        st = MYTIME_WARN_TRUNCATED;
    }

    this->FromTime(h, mi, s, neg);
    this->SetMicroSecond(ms);

    return ret;
}

void MyTime::SetZero() {
    this->hour_ = 0;
    this->minute_ = 0;
    this->second_ = 0;
    this->micro_second_ = 0;
    this->neg_ = false;
}

bool MyTime::CheckRange() const {

    return CheckHour() && CheckMinute() && CheckSecond() && CheckMicroSecond();
}

void MyTime::SetMaxTime() {

    this->hour_ = TIME_MAX_HOUR;
    this->minute_ = TIME_MAX_MINUTE;
    this->second_ = TIME_MAX_SECOND;

    this->neg_ = false;

    return ;
}

bool MyTime::operator == ( const MyTime & other) const{

    if ( this == &other ) {
        return true;
    }

    if ( (this->hour_ == other.hour_)
        && (this->minute_ == other.minute_)
        && (this->second_ == other.second_)
        && (this->micro_second_ == other.micro_second_)
        && (this->neg_ == other.neg_)
        ) {
        return true;
    } else {
        return false;
    }
}

bool MyTime::operator != ( const MyTime & other) const{

    return !(*this == other );
}

bool MyTime::operator < ( const MyTime & other) const{

    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left < right ) {
        return true;
    } else {
        return false;
    }
}

bool MyTime::operator > ( const MyTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left > right ) {
        return true;
    } else {
        return false;
    }
}

bool MyTime::operator <= ( const MyTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left <= right ) {
        return true;
    } else {
        return false;
    }
}

bool MyTime::operator >= ( const MyTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left >= right ) {
        return true;
    } else {
        return false;
    }
}

int32_t MyTime::AdjustYear( const int32_t y) {

    int32_t yy = 0;

    if ( (y >= 0) && (y <= 69)) {
        yy = 2000 + y;
    } else if ( (y >= 70) && ( y <= 99) ) {
        yy = 1900 + y;
    } else {
        yy = y;
    }

    return yy;
}

int32_t MyTime::GetFracIndex(const std::string& str) {

    int32_t index = -1;

    for ( int32_t i = str.length() - 1; i >= 0; i--) {

        if ( !IsDigit( static_cast<uint8_t>(str[i]))) {
            if (str[i] == '.') {
                index = static_cast<int32_t>(i);
            }
            break;
        }
    }
    return index;
}

bool MyTime::ParseDateTime(const std::string& format, std::vector<std::string>& seps) {

    std::string str_tmp = format;
    str_tmp = trim(str_tmp);
    std::string sub;

    int32_t start = 0;

    for ( size_t i = 0; i < str_tmp.length(); i++) {

        if ( ( i == 0) || ( i == str_tmp.length() - 1 )) {
            if ( !IsDigit( static_cast<uint8_t>(str_tmp[i]))) {
                return false;
            }

            continue;
        }

        if ( !IsDigit(static_cast<uint8_t>( str_tmp[i]))) {
            if ( !IsDigit(static_cast<uint8_t>(str_tmp[i-1]))) {
                return false;
            }

            if ( ((str_tmp[i] == ':') || (str_tmp[i] == '.' || (str_tmp[i] == 'T')) ) && (seps.size() < 3) ) {
                return false;
            }

            sub = str_tmp.substr(start, i - start);
            start = i + 1;

            seps.push_back(sub);
        }
    }

    sub = str_tmp.substr(start);
    seps.push_back(sub);

    return true;
}

bool MyTime::SplitDateTime(const std::string& format, std::vector<std::string>& seps, std::string& spStr) {

    int32_t index = GetFracIndex(format);
    std::string str_tmp;
    if ( index > 0) {
        spStr = format.substr(index+1);
        str_tmp = format.substr(0, index);
    } else {
        str_tmp = format;
    }

    return ParseDateTime( str_tmp, seps);
}

bool MyTime::FromDateTimeStr(const std::string& format) {

    bool ret = false;
    std::vector<std::string> seps;
    std::string spStr;
    std::string str = format;
    int err = 1;

    ret = SplitDateTime( str, seps, spStr);
    if ( !ret ) {
        return ret;
    }

    uint32_t y = 0, mo = 0, d = 0, h = 0, mi = 0, s = 0, ms = 0;
    bool hms = false;

    switch(seps.size()) {
        case 1: {
            auto sep_one_len = seps[0].size();
            switch( sep_one_len) {
                // no delimiter
                case 14:{  // YYYYMMDDHHMMSS
                    err = std::sscanf(seps[0].c_str(), "%4d%2d%2d%2d%2d%2d", &y, &mo, &d, &h, &mi, &s);
                    hms = true;
                    break;
                }
                case 12: { // YYMMDDHHMMSS
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%2d", &y, &mo, &d, &h, &mi, &s);
                    y = AdjustYear(y);
                    hms = true;
                    break;
                }
                case 11: { // YYMMDDHHMMS
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d%1d", &y, &mo, &d, &h, &mi, &s);
                    y = AdjustYear(y);
                    hms = true;
                    break;
                }
                case 10: { // YYMMDDHHMM
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%2d", &y, &mo, &d, &h, &mi);
                    y = AdjustYear(y);
                    break;
                }
                case 9: { // YYMMDDHHM
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%2d%2d%1d", &y, &mo, &d, &h, &mi);
                    y = AdjustYear(y);
                    break;
                }
                case 8: { // YYYYMMDD
                    err = std::sscanf(seps[0].c_str(), "%4d%2d%2d", &y, &mo, &d);
                    break;
                }
                case 6: { // YYMMDD
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%2d", &y, &mo, &d);
                    y = AdjustYear(y);
                    break;
                }
                case 5: { // YYMMD
                    err = std::sscanf(seps[0].c_str(), "%2d%2d%1d", &y, &mo, &d);
                    y = AdjustYear(y);
                    break;
                }
                default : {
                    this->SetZero();
                    ret = false;
                    return ret;
                }
            }

            auto sp_len = spStr.length();
            if ( sep_one_len == 5 || sep_one_len == 6 || sep_one_len == 8 ) {
                switch ( sp_len ) {
                    case 0: {
                        break;
                    }
                    case 1:
                    case 2: {
                        err = std::sscanf(spStr.c_str(), "%2d", &h);
                        break;
                    }
                    case 3:
                    case 4: {
                        err = std::sscanf(spStr.c_str(), "%2d%2d", &h, &mi);
                        break;
                    }
                    default: {
                        err = std::sscanf(spStr.c_str(), "%2d%2d%2d", &h, &mi, &s);
                        break;
                    }
                }
            } else if ( sep_one_len == 9 || sep_one_len == 10) {
                if ( sp_len == 0) {
                    s = 0;
                } else {
                    err = std::sscanf(spStr.c_str(), "%2d", &s);
                }
            }

            break;
        }
        case 2: {
            // YYYY-MM

            if ( spStr.length() == 0) {

                this->SetZero();

                ret = false;
                return ret;
            }

            // YYYY-MM/DD
            // seps.push_back(spStr);

            y = std::stoi(seps[0]);
            mo = std::stoi(seps[1]);
            d = std::stoi(spStr);
            break;
        }
        case 3: {
            // YYYY-MM-DD
            y = std::stoi(seps[0]);
            mo = std::stoi(seps[1]);
            d = std::stoi(seps[2]);
            break;
        }
        case 4: {
            // YYYY-MM-DD HH
            y = std::stoi(seps[0]);
            mo = std::stoi(seps[1]);
            d = std::stoi(seps[2]);
            h = std::stoi(seps[3]);
            break;
        }
        case 5: {
            // YYYY-MM-DD HH-MM
            y = std::stoi(seps[0]);
            mo = std::stoi(seps[1]);
            d = std::stoi(seps[2]);
            h = std::stoi(seps[3]);
            mi = std::stoi(seps[4]);
            break;
        }
        case 6: {
            // YYYY-MM-DD HH-MM-SS
            y = std::stoi(seps[0]);
            mo = std::stoi(seps[1]);
            d = std::stoi(seps[2]);
            h = std::stoi(seps[3]);
            mi = std::stoi(seps[4]);
            s = std::stoi(seps[5]);
            hms = true;
            break;
        }
        default: {
            this->SetZero();
            ret = false;
            return ret;
        }
    }

    if ( err == 0 && err == EOF ) {
        this->SetZero();
        ret = false;
        return ret;
    }

    if ( seps[0].length() == 2) {
        if ( ( y == 0) && ( mo == 0) && ( d == 0 ) && ( h == 0) && ( mi == 0) && ( s == 0) && ( spStr == "") ) {
            ; //
        } else {
            y = AdjustYear(y);
        }
    }
    uint32_t ms_tmp = 0;
    uint32_t len_point = spStr.length();

    if ( hms && (len_point > 0) ) {
        ms_tmp = static_cast<uint32_t>(std::stoul(spStr));
        while ( ms_tmp > 1000*1000) {
            ms_tmp /= 10;
        }

        int32_t fix_len = ((len_point >= 6) ? 0:(6-len_point));

        ms = ms_tmp * log10num[fix_len];

//        do {
//            ms = ms_tmp;
//            ms_tmp = ms_tmp * 10;
//            fix_len--;
//        } while ( ms_tmp < log10num[fix_len] && ms_tmp > 0);
    }


    FromTime(h, mi, s, true);
    this->SetMicroSecond(ms);
    ret = true;

    return ret;
}

std::ostream& operator<< ( std::ostream& os, const MyTime& t)
{
    os << "Neg:" << (t.neg_ ? "true":"fase")
        << ", Hour:" << t.hour_
        << ", Minute:" << t.minute_
        << ", Second:" << t.second_
        << ", MicroSecond:" << t.micro_second_ ;

    return os;
}


} // namespace dataytpe
} // namespace jim
