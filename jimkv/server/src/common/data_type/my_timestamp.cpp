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

#include "my_timestamp.h"
#include <vector>
#include <string>
#include <algorithm>
#include <utility>
#include <sstream>
#include <iomanip>
#include <cstdio>

namespace jimkv {
namespace datatype{

const std::string my_zero_datetime = "0000-00-00 00:00:00.000000";

const int32_t MyDateTime::days_in_month_[14] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31, 0 };

const uint32_t MyDateTime::log10num[] = {
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

MyDateTime::MyDateTime() {
    year_ = 0;
    month_ = 0;
    day_ = 0;
    hour_ = 0;
    minute_ = 0;
    second_ = 0;
    micro_second_ = 0;
    neg_ = false;
}

MyDateTime::MyDateTime(const MyDateTime& dt){
    this->year_ = dt.year_;
    this->month_ = dt.month_;
    this->day_ = dt.day_;
    this->hour_ = dt.hour_;
    this->minute_ = dt.minute_;
    this->second_ = dt.second_;
    this->micro_second_ = dt.micro_second_;
    this->neg_ = dt.neg_;
}

MyDateTime& MyDateTime::operator=( const MyDateTime& dt){

    if (this == &dt) {
        return *this;
    }

    this->year_ = dt.year_;
    this->month_ = dt.month_;
    this->day_ = dt.day_;
    this->hour_ = dt.hour_;
    this->minute_ = dt.minute_;
    this->second_ = dt.second_;
    this->micro_second_ = dt.micro_second_;
    this->neg_ = dt.neg_;

    return *this;
}

MyDateTime::~MyDateTime(){
}

MyDateTime& MyDateTime::FromDateTime( const uint32_t y, const uint32_t m, const uint32_t d, const uint32_t h, const uint32_t mi, uint32_t s) {

    this->year_ = y;
    this->month_ = m;
    this->day_ = d;
    this->hour_ = h;
    this->minute_ = mi;
    this->second_ = s;

    return *this;
}

uint32_t MyDateTime::GetYear() const{
    return this->year_;
}

void MyDateTime::SetYear( const uint32_t y){
    this->year_ = y;
}

bool MyDateTime::CheckYear() const {
    return this->year_ <= 9999;
}

uint32_t MyDateTime::GetMonth() const{
    return this->month_;
}

void MyDateTime::SetMonth( const uint32_t m){
    this->month_ = m;
}

bool MyDateTime::CheckMonth() const {
    return this->month_ <= 12;
}

uint32_t MyDateTime::GetDay() const{
    return this->day_;
}

void MyDateTime::SetDay( const uint32_t d){
    this->day_ = d;
}

bool MyDateTime::CheckDay() const {

    if ( !this->CheckMonth()) {
        return false;
    }

    uint32_t max_day = 0;
    if ( this->IsLeapYear() && (this->month_ == 2)) {
        max_day = days_in_month_[this->month_] + 1;
    } else {
        max_day = days_in_month_[this->month_];
    }

    if ( this->day_ <= max_day){
        return true;
    } else {
        return false;
    }
}

uint32_t MyDateTime::GetHour() const{
    return this->hour_;
}

void MyDateTime::SetHour( const uint32_t h){
    this->hour_ = h;
}

bool MyDateTime::CheckHour() const {
    return this->hour_ < 24;
}

uint32_t MyDateTime::GetMinute() const{
    return this->minute_;
}

void MyDateTime::SetMinute( const uint32_t m){
    this->minute_ = m;
}

bool MyDateTime::CheckMinute() const {
    return this->minute_ < 60;
}

uint32_t MyDateTime::GetSecond() const{
    return this->second_;
}

void MyDateTime::SetSecond( const uint32_t s){
    this->second_ = s;
}

bool MyDateTime::CheckSecond() const {
    return this->second_< 60;
}

uint64_t MyDateTime::GetMicroSecond() const{
    return this->micro_second_;
}

void MyDateTime::SetMicroSecond( const uint64_t ms){
    this->micro_second_ = ms;
}

bool MyDateTime::CheckMicroSecond() const {
    return this->micro_second_ <= 999999;
}

bool MyDateTime::MyDateTime::GetNeg() const{
    return this->neg_;
}

void MyDateTime::MyDateTime::SetNeg( bool n){
    this->neg_ = n;
}

int64_t MyDateTime::ToPackInt64() const{

    int64_t ymd = (( static_cast<int64_t>(this->year_) * 13 + static_cast<int64_t>(this->month_)) << 5 ) | static_cast<int64_t>(this->day_);
    int64_t hms = ( static_cast<int64_t>(this->hour_) << 12) | ( static_cast<int64_t>(this->minute_) << 6) | static_cast<int64_t>(this->second_);

    return (((ymd << 17 | hms) << 24 ) | static_cast<int64_t>(this->micro_second_)) * (this->neg_ ? -1 : 1 );
}

void MyDateTime::FromPackInt64(const int64_t dt){

    int64_t dt_tmp = dt;
    if ( dt_tmp < 0) {
        this->neg_ = true;
        dt_tmp = -dt;
    }

    this->micro_second_ = dt_tmp % (1LL << 24);

    int64_t ymdhms = dt_tmp >> 24;

    int64_t ymd = ymdhms >> 17;
    int64_t ym = ymd >> 5 ;
    int64_t hms = ymdhms % (1 << 17);

    this->day_ = ymd % (1 << 5);
    this->month_ = ym % 13;
    this->year_ = ym / 13;

    this->second_ = hms % (1 << 6);
    this->minute_ = ( hms >> 6) % (1 << 6);
    this->hour_ = (hms >> 12);

    return;
}

uint64_t MyDateTime::ToNumberUint64() const {

    uint64_t ymd = static_cast<uint64_t>(this->year_) * 10000UL + static_cast<uint64_t>(this->month_) * 100UL + static_cast<uint64_t>(this->day_);
    uint64_t hms = static_cast<uint64_t>(this->hour_) * 10000UL + static_cast<uint64_t>(this->minute_) * 100UL + static_cast<uint64_t>(this->second_);

    return ymd * 1000000 + hms;
}

bool MyDateTime::FromNumberUint64(const uint64_t u, StWarn &st){

    uint64_t ui = u;

    bool ret = true;
    const int32_t YY_PART_YEAR = 70;
    uint64_t ymd = 0;
    uint64_t hms = 0;

    st = MYDATETIME_WARN_NONE;

    if (ui == 0LL || ui >= 10000101000000LL)
    {
        if (ui > 99999999999999LL) /* 9999-99-99 99:99:99 */
        {
            st = MYDATETIME_WARN_OUT_OF_RANGE;
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

    this->year_ =  ymd/10000L;
    this->month_ = (ymd%10000L)/100;
    this->day_ = (ymd%10000)%100;
    this->hour_ = hms/10000L;
    this->minute_ = (hms%10000L)/100;
    this->second_ = (hms%10000L)%100;

    ret = true;
    st = MYDATETIME_WARN_NONE;
    return ret;

    err:
    ret = false;
    st = MYDATETIME_WARN_TRUNCATED;
    return ret ;
}

double MyDateTime::ToNumberFloat64() const {
     std::ostringstream ost;
     ost << (this->neg_? '-':' ')
        << std::setfill('0') << std::setw(4)
        << this->year_ << std::setw(2)
        << this->month_ << std::setw(2)
        << this->day_<< std::setw(2)
        << this->hour_ << std::setw(2)
        << this->minute_ << std::setw(2)
        << this->second_
        << "." << std::setw(6)
        << this->micro_second_;

     return std::stod(ost.str());
}

bool MyDateTime::FromNumberFloat64(const double f64, StWarn &st) {

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

    uint64_t intg = std::stol(str_intg);
    uint64_t point = std::stol(str_point);

    bool ret = this->FromNumberUint64( intg, st);

    if (ret) {
        this->SetMicroSecond(point);
        // neg
    } else {
        this->SetZero();
    }

    return ret;
}

std::string MyDateTime::ToNumberString() const {
    std::ostringstream ost;
    ost << (this->neg_? '-':' ')
        << std::setfill('0') << std::setw(4)
        << this->year_ << std::setw(2)
        << this->month_ << std::setw(2)
        << this->day_<< std::setw(2)
        << this->hour_ << std::setw(2)
        << this->minute_ << std::setw(2)
        << this->second_
        << "." << std::setw(6)
        << this->micro_second_;

    return ost.str();
}

std::string MyDateTime::ToString() const{

    std::ostringstream ost;
    ost << (this->neg_ ? "-" : "")
        << std::setfill('0') << std::setw(4)
        << this->year_ << "-" << std::setw(2)
        << this->month_ << "-" << std::setw(2)
        << this->day_ << " " << std::setw(2)
        << this->hour_ << ":" << std::setw(2)
        << this->minute_ << ":" <<std::setw(2)
        << this->second_ << "." << std::setw(6)
        << this->micro_second_ ;

    std::string str_ret = ost.str();

    return str_ret;
}

bool MyDateTime::FromString(const std::string& str, StWarn &st ){

    bool ret = false;
    std::vector<std::string> seps;
    std::string spStr;
    int err = 1;

    st = MYDATETIME_WARN_NONE;

    ret = SplitDateTime(str, seps, spStr);
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

    if ( hms && (len_point > 0)) {
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


    FromDateTime(y, mo, d, h, mi, s);
    this->SetMicroSecond(ms);
    ret = true;

    return ret;
}


void MyDateTime::SetZero(){
    this->year_ = 0;
    this->month_ = 0;
    this->day_ = 0;
    this->hour_ = 0;
    this->minute_ = 0;
    this->second_ = 0;
    this->micro_second_ = 0;
    this->neg_ = false;
}

bool MyDateTime::IsLeapYear() const {
    return ((this->year_ % 4 == 0) && (this->year_ % 400 != 0)) || (this->year_ % 400 == 0);
}

bool MyDateTime::CheckRange() const {
    return CheckYear() && CheckMonth() && CheckDay() && CheckHour() && CheckMinute() && CheckSecond() && CheckMicroSecond();
}

bool MyDateTime::operator == ( const MyDateTime & other) const {
    if (this == &other) {
        return true;
    }

    if ( (this->year_ == other.year_)
        && (this->month_ == other.month_)
        && (this->day_ == other.day_)
        && (this->hour_ == other.hour_)
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

bool MyDateTime::operator != ( const MyDateTime & other) const {

    return !(*this==other);
}

bool MyDateTime::operator < ( const MyDateTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left < right ) {
        return true;
    } else {
        return false;
    }
}

bool MyDateTime::operator > ( const MyDateTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left > right ) {
        return true;
    } else {
        return false;
    }
}

bool MyDateTime::operator <= ( const MyDateTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left <= right ) {
        return true;
    } else {
        return false;
    }
}

bool MyDateTime::operator >= ( const MyDateTime & other) const{
    int64_t left = this->ToPackInt64();
    int64_t right = other.ToPackInt64();

    if ( left >= right ) {
        return true;
    } else {
        return false;
    }
}

int32_t MyDateTime::AdjustYear( const int32_t y) {

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

int32_t MyDateTime::GetFracIndex(const std::string& str){

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

bool MyDateTime::ParseDateTime(const std::string& format, std::vector<std::string>& seps) {

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

            sub = str_tmp.substr(start, i - start);
            start = i + 1;

            seps.push_back(sub);
        }
    }

    sub = str_tmp.substr(start);
    seps.push_back(sub);

    return true;
}

bool MyDateTime::SplitDateTime(const std::string& format, std::vector<std::string>& seps, std::string& spStr)
{
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

std::ostream& operator<< ( std::ostream& os, const MyDateTime& dt)
{
    os << "Neg:" << (dt.neg_ ? "true":"false")
       << ", Year:" << dt.year_
       << ", Month:" << dt.month_
       << ", Day:" << dt.day_
       << ", Hour:" << dt.hour_
       << ", Minute:" << dt.minute_
       << ", Second:" << dt.second_
       << ", MicroSecond:" << dt.micro_second_;

    return os;
}


} // namespace dataytpe
}  // namespace jimkv
