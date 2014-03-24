/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <sys/time.h>
#include <time.h>
#include <stdarg.h>
#include <errno.h>
#include <execinfo.h>
#include <pthread.h>
#include <regex>
#include <cxxabi.h>
#ifdef __MACH__
#include <mach/clock.h>
#include <mach/mach.h>
#include <mach/mach_time.h>
#endif
#include "Common.h"

namespace hadoop {
namespace common {

string Strings::ToString(int32 v) {
  char tmp[32];
  snprintf(tmp, 32, "%d", v);
  return tmp;
}

string Strings::ToString(uint32 v) {
  char tmp[32];
  snprintf(tmp, 32, "%u", v);
  return tmp;
}

string Strings::ToString(int64 v) {
  char tmp[32];
  snprintf(tmp, 32, "%lld", v);
  return tmp;
}

string Strings::ToString(int64 v, char pad, int64 len) {
  char tmp[32];
  snprintf(tmp, 32, "%%%c%lldlld", pad, len);
  return Format(tmp, v);
}

string Strings::ToString(uint64 v) {
  char tmp[32];
  snprintf(tmp, 32, "%llu", v);
  return tmp;
}

string Strings::ToString(bool v) {
  if (v) {
    return "true";
  } else {
    return "false";
  }
}

string Strings::ToString(float v) {
  char tmp[32];
  snprintf(tmp, 32, "%f", v);
  return tmp;
}

string Strings::ToString(double v) {
  char tmp[32];
  snprintf(tmp, 32, "%lf", v);
  return tmp;
}

string Strings::ToString(const vector<string> & vs) {
  string ret;
  ret.append("[");
  ret.append(Join(vs, ","));
  ret.append("]");
  return ret;
}

bool Strings::toBool(const string & str) {
  if (str == "true") {
    return true;
  } else {
    return false;
  }
}

int64 Strings::toInt(const string & str) {
  if (str.length() == 0) {
    return 0;
  }
  return strtoll(str.c_str(), nullptr, 10);
}

float Strings::toFloat(const string & str) {
  if (str.length() == 0) {
    return 0.0f;
  }
  return strtof(str.c_str(), nullptr);
}


string Strings::Format(const char * fmt, ...) {
  char tmp[256];
  string dest;
  va_list al;
  va_start(al, fmt);
  int len = vsnprintf(tmp, 255, fmt, al);
  va_end(al);
  if (len>255) {
    char * destbuff = new char[len+1];
    va_start(al, fmt);
    len = vsnprintf(destbuff, len+1, fmt, al);
    va_end(al);
    dest.append(destbuff, len);
    delete destbuff;
  } else {
    dest.append(tmp, len);
  }
  return dest;
}

void Strings::Append(string & dest, const char * fmt, ...) {
  char tmp[256];
  va_list al;
  va_start(al, fmt);
  int len = vsnprintf(tmp, 255, fmt, al);
  if (len>255) {
    char * destbuff = new char[len+1];
    len = vsnprintf(destbuff, len+1, fmt, al);
    dest.append(destbuff, len);
  } else {
    dest.append(tmp, len);
  }
  va_end(al);
}

string Strings::ToUpper(const string & name) {
  string ret = name;
  for (size_t i = 0 ; i < ret.length() ; i++) {
    ret.at(i) = ::toupper(ret[i]);
  }
  return ret;
}

string Strings::ToLower(const string & name) {
  string ret = name;
  for (size_t i = 0 ; i < ret.length() ; i++) {
    ret.at(i) = ::tolower(ret[i]);
  }
  return ret;
}

string Strings::Trim(const string & str) {
  if (str.length()==0) {
    return str;
  }
  size_t l = 0;
  while (l<str.length() && isspace(str[l])) {
    l++;
  }
  if (l>=str.length()) {
    return string();
  }
  size_t r = str.length();
  while (isspace(str[r-1])) {
    r--;
  }
  return str.substr(l, r-l);
}


void Strings::Split(const string & src, const string & sep,
                       vector<string> & dest, bool clean) {
  if (sep.length()==0) {
    return;
  }
  size_t cur = 0;
  while (true) {
    size_t pos;
    if (sep.length()==1) {
      pos = src.find(sep[0], cur);
    } else {
      pos = src.find(sep, cur);
    }
    string add = src.substr(cur,pos-cur);
    if (clean) {
      string trimed = Trim(add);
      if (trimed.length()>0) {
        dest.push_back(trimed);
      }
    } else {
      dest.push_back(add);
    }
    if (pos==string::npos) {
      break;
    }
    cur=pos+sep.length();
  }
}

string Strings::Join(const vector<string> & strs, const string & sep) {
  string ret;
  for (size_t i = 0; i < strs.size(); i++) {
    if (i > 0) {
      ret.append(sep);
    }
    ret.append(strs[i]);
  }
  return ret;
}

string Strings::ReplaceAll(const string & src, const string & find,
    const string & replace) {
  vector<string> fds;
  Split(src, find, fds, false);
  return Join(fds, replace);
}

bool Strings::StartsWith(const string & str, const string & prefix) {
  if ((prefix.length() > str.length()) ||
      (memcmp(str.data(), prefix.data(), prefix.length()) != 0)) {
    return false;
  }
  return true;
}

bool Strings::EndsWith(const string & str, const string & suffix) {
  if ((suffix.length() > str.length()) ||
      (memcmp(str.data() + str.length() - suffix.length(),
              suffix.data(),
              suffix.length()) != 0)) {
    return false;
  }
  return true;
}

int64 Strings::LastSeparator(const string & str, char sep) {
   size_t pos = str.rfind(sep);
   if (pos == str.npos) {
     return -1;
   } else {
     return (int64)pos;
   }
}

string Strings::LastPart(const string & str, char sep) {
  int64 pos = LastSeparator(str, sep) + 1;
  return string(str.c_str() + pos, str.length() - pos);
}

const char * Strings::LastPartStart(const string & str, char sep) {
  return str.c_str() + LastSeparator(str, sep) + 1;
}

string Strings::toBase64(const string & str) {
  static const char basis_64[] =
      "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  ssize_t len = str.size();
  string ret((len + 2) / 3 * 4, '\0');
  char * p = const_cast<char*>(ret.c_str());
  ssize_t i;
  for (i = 0; i < len - 2; i += 3) {
    *p++ = basis_64[(str[i] >> 2) & 0x3F];
    *p++ = basis_64[((str[i] & 0x3) << 4) | ((str[i + 1] & 0xF0U) >> 4)];
    *p++ = basis_64[((str[i + 1] & 0xF) << 2) | ((str[i + 2] & 0xC0U) >> 6)];
    *p++ = basis_64[str[i + 2] & 0x3F];
  }
  if (i < len) {
    *p++ = basis_64[(str[i] >> 2) & 0x3F];
    if (i == (len - 1)) {
      *p++ = basis_64[((str[i] & 0x3) << 4)];
      *p++ = '=';
    }
    else {
      *p++ = basis_64[((str[i] & 0x3) << 4) | ((str[i + 1] & 0xF0U) >> 4)];
      *p++ = basis_64[((str[i + 1] & 0xF) << 2)];
    }
    *p++ = '=';
  }
  return ret;
}

string Strings::fromBase64(const string & str) {
  static const uint8 pr2six[256] = {
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63,
    52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 64, 64, 64, 64, 64, 64,
    64,  0,  1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14,
    15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 64, 64, 64, 64, 64,
    64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40,
    41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
    64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64
  };
  ssize_t last = str.length() - 1;
  while (last >= 0 && pr2six[(uint8)(str[last])] >= 64) {
    last--;
  }
  ssize_t len = last + 1;
  size_t retlen = len / 4 * 3;
  if (len % 4 == 2) {
    retlen += 1;
  } else if (len % 4 == 3) {
    retlen += 2;
  }
  string ret(retlen, '\0');
  const uint8 * bufin = (uint8*)(str.c_str());
  uint8 * bufout = (uint8*)(ret.c_str());
  while (len > 4) {
    *(bufout++) = (uint8) (pr2six[*bufin] << 2 | pr2six[bufin[1]] >> 4);
    *(bufout++) = (uint8) (pr2six[bufin[1]] << 4 | pr2six[bufin[2]] >> 2);
    *(bufout++) = (uint8) (pr2six[bufin[2]] << 6 | pr2six[bufin[3]]);
    bufin += 4;
    len -= 4;
  }
  if (len > 1) {
    *(bufout++) = (uint8) (pr2six[*bufin] << 2 | pr2six[bufin[1]] >> 4);
  }
  if (len > 2) {
    *(bufout++) = (uint8) (pr2six[bufin[1]] << 4 | pr2six[bufin[2]] >> 2);
  }
  if (len > 3) {
    *(bufout++) = (uint8) (pr2six[bufin[2]] << 6 | pr2six[bufin[3]]);
  }
  return ret;
}


#ifdef __MACH__

int64 Time::CurrentMonoNano() {
  static mutex lock;
  static mach_timebase_info_data_t tb;
  static uint64 timestart = 0;
  if (timestart == 0) {
    lock.lock();
    if (timestart == 0) {
      mach_timebase_info(&tb);
      timestart = mach_absolute_time();
    }
    lock.unlock();
  }
  uint64 ret = mach_absolute_time() - timestart;
  ret *= tb.numer;
  ret /= tb.denom;
  return (int64)ret;
}

int64 Time::CurrentTimeNano() {
  clock_serv_t cclock;
  mach_timespec_t mts;
  host_get_clock_service(mach_host_self(), CALENDAR_CLOCK, &cclock);
  clock_get_time(cclock, &mts);
  mach_port_deallocate(mach_task_self(), cclock);
  return 1000000000ULL * mts.tv_sec + mts.tv_nsec;
}

#else

int64 Time::CurrentMonoNano() {
  timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return 1000000000 * ts.tv_sec + ts.tv_nsec;
}
int64 Time::CurrentTimeNano() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return 1000000000 * ts.tv_sec + ts.tv_nsec;
}

#endif

// thread
uint64 Thread::CurrentId() {
  return (uint64)pthread_self();
}

void Thread::Sleep(uint32 millisec) {
  usleep(millisec * 1000);
}


string demangle(const char * sym) {
#ifdef __MACH__
  string orig(sym);
  auto start = orig.find(" _Z");
  if (start == orig.npos) {
    return orig;
  }
  auto end = orig.find(" ", start+1);
  string cxxmangled = orig.substr(start + 1, end - (start + 1));
  char buff[256];
  size_t length = 256;
  int status;
  abi::__cxa_demangle(cxxmangled.c_str(), buff, &length, &status);
  string ret = orig.substr(0, start);
  ret.append(" ");
  ret.append(buff);
  ret.append(orig.substr(end));
  ret = Strings::ReplaceAll(ret,
      "std::__1::basic_string<char, std::__1::char_traits<char>, std::__1::allocator<char> >",
      "string");
  ret = Strings::ReplaceAll(ret, "std::__1::", "");
  return ret;
#else
  return sym;
#endif
}

void Thread::AddStackTrace(string & dst) {
  void *array[64];
  size_t size;
  size = backtrace(array, 64);
  char ** traces = backtrace_symbols(array, size);
  for (size_t i = 1; i < size; i++) {
    dst.append(demangle(traces[i]));
    dst.append("\n");
  }
  free(traces);
}

void Thread::LogStackTrace(const char * msg) {
  string st;
  AddStackTrace(st);
  LOG("%s\n%s", msg, st.c_str());
}

// log
const char * gLogLevels[] = {
    "DEBUG", "INFO", "WARN", "ERROR", "FATAL"
};

static LogLevel SetupLogLevel() {
  LogLevel ret = LOG_LEVEL_INFO;
  char * s = getenv("LOG_LEVEL");
  if (s != nullptr) {
    for (int i = 0; i < 4; i++) {
      if (strcmp(gLogLevels[i], s) == 0) {
        ret = (LogLevel) i;
        break;
      }
    }
  }
  if (getenv("DEBUG")) {
    ret = LOG_LEVEL_DEBUG;
  }
  return ret;
}

static FILE * SetupLogDevice() {
  FILE * ret = stderr;
  char * s = getenv("LOG_DEVICE");
  if (s != nullptr) {
    if (strcmp("stdout", s)==0) {
      ret = stdout;
    }
    else if (strcmp("stderr", s)==0) {
      ret = stderr;
    }
    else {
      FILE * t = fopen(s, "w+");
      if (t != nullptr) {
        ret = t;
      }
    }
  }
  return ret;
}

LogLevel Log::Level = SetupLogLevel();
FILE * Log::Device = SetupLogDevice();

static const int BUFFSIZE = 2048;
void Log::LogMessage(LogLevel level, const char * fmt, ...) {
  timeval log_timer = {0, 0};
  struct tm log_tm;
  gettimeofday(&log_timer, nullptr);
  localtime_r(&(log_timer.tv_sec), &log_tm);
  char buff[BUFFSIZE+1];
  int len1 = snprintf(buff, BUFFSIZE,
                    "%02d-%02d-%02d %02d:%02d:%02d.%03d %012llx %s ",
                    log_tm.tm_year % 100,
                    log_tm.tm_mon + 1,
                    log_tm.tm_mday,
                    log_tm.tm_hour,
                    log_tm.tm_min,
                    log_tm.tm_sec,
                    (int32_t)(log_timer.tv_usec / 1000),
                    (uint64_t)pthread_self(),
                    gLogLevels[level]);
  int rest = BUFFSIZE - len1;
  va_list al;
  va_start(al, fmt);
  int len2 = vsnprintf(buff + len1, rest, fmt, al);
  va_end(al);
  if (len2 > rest) {
    // truncate
    len2 = rest;
  }
  buff[len1 + len2] = '\n';
  ::fwrite(buff, len1 + len2 + 1, 1, Device);
}

static uint32 GetRandomSeed() {
  uint64 seed64 = Time::CurrentTimeNano() ^ Thread::CurrentId();
  return (seed64 >> 32) ^ (seed64 & 0xffffffff);
}

std::mt19937 gRandomGen(GetRandomSeed());
mutex gRandomGock;

uint32 Random::NextUInt32() {
  static std::uniform_int_distribution<uint32> dist;
  ScopeLock slock(gRandomGock);
  return dist(gRandomGen);
}

uint64 Random::NextUInt64() {
  static std::uniform_int_distribution<uint64> dist;
  ScopeLock slock(gRandomGock);
  return dist(gRandomGen);
}

float Random::NextFloat() {
  static std::uniform_real_distribution<float> dist;
  ScopeLock slock(gRandomGock);
  return dist(gRandomGen);
}

double Random::NextDouble() {
  static std::uniform_real_distribution<double> dist;
  ScopeLock slock(gRandomGock);
  return dist(gRandomGen);
}

void Random::NextBytes(uint32 len, string & bytes) {
  static std::uniform_int_distribution<uint64> dist;
  ScopeLock slock(gRandomGock);
  bytes.resize(len);
  uint8 * data = (uint8*)bytes.data();
  uint64 value;
  for (uint32 i = 0; i < len; i++) {
    if (i % 8 == 0) {
      value = dist(gRandomGen);
    }
    data[i] = (uint8)(value >> (i*8));
  }
}

void Random::NextVisableBytes(uint32 len, string & bytes) {
  static std::uniform_int_distribution<uint64> dist;
  ScopeLock slock(gRandomGock);
  bytes.resize(len);
  uint8 * data = (uint8*)bytes.data();
  uint64 value;
  for (uint32 i = 0; i < len; i++) {
    if (i % 8 == 0) {
      value = dist(gRandomGen);
    }
    data[i] = ((uint8)(value >> (i*8)) % (126-33)) + 33;
  }
}

void Random::NextUUID(string & uuid) {
  NextBytes(16, uuid);
  // set version
  // must be 0b0100xxxx
  uuid[6] &= 0x4F; //0b01001111
  uuid[6] |= 0x40; //0b01000000
  // set variant
  // must be 0b10xxxxxx
  uuid[8] &= 0xBF;
  uuid[8] |= 0x80;
}

}
}

