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

/**
 * Common header files, most common used util classes
 * include string, time, thread, log and various exceptions
 */

#ifndef COMMON_H_
#define COMMON_H_

#include <unistd.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <stdexcept>
#include <string>
#include <vector>
#include <deque>
#include <map>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <random>
#include <memory>
#include <thread>
#include <mutex>
#include <functional>

#include <json/json.h>

namespace hadoop {

// TODO: add c++11 detection
// #define __cplusplus 201103L

using std::string;
using std::vector;
using std::map;
using std::set;
using std::pair;
using std::make_pair;
using std::unique_ptr;
using std::shared_ptr;
using std::thread;
using std::mutex;
using std::lock_guard;
using std::unique_lock;
using std::condition_variable;

typedef int8_t int8;
typedef uint8_t uint8;
typedef int16_t int16;
typedef uint16_t uint16;
typedef int32_t int32;
typedef uint32_t uint32;
typedef int64_t int64;
typedef uint64_t uint64;
typedef lock_guard<mutex> ScopeLock;
typedef unique_lock<mutex> UniqueLock;
typedef condition_variable Condition;

}

namespace hadoop {
namespace common {

/**
 * byte order conversion utils
 */
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
inline uint16 HToBe(uint16 v) {
  return (v >> 8) | (v << 8);
}
inline uint32 HToBe(uint32 v) {
  return ((v & 0xff000000) >> 24) |
         ((v & 0x00ff0000) >> 8) |
         ((v & 0x0000ff00) << 8) |
         ((v & 0x000000ff) << 24);
}
inline uint64 HToBe(uint64 v) {
  return ((v & 0xff00000000000000ULL) >> 56) |
         ((v & 0x00ff000000000000ULL) >> 40) |
         ((v & 0x0000ff0000000000ULL) >> 24) |
         ((v & 0x000000ff00000000ULL) >>  8) |
         ((v & 0x00000000ff000000ULL) <<  8) |
         ((v & 0x0000000000ff0000ULL) << 24) |
         ((v & 0x000000000000ff00ULL) << 40) |
         ((v & 0x00000000000000ffULL) << 56);
}
#else
inline uint16 HToBe(uint16 v) {
  return v;
}
inline uint32 HToBe(uint32 v) {
  return v;
}
inline uint64 HToBe(uint64 v) {
  return v;
}
#endif
inline int16 HToBe(int16 v) {
  return (int16)HToBe((uint16)v);
}
inline int32 HToBe(int32 v) {
  return (int32)HToBe((uint32)v);
}
inline int64 HToBe(int64 v) {
  return (int64)HToBe((uint64)v);
}

/**
 * String conversion utils
 */
class Strings {
public:
  static string ToString(int32 v);
  static string ToString(uint32 v);
  static string ToString(int64 v);
  static string ToString(int64 v, char pad, int64 len);
  static string ToString(uint64 v);
  static string ToString(bool v);
  static string ToString(float v);
  static string ToString(double v);
  static string ToString(const vector<string> & vs);

  static int64 toInt(const string & str);

  static bool toBool(const string & str);

  static float  toFloat(const string & str);

  static string Format(const char * fmt, ...);

  static void Append(string & dest, const char * fmt, ...);

  static string ToUpper(const string & name);

  static string ToLower(const string & name);

  static string Trim(const string & str);

  static void Split(const string & src, const string & sep,
                    vector<string> & dest, bool clean = false);

  static string Join(const vector<string> & strs,
                          const string & sep);

  static string ReplaceAll(const string & src, const string & find,
      const string & replace);

  static bool StartsWith(const string & str, const string & prefix);

  static bool EndsWith(const string & str, const string & suffix);

  static int64 LastSeparator(const string & str, char sep);

  static string LastPart(const string & str, char sep);

  static const char * LastPartStart(const string & str, char sep);

  static string toBase64(const string & str);

  static string fromBase64(const string & str);
};

/**
 * time primitives
 */
class Time {
public:
  static const int64 MAX = 0x7fffffffffffffffL;
  // monotonic timer in ns
  static int64 CurrentMonoNano();
  // monotonic timer in ms
  static int64 CurrentMonoMill() {
    return CurrentMonoNano() / 1000000;
  }
  // ns since epoch
  static int64 CurrentTimeNano();
  // ms since epoch
  static int64 CurrentTimeMill() {
    return CurrentTimeNano() / 1000000;
  }
  static int64 Timeout(int64 timeout) {
    if (timeout > 0) {
      int64 ret = Time::CurrentMonoMill() + timeout;
      return ret > 0 ? ret : MAX;
    } else {
      return MAX;
    }
  }
};

/**
 * thread utils
 */
class Thread {
public:
  static uint64 CurrentId();
  static void Sleep(uint32 millisec);
  static void AddStackTrace(string & dst);
  static void LogStackTrace(const char * msg);
};

/**
 * log utils
 */
enum LogLevel {
  LOG_LEVEL_DEBUG = 0,
  LOG_LEVEL_INFO = 1,
  LOG_LEVEL_WARN = 2,
  LOG_LEVEL_ERROR = 2,
  LOG_LEVEL_FATAL = 3
};

class Log {
protected:
  static LogLevel Level;
  static FILE * Device;
public:
  static LogLevel GetLevel() {
    return Level;
  }
  static void SetLevel(LogLevel level) {
    Level = level;
  }
  static FILE * GetDevice() {
    return Device;
  }
  static void SetDevice(FILE * device) {
    Device = device;
  }
  static void LogMessage(LogLevel level, const char * fmt, ...);
};

#define LOG_EX(level, fmt, args...) \
  if (::hadoop::common::Log::GetDevice() &&\
      level >= ::hadoop::common::Log::GetLevel()) {\
    ::hadoop::common::Log::LogMessage(level, fmt, ##args);\
  }
#define LOG_DEBUG(fmt, args...) LOG_EX(::hadoop::common::LOG_LEVEL_DEBUG, fmt, ##args)
#define LOG_INFO(fmt, args...) LOG_EX(::hadoop::common::LOG_LEVEL_INFO, fmt, ##args)
#define LOG_WARN(fmt, args...) LOG_EX(::hadoop::common::LOG_LEVEL_WARN, fmt, ##args)
#define LOG_FATAL(fmt, args...) LOG_EX(::hadoop::common::LOG_LEVEL_FATAL, fmt, ##args)
#define LOG(fmt, args...) LOG_INFO(fmt, ##args)

/**
 * Random utils
 * use lock for multi-thread synchronization
 */
class Random {
public:
  static uint32 NextUInt32();
  static uint64 NextUInt64();
  static float NextFloat();
  static double NextDouble();
  static void NextBytes(uint32 len, string & bytes);
  static void NextVisableBytes(uint32 len, string & text);
  static void NextUUID(string & uuid);
};

}
}

#include "Exceptions.h"


#endif /* COMMON_H_ */
