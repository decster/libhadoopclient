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

#ifndef EXCEPTIONS_H_
#define EXCEPTIONS_H_


namespace hadoop {
namespace common {

/**
 * Hadoop client related exceptions
 * Unlike java which has multiple kinds of exceptions,
 * Here only one general exception type is used because:
 * exception in c++ has many pitfalls, it's better to limit
 * its usage.
 */
class Exception: public std::exception {
public:
  enum Type {
    NoError = 0,
    UnknownException = 1,
    IllegalArgumentException = 2,
    OutOfMemoryException = 3,
    UnsupportException = 4,
    IOException = 5,
    AccessControlException = 6,
    ChecksumException = 7,
    InvalidBlockTokenException = 8,
    BlockMissingException = 9,
    InvalidEncryptionKeyException = 10,
    NotReplicatedYetException = 11,
  };

  // type enum to string
  static const char * TypeToString(Type type);
  // type enum to posix errno
  static int TypeToErrno(Type type);
  // java exception type to type enum
  static Type JavaToNative(const string & javaName);

protected:
  Type _type;
  string _reason;
  string _stackTrace;

public:
  Exception(Type type, const char * where, const string & what);
  Exception(const char * where, const string & what);
  Exception(const string & what);
  virtual ~Exception() throw () {
  }

  virtual const char* what() const throw () {
    return _reason.c_str();
  }

  const Type type() {
    return _type;
  }

  string & reason() {
    return _reason;
  }

  string & stackTrace() {
    return _stackTrace;
  }

  void logWarn();

  void logInfo();

  static void SetLogException(bool logException);
};

#define STRINGIFY(x) #x
#define TOSTRING(x) STRINGIFY(x)
#define AT __FILE__ ":" TOSTRING(__LINE__)
#define THROW_EXCEPTION(type, what) throw Exception(Exception::type, (AT), (what))
#define THROW_EXCEPTION_EX(type, fmt, args...) \
        throw Exception(Exception::type, (AT), Strings::Format(fmt, ##args))

void HadoopAssert(bool p, const char * where, const char * what);
#define HADOOP_ASSERT(p, msg) HadoopAssert((p), (AT), (msg))

}
}


#endif /* EXCEPTIONS_H_ */
