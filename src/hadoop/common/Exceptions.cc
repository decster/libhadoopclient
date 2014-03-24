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

#include "Common.h"
#include "Exceptions.h"

namespace hadoop {
namespace common {

static const char * TypeNames[] = {
  "NoError",
  "UnknownException",
  "IllegalArgumentException",
  "OutOfMemoryException",
  "UnsupportException",
  "IOException",
  "AccessControlException",
  "ChecksumException",
  "InvalidBlockTokenException",
  "BlockMissingException",
  "InvalidEncryptionKeyException",
  "NotReplicatedYetException",
};

const char * Exception::TypeToString(Type type) {
 if (type < sizeof(TypeNames)/sizeof(const char *)) {
   return TypeNames[type];
 } else {
   return TypeNames[1];
 }
}

int Exception::TypeToErrno(Type type) {
  switch (type) {
    case IllegalArgumentException:
      return EINVAL;
    case OutOfMemoryException:
      return ENOMEM;
    case UnsupportException:
      return ENOTSUP;
    case AccessControlException:
      return EACCES;
    default:
      return type;
  }
}

struct JavaTypeMapEntry {
  const char * javaTypeName;
  Exception::Type nativeType;
};

static const JavaTypeMapEntry JavaTypeMap[] =
{
  {
    "java.lang.IllegalArgumentException",
    Exception::IllegalArgumentException
  },
  {
    "java.lang.OutOfMemoryError",
    Exception::OutOfMemoryException
  },
  {
    "java.io.IOException",
    Exception::IOException
  },
  {
    "org.apache.hadoop.security.AccessControlException",
    Exception::AccessControlException
  },
  {
    "org.apache.hadoop.fs.ChecksumException",
    Exception::ChecksumException
  },
  {
    "org.apache.hadoop.hdfs.security.token.block.InvalidBlockTokenException",
    Exception::InvalidBlockTokenException
  },
  {
    "org.apache.hadoop.hdfs.BlockMissingException",
    Exception::BlockMissingException
  },
  {
    "org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException",
    Exception::InvalidEncryptionKeyException
  },
  {
    "org.apache.hadoop.hdfs.server.namenode.NotReplicatedYetException",
    Exception::NotReplicatedYetException
  },
  {
    nullptr,
    Exception::NoError
  },
};

Exception::Type Exception::JavaToNative(const string & javaName) {
  for (size_t i = 0; JavaTypeMap[i].javaTypeName; i++) {
    if (javaName == JavaTypeMap[i].javaTypeName) {
      return JavaTypeMap[i].nativeType;
    }
  }
  return IOException;
}


// true for easy debug
static bool GLogException = false;

void Exception::SetLogException(bool logException) {
  GLogException = logException;
}

Exception::Exception(Type type, const char * where,
    const string & what) {
  _type = type;
  _reason = what;
  if (where != nullptr) {
    _stackTrace = where;
    _stackTrace.append("\n");
  }
  Thread::AddStackTrace(_stackTrace);
  if (GLogException || getenv("DEBUG")) {
    LOG_WARN("%s(%s) thrown at:\n%s",
        TypeToString(_type), _reason.c_str(), _stackTrace.c_str());
  }
}

Exception::Exception(const char * where, const string & what) {
  _type = UnknownException;
  _reason = what;
  if (where != nullptr) {
    _stackTrace = where;
    _stackTrace.append("\n");
  }
  Thread::AddStackTrace(_stackTrace);
  if (GLogException || getenv("DEBUG")) {
    LOG_WARN("%s(%s) thrown at:\n%s",
        TypeToString(_type), _reason.c_str(), _stackTrace.c_str());
  }
}

Exception::Exception(const string & what) {
  _type = UnknownException;
  _reason = what;
  Thread::AddStackTrace(_stackTrace);
  if (GLogException || getenv("DEBUG")) {
    LOG_WARN("%s(%s) thrown at:\n%s",
        TypeToString(_type), _reason.c_str(), _stackTrace.c_str());
  }
}

void Exception::logWarn() {
  LOG_WARN("%s(%s) thrown at:\n%s",
      TypeToString(_type), _reason.c_str(), _stackTrace.c_str());
}

void Exception::logInfo() {
  LOG_INFO("%s(%s) thrown at:\n%s",
      TypeToString(_type), _reason.c_str(), _stackTrace.c_str());
}

void HadoopAssert(bool p, const char * where, const char * what) {
  if (!p) {
    throw Exception(Exception::IOException, where, what);
  }
}

}
}
