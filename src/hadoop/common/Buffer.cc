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

#include "Buffer.h"

#include <stdlib.h>
#include <sys/errno.h>
#include <unistd.h>
#include <cstring>

#include "google/protobuf/message_lite.h"
#include "exceptions.h"


namespace hadoop {
namespace common {

Buffer::Buffer(Buffer & from) : data(nullptr), pos(0), limit(0), capacity(0) {
  reset(from.limit);
  if (limit > 0) {
    memcpy(data, from.data, limit);
  }
}

string Buffer::toString() {
  string ret = Strings::Format("[%llu/%llu/%llu[", pos, limit, capacity);
  int i = 0;
  for (;i < 32 && i < limit; i++) {
    ret.append(Strings::Format("%02x", (uint8)data[i]));
  }
  if (i < limit) {
    ret.append("...");
  }
  ret.append("]]");
  return ret;
}


Buffer::Buffer(uint64 capacity) :
    data(nullptr), pos(0), limit(0), capacity(0) {
  reset(capacity);
  this->capacity = capacity;
}

Buffer::~Buffer() {
  if (data != nullptr) {
    free(data);
    data = nullptr;
  }
}

void Buffer::release() {
  if (data != nullptr) {
    free(data);
    data = nullptr;
    pos = 0;
    limit = 0;
    capacity = 0;
  }
}

void Buffer::reset(uint64 limit) {
  if (limit > capacity) {
    data = (char*)malloc(limit);
    if (data == nullptr) {
      THROW_EXCEPTION_EX(OutOfMemoryException, "malloc failed(size=%lu)",
          limit);
    }
    this->capacity = limit;
  }
  this->pos = 0;
  this->limit = limit;
}

void Buffer::expandLimit(uint64 length) {
  uint64 newlimit = limit + length;
  if (newlimit <= capacity) {
    limit = newlimit;
  } else {
    char * newdata = (char*)malloc(newlimit);
    if (newdata == nullptr) {
      THROW_EXCEPTION_EX(OutOfMemoryException, "malloc failed(size=%lu)",
          newlimit);
    }
    if (limit > 0) {
      memcpy(newdata, data, limit);
    }
    if (data != nullptr) {
      free(data);
    }
    data = newdata;
    limit = newlimit;
    capacity = newlimit;
  }
}

uint64 Buffer::read(void * dst, uint64 length) {
  uint64 cp = std::min(remain(), length);
  if (cp > 0) {
    ::memcpy(dst, current(), cp);
    pos += cp;
  }
  return cp;
}

uint64 Buffer::write(const void * src, uint64 length) {
  uint64 cp = std::min(remain(), length);
  if (cp > 0) {
    ::memcpy(current(), src, cp);
    pos += cp;
  }
  return cp;
}

uint32 Buffer::readVarint32() {
  uint32 result;
  uint8 b;
  b = (uint8)data[pos++];
  result = b & 0x7F;
  if ((b & 0x80) == 0) {
    return result;
  }
  b = (uint8)data[pos++];
  result |= (uint32)(b & 0x7F) << 7;
  if ((b & 0x80) == 0) {
    return result;
  }
  b = (uint8)data[pos++];
  result |= (uint32)(b & 0x7F) << 14;
  if ((b & 0x80) == 0) {
    return result;
  }
  b = (uint8)data[pos++];
  result |= (uint32)(b & 0x7F) << 21;
  if ((b & 0x80) == 0) {
    return result;
  }
  b = (uint8)data[pos++];
  result |= (uint32)(b & 0x7F) << 28;
  if ((b & 0x80) == 0) {
    return result;
  }
  THROW_EXCEPTION(IOException, "readVarint32 failed, bad format");
}

void Buffer::writeVarint32(uint32 v) {
  uint8 * ud = (uint8*)(data + pos);
  if (v < (1U << 7)) {
    ud[0] = (uint8)v;
    pos += 1;
  } else if (v < (1U << 14)) {
    ud[0] = v | 0x80;
    ud[1] = (v >> 7)  & 0xff;
    pos += 2;
  } else if (v < (1U << 21)) {
    ud[0] = v | 0x80;
    ud[1] = (v >> 7)  | 0x80;
    ud[2] = (v >> 14) & 0xff;
    pos += 3;
  } else if (v < (1U << 28)) {
    ud[0] = v | 0x80;
    ud[1] = (v >> 7)  | 0x80;
    ud[2] = (v >> 14) | 0x80;
    ud[3] = (v >> 21) & 0xff;
    pos += 4;
  } else {
    ud[0] = v | 0x80;
    ud[1] = (v >> 7)  | 0x80;
    ud[2] = (v >> 14) | 0x80;
    ud[3] = (v >> 21) | 0x80;
    ud[4] = (v >> 28) & 0xff;
    pos += 5;
  }
}

uint32 Buffer::readUInt32BePrefixed(::google::protobuf::MessageLite & msg) {
  uint64 oldpos = pos;
  int32 size = readUInt32Be();
  if (!msg.ParseFromArray(current(), size)) {
    THROW_EXCEPTION(IOException, "parse protobuf msg error");
  }
  pos += size;
  return pos - oldpos;
}

uint32 Buffer::readVarInt32Prefixed(::google::protobuf::MessageLite & msg) {
  uint64 oldpos = pos;
  uint32 size = readVarint32();
  if (!msg.ParseFromArray(current(), size)) {
    THROW_EXCEPTION(IOException, "parse protobuf msg error");
  }
  pos += size;
  return pos - oldpos;
}

uint32 Buffer::tryReadVarInt32Prefixed(::google::protobuf::MessageLite & msg) {
  bool hasVarint = false;
  if (remain() >= 5) {
    hasVarint = true;
  } else {
    for (uint64 p = pos; p < limit; p++) {
      if (((uint8)data[p]) < 0x80) {
        hasVarint = true;
        break;
      }
    }
  }
  if (!hasVarint) {
    return 0;
  }
  uint64 oldpos = pos;
  uint32 size = readVarint32();
  if (remain() < size) {
    pos = oldpos;
    return 0;
  }
  if (!msg.ParseFromArray(current(), size)) {
    pos = oldpos;
    THROW_EXCEPTION(IOException, "parse protobuf msg error");
  }
  pos += size;
  return pos - oldpos;
}

uint32 Buffer::writeUInt32BePrefixed(::google::protobuf::MessageLite & msg) {
  uint64 oldpos = pos;
  int32 size = msg.GetCachedSize();
  writeUInt32Be(size);
  msg.SerializeWithCachedSizesToArray((uint8*)current());
  pos += size;
  return pos - oldpos;
}

uint32 Buffer::writeVarInt32Prefixed(::google::protobuf::MessageLite & msg) {
  uint64 oldpos = pos;
  int32 size = msg.GetCachedSize();
  writeVarint32(size);
  msg.SerializeWithCachedSizesToArray((uint8*)current());
  pos += size;
  return pos - oldpos;
}

int64 Buffer::read(int fd) {
  int64 total = remain();
  if (total == 0) {
    return 0;
  }
  errno = 0;
  int64 rd = ::read(fd, data + pos, total);
  if (rd >= 0) {
    pos += rd;
    return rd;
  } else if (errno != EAGAIN) {
    THROW_EXCEPTION_EX(IOException,
        "read(fd=%d, pos=%llu, limit=%llu, len=%llu) ret=%lld failed: %d %s",
        fd, pos, limit, total, rd, errno, strerror(errno));
  }
  return 0;
}

int64 Buffer::write(int fd) {
  int64 total = remain();
  if (total == 0) {
    return 0;
  }
  errno = 0;
  int64 wt = ::write(fd, data + pos, total);
  if (wt >= 0) {
    pos += wt;
    return wt;
  } else if (errno != EAGAIN) {
    THROW_EXCEPTION_EX(IOException,
        "write(fd=%d, pos=%llu, limit=%llu, len=%llu) ret=%lld failed: %d %s",
        fd, pos, limit, total, wt, errno, strerror(errno));
  }
  return 0;
}

int64 Buffer::readAll(int fd, bool fail) {
  uint64 oldPos = pos;
  while (remain() > 0) {
    errno = 0;
    int64 rd = ::read(fd, current(), remain());
    if (rd == 0) {
      // EOF
      break;
    } else if (rd < 0) {
      if (errno != EAGAIN) {
        THROW_EXCEPTION_EX(IOException, "readAll(fd=%d) failed: %s", fd,
            strerror(errno));
      }
      break;
    }
    pos += rd;
  }
  if (fail && remain() > 0) {
    THROW_EXCEPTION_EX(IOException,
        "readAll(fd=%d, len=%llu) cannot read enough data", fd, limit - oldPos);
  }
  return pos - oldPos;
}

int64 Buffer::writeAll(int fd, bool fail) {
  uint64 oldPos = pos;
  while (remain() > 0) {
    errno = 0;
    int64 wt = ::write(fd, current(), remain());
    if (wt == 0) {
      // EOF
      break;
    } else if (wt < 0) {
      if (errno != EAGAIN) {
        THROW_EXCEPTION_EX(IOException, "writeAll(fd=%d) failed: %s", fd,
            strerror(errno));
      }
      break;
    }
    pos += wt;
  }
  if (fail && remain() > 0) {
    THROW_EXCEPTION_EX(IOException,
        "writeAll(fd=%d, len=%llu) cannot write enough data", fd, limit - oldPos);
  }
  return pos - oldPos;
}

void Buffer::readVarInt32Prefixed(int fd,
    ::google::protobuf::MessageLite & msg) {
  uint32 bodyLength = 0;
  uint8 bt;
  int shift = 0;
  do {
    if (::read(fd, &bt, 1) != 1) {
      THROW_EXCEPTION_EX(IOException,
          "readVarInt32Prefixed read varint(fd=%d, len=1) failed", fd);
    }
    bodyLength |= ((bt & 0x7fU) << shift);
    shift += 7;
  } while (bt & 0x80U);
  reset(bodyLength);
  if (bodyLength != readAll(fd)) {
    THROW_EXCEPTION_EX(IOException,
        "readVarInt32Prefixed read proto message body(fd=%d, len=%u) failed",
        fd, bodyLength);
  }
  msg.ParseFromArray(data, pos);
  reset(0);
}

void Buffer::writeVarInt32Prefixed(int fd, ::google::protobuf::MessageLite & msg) {
  int maxSize = 5 + msg.ByteSize();
  reset(maxSize);
  uint32 total = writeVarInt32Prefixed(msg);
  rewind();
  if (total != writeAll(fd)) {
    THROW_EXCEPTION_EX(IOException,
        "writeVarInt32Prefixed(fd=%d, len=%u) failed", fd, total);
  }
  reset(0);
}

}
}
