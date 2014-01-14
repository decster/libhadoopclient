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

#ifndef BUFFER_H_
#define BUFFER_H_

#include "common.h"

namespace google {
namespace protobuf {
class MessageLite;
}
}

namespace hadoop {
namespace common {

class Buffer {
public:
  char * data;
  uint64 pos;
  uint64 limit;
  uint64 capacity;

  explicit Buffer(Buffer &);
  explicit Buffer(uint64 capacity = 0);
  ~Buffer();
  void release();
  string toString();

  char * current() {
    return data + pos;
  }
  uint64 remain() {
    return limit - pos;
  }
  void rewind() {
    limit = pos;
    pos = 0;
  }
  void reset() {
    pos = 0;
  }
  void reset(uint64 limit);
  void expandLimit(uint64 length);

  uint64 read(void * dst, uint64 length);
  uint64 write(const void * src, uint64 length);

  // util methods for primitive type serialization
  // note: caller must ensure there is enough data/space in buffer
  uint8 readUInt8() {
    uint8 ret = *(int8*) (data + pos);
    pos += 1;
    return ret;
  }
  uint16 readUInt16() {
    uint16 ret = *(int16*) (data + pos);
    pos += 2;
    return ret;
  }
  uint32 readUInt32() {
    uint32 ret = *(uint32*) (data + pos);
    pos += 4;
    return ret;
  }
  uint64 readUInt64() {
    uint64 ret = *(uint64*) (data + pos);
    pos += 8;
    return ret;
  }
  int16 readUInt16Be() {
    return HToBe(readUInt16());
  }
  int32 readUInt32Be() {
    return HToBe(readUInt32());
  }
  int64 readUInt64Be() {
    return HToBe(readUInt64());
  }
  void writeUInt8(uint8 v) {
    *(uint8*) (data + pos) = v;
    pos += 1;
  }
  void writeUInt16(uint16 v) {
    *(uint16*) (data + pos) = v;
    pos += 2;
  }
  void writeUInt32(uint32 v) {
    *(uint32*) (data + pos) = v;
    pos += 4;
  }
  void writeUInt64(uint64 v) {
    *(uint64*) (data + pos) = v;
    pos += 8;
  }
  void writeUInt16Be(uint16 v) {
    writeUInt16(HToBe(v));
  }
  void writeUInt32Be(uint32 v) {
    writeUInt32(HToBe(v));
  }
  void writeInt64Be(uint64 v) {
    writeUInt64(HToBe(v));
  }
  uint32 readVarint32();
  void writeVarint32(uint32 v);

  // util methods for protobuf serialization, return total bytes processed
  uint32 readUInt32BePrefixed(::google::protobuf::MessageLite & msg);
  uint32 readVarInt32Prefixed(::google::protobuf::MessageLite & msg);
  // Try read a vint32 prefixed protobuf, if data not enough, return 0
  // else return byte consumed
  uint32 tryReadVarInt32Prefixed(::google::protobuf::MessageLite & msg);
  // note: caller must already called msg.GetSize and
  //       ensure there is enough space
  uint32 writeUInt32BePrefixed(::google::protobuf::MessageLite & msg);
  uint32 writeVarInt32Prefixed(::google::protobuf::MessageLite & msg);

  // util methods for file/network io
  // read/write once
  int64 read(int fd);
  int64 write(int fd);
  // read/write as much as possible,
  // throw exception if fail equals true and can not process enough data
  int64 readAll(int fd, bool fail=false);
  int64 writeAll(int fd, bool fail=false);

  // it is somehow difficult to read an varint from handler(blocking), because
  // varint does not have fixed length, we have to read from the stream byte
  // by byte. so a helper method is provided here, it uses this buffer as
  // working buffer.
  void readVarInt32Prefixed(int fd, ::google::protobuf::MessageLite & msg);

  void writeVarInt32Prefixed(int fd, ::google::protobuf::MessageLite & msg);
};

}
}

#endif /* BUFFER_H_ */
