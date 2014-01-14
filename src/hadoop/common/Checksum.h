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

#ifndef CHECKSUM_H_
#define CHECKSUM_H_

#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <algorithm>

namespace hadoop {
namespace common {

enum ChecksumType {
  CHECKSUM_NONE = 0,
  CHECKSUM_CRC32 = 1,
  CHECKSUM_CRC32C = 2,
};



class Checksum {
protected:
  ChecksumType _type;
  uint64_t _bytesPerChecksum;

public:
  Checksum(const Checksum & cs) :
      _type(cs.type()), _bytesPerChecksum(cs.bytesPerChecksum()) {
  }

  Checksum(ChecksumType type = CHECKSUM_NONE, uint64_t bytesPerChecksum = 512) :
      _type(type), _bytesPerChecksum(bytesPerChecksum) {
  }

  void reset(ChecksumType type, uint64_t bytesPerChecksum) {
    _type = type;
    _bytesPerChecksum = bytesPerChecksum;
  }

  ChecksumType type() const {
    return _type;
  }

  uint64_t bytesPerChecksum() const {
    return _bytesPerChecksum;
  }

  uint32_t checksumSize() const {
    return 4;
  }

  uint64_t chunks(uint64_t size) const {
    return (size + _bytesPerChecksum - 1) / _bytesPerChecksum;
  }

  /**
   * validate a chunk of data with one checksum(stored in network(big-endian) order)
   * return true if validate success
   */
  bool validateOne(const char * data, uint64_t length, const char * checksum) const {
    uint32_t cs = init(_type);
    update(_type, cs, data, length);
    return *((uint32_t*)checksum) == ntohl(getValue(_type, cs));
  }

  void checksumOne(const char * data, uint64_t length, char * checksum) const {
    uint32_t cs = init(_type);
    update(_type, cs, data, length);
    *((uint32_t*)checksum) = ntohl(getValue(_type, cs));
  }

  /**
   * validate chunks of data with checksums(stored in network(big-endian) order)
   * return nullptr is success, or return start address of corrupt block
   */
  const char * validate(const char * data, uint64_t dataLen,
      const char * checksum) const;

  /**
   * compute checksums of data chunks(size bytesPerChecksum)
   */
  void checksum(const char * data, uint64_t dataLen, char * checksum) const;

  static uint32_t init(ChecksumType type);

  static void update(ChecksumType type, uint32_t & value, const void * buff,
      uint64_t length);

  static uint32_t getValue(ChecksumType type, uint32_t value);
};

}
}

#endif /* CHECKSUM_H_ */
