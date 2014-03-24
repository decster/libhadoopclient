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

#ifndef FILESYSTEM_H_
#define FILESYSTEM_H_

#include "Common.h"
#include "Checksum.h"
#include "Json.h"

namespace hadoop {
namespace common {

enum FsPermissionEnum {
  PERMISSION_SET_UID = 04000,
  PERMISSION_SET_GID = 02000,
  PERMISSION_STICKY = 01000,
  PERMISSION_USER_R = 0400,
  PERMISSION_USER_W = 0200,
  PERMISSION_USER_X = 0100,
  PERMISSION_GROUP_R = 0040,
  PERMISSION_GROUP_W = 0020,
  PERMISSION_GROUP_X = 0001,
  PERMISSION_OTHER_R = 0004,
  PERMISSION_OTHER_W = 0002,
  PERMISSION_OTHER_X = 0001,
};

class FsPermission {
public:
  uint16 mask;
  FsPermission(uint16 mask) : mask(mask) {}
  void fromString(const string & s);
  string toString() const;
  string toLsString() const;

  static FsPermission FileDefault;
  static FsPermission DirDefault;
};

enum CreateFlagEnum {
  CREATE_FLAG_CREATE = 1,
  CREATE_FLAG_OVERWRITE = 2,
  CREATE_FLAG_APPEND = 4,
  CREATE_FLAG_SYNC_BLOCK = 8,
};

class CreateFlag {
public:
  uint16 flag;
  CreateFlag(uint16 flag) : flag(flag) {}
  bool hasOverWrite() {
    return flag & CREATE_FLAG_APPEND;
  }
  bool hasAppend() {
    return flag & CREATE_FLAG_APPEND;
  }
  bool hasSyncBlock() {
    return flag & CREATE_FLAG_SYNC_BLOCK;
  }
  void fromString(const string & s);
  string toString() const;

  static CreateFlag Default;
};

enum SyncFlagEnum {
  SYNC_FLAG_UPDATE_LENGTH = 1,
};

class SyncFlag {
public:
  uint32 flag;
  SyncFlag(uint32 flag) : flag(flag) {}
  void fromString(const string & s);
  string toString() const;

  static SyncFlag Default;
};

enum FileType {
  FILE_TYPE_DIR = 1,
  FILE_TYPE_FILE = 2,
  FILE_TYPE_SYMLINK = 3,
  FILE_TYPE_OTHER = 4,
};

class FileStatus {
public:
  FileType type;
  string path;
  int64 length;
  string symlink;
  string owner;
  string group;
  FsPermission permission;
  uint32 blockReplication;
  uint64 blockSize;
  // the modification time of file in milliseconds since January 1, 1970 UTC
  uint64 modificationTime;
  // the access time of file in milliseconds since January 1, 1970 UTC
  uint64 accessTime;
  uint64 fileId;
  int32 childrenNum;

  FileStatus() :
    type(FILE_TYPE_OTHER),
    length(0),
    permission(0),
    blockReplication(0),
    blockSize(0),
    modificationTime(0),
    accessTime(0),
    fileId(0),
    childrenNum(-1) {
  }

  string toString() const;

  bool isFile() const {
    return type == FileType::FILE_TYPE_FILE;
  }

  bool isDirectory() const {
    return type == FileType::FILE_TYPE_DIR;
  }

  Json toJson() const;
  void fromJson(Json & json);
};

class ContentSummary {
public:
  uint64 length;
  uint64 fileCount;
  uint64 directoryCount;
  uint64 quota;
  uint64 spaceConsumed;
  uint64 spaceQuota;

  ContentSummary(uint64 length, uint64 fileCount, uint64 directoryCount,
      uint64 quota, uint64 spaceConsumed, uint64 spaceQuota);
  ContentSummary();

  string toString() const;

  Json toJson() const;
  void fromJson(Json & json);
};

struct FileSystemStats {
  uint64 capacity;
  uint64 used;
  uint64 remaining;
  uint64 under_replicated;
  uint64 corrupt_blocks;
  uint64 missing_blocks;
  uint64 blockPoolUsed;
  string toString() const;
  Json toJson() const;
  void fromJson(Json & json);
};

class FsServerDefaults {
public:
  uint64 blockSize;
  uint32 bytesPerChecksum;
  uint32 writePacketSize;
  uint32 replication;
  uint32 fileBufferSize;
  bool encryptDataTransfer;
  uint64 trashInterval;
  ChecksumType checksumType;
  string toString() const;
  Json toJson() const;
  void fromJson(Json & json);
};

}
}

#endif /* FILESYSTEM_H_ */
