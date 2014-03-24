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

#include "FileSystem.h"

namespace hadoop {
namespace common {

FsPermission FsPermission::FileDefault(0666);
FsPermission FsPermission::DirDefault(0777);

void FsPermission::fromString(const string & s) {
  uint32 int32mask;
  sscanf("%o", s.c_str(), &int32mask);
  mask = int32mask;
}

string FsPermission::toString() const {
  return Strings::Format("%04o", mask);
}

string FsPermission::toLsString() const {
  string ret("-rwxrwxrwx");
  for (int i=0;i<9;i++) {
    if (((1<<i) & mask) == 0) {
      ret[9-i] = '-';
    }
  }
  if (PERMISSION_STICKY & mask) {
    if (PERMISSION_OTHER_X & mask) {
      ret[9] = 't';
    } else {
      ret[9] = 'T';
    }
  }
  return ret;
}

CreateFlag CreateFlag::Default(CreateFlagEnum::CREATE_FLAG_CREATE);

void CreateFlag::fromString(const string & s) {
  flag = Strings::toInt(s);
}

string CreateFlag::toString() const {
  return Strings::ToString(flag);
}

SyncFlag SyncFlag::Default(SYNC_FLAG_UPDATE_LENGTH);

void SyncFlag::fromString(const string & s) {
  flag = Strings::toInt(s);
}

string SyncFlag::toString() const {
  return Strings::ToString(flag);
}

string FileStatus::toString() const {
  return Strings::Format("FileStatus(path=%s, len=%llu, user=%s, group=%s)",
    path.c_str(), length, owner.c_str(), group.c_str());
}

Json FileStatus::toJson() const {
  // TODO:
  return Json::Dict();
}

void FileStatus::fromJson(Json & json) {
  // TODO:
}

ContentSummary::ContentSummary(uint64 length, uint64 fileCount,
    uint64 directoryCount, uint64 quota, uint64 spaceConsumed,
    uint64 spaceQuota) :
    length(length), fileCount(fileCount), directoryCount(directoryCount),
    quota(quota), spaceConsumed(spaceConsumed), spaceQuota(spaceQuota) {
}

ContentSummary::ContentSummary() :
    length(0), fileCount(0), directoryCount(0),
    quota(0), spaceConsumed(0), spaceQuota(0) {
}

string ContentSummary::toString() const {
  // quota can be -1 to indicate unlimited quota
  return Strings::Format(
      "Summary(length=%llu,file=%llu,dir=%llu,inodeQuota=%lld,consumed=%llu,"
      "spaceQuata=%lld)",
      length, fileCount, directoryCount, quota, spaceConsumed, spaceQuota);
}

Json ContentSummary::toJson() const {
  // TODO:
  return Json::Dict();
}

void ContentSummary::fromJson(Json & json) {
  // TODO:
}

string FileSystemStats::toString() const {
  return Strings::Format(
      "FsStats(capacity=%llu,used=%llu,remaining=%llu,underReplicated=%llu,"
      "corruptBlocks=%llu,missingBlocks=%llu,blockPoolUsed=%llu)",
      capacity, used, remaining, under_replicated, corrupt_blocks,
      missing_blocks, blockPoolUsed);
}

Json FileSystemStats::toJson() const {
  // TODO:
  return Json::Dict();
}

void FileSystemStats::fromJson(Json & json) {
}

string FsServerDefaults::toString() const {
  return Strings::Format(
      "FsServerDefaults(blockSize=%llu,bytesPerChecksum=%llu)",
      blockSize,
      bytesPerChecksum);
}

Json FsServerDefaults::toJson() const {
  // TODO:
  return Json::Dict();
}

void FsServerDefaults::fromJson(Json & json) {
  // TODO:
}

}
}
