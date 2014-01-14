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

#ifndef CLIENTNAMENODEPROTOCOL_H_
#define CLIENTNAMENODEPROTOCOL_H_

#include "hadoop/common/Json.h"
#include "hadoop/common/RpcClient.h"
#include "hadoop/common/FileSystem.h"
#include "Hdfs.h"

namespace hadoop {
namespace hdfs {

using namespace hadoop::common;

class DirectoryListing {
public:
  vector<FileStatus> status;
  vector<LocatedBlocks> locations;
  int32 remainingEntries;
};

class ClientNamenodeProtocol {
public:
  const static int VERSION = 1;

  ClientNamenodeProtocol(ClientNamenodeProtocol &) = delete;

  ClientNamenodeProtocol(const User & user, const string & addr) :
      _client("org.apache.hadoop.hdfs.protocol.ClientProtocol", VERSION, user,
          addr) {
  }

  ~ClientNamenodeProtocol() {
  }

  RpcClient & getClient();

  void getBlockLocations(const string & src, uint64 offset, uint64 length,
      LocatedBlocks & blocks);

  void getServerDefaults(FsServerDefaults & defaults);

  void create(const string & src, FsPermission masked,
      const string & clientName, CreateFlag flag, bool createParent,
      uint16 replication, uint64 blockSize, FileStatus & st);

  void append(const string & src, const string & clientName,
      LocatedBlock & block);

  bool setReplication(const string & src, uint16_t replicaiton);

  void setPermission(const string & src, FsPermission permission);

  void setOwner(const string & src, const string & user, const string & group);

  void abandonBlock(ExtendedBlock & block, const string & src,
      const string & holder);

  void addBlock(const string & src, uint64 fileId, const string & clientName,
      ExtendedBlock * previous, const vector<DatanodeInfo> & excludeNodes,
      const vector<string> favorNodes,
      LocatedBlock & locatedBlock);

  void getAdditionalDatanode(const string & src, ExtendedBlock & blk,
      vector<DatanodeInfo> & existings, vector<DatanodeInfo> & excludes,
      uint32_t numAdditionalNodes, const string & clientName,
      LocatedBlock & locatedBlock);

  bool complete(const string & src, uint64 fileId, const string & clientName,
      ExtendedBlock * last);

  void reportBadBlocks(vector<LocatedBlock> & blocks);

  bool rename(const string & src, const string & dst);

  void concat(const string & trg, const vector<string> & srcs);

  void rename2(const string & src, const string & dst, bool overwrite);

  bool delete_(const string & src, bool recursive);

  bool mkdirs(const string & src, FsPermission masked, bool createParent);

  bool getListing(const string & src, const string & startAfter,
      bool needLocation, DirectoryListing & listing);

  void renewLease(const string & clientName);

  bool recoverLease(const string & src, const string & clientName);

  void getFsStats(FileSystemStats & stats);

  enum DatanodeReportType {
    DATANODEREPORTTYPE_ALL = 1,
    DATANODEREPORTTYPE_LIVE = 2,
    DATANODEREPORTTYPE_DEAD = 3,
  };

  void getDatanodeReport(DatanodeReportType type,
      vector<DatanodeInfo> & datanodes);

  uint64 getPreferredBlockSize(const string & filename);

  enum SafeModeAction {
    SAFEMODE_LEAVE = 1,
    SAFEMODE_ENTER = 2,
    SAFEMODE_GET = 3,
  };

  bool setSafeMode(SafeModeAction action);

  void saveNamespace();

  uint64 rollEdits();

  bool restoreFailedStorage(const string & arg);

  void refreshNodes();

  void finalizeUpgrade();

  void metaSave(const string & filename);

  void setBalancerBandwidth(uint64 bandwidth);

  bool getFileInfo(const string & src, FileStatus & status);

  bool getFileLinkInfo(const string & src, FileStatus & status);

  void getContentSummary(const string & path, ContentSummary & summary);

  void setQuota(const string & path, int64_t namespaceQuota,
      int64_t diskspaceQuota);

  void fsync(const string & src, const string & client);

  void setTimes(const string & src, uint64 mtime, uint64 atime);

  void createSymlink(const string & target, const string & link,
      FsPermission dirPerm, bool createParent);

  void getLinkTarget(const string & path, string & target);

  void updateBlockForPipeline(ExtendedBlock & block, const string & clientName,
      LocatedBlock & updated);

  void updatePipeline(const string & clientName, ExtendedBlock & oldBlock,
      ExtendedBlock & newBlock, vector<DatanodeID> & newNodes);

  void getDelegationToken(const string & renewer, Token & token);

  void renewDelegationToken(Token & token);

  void cancelDelegationToken(Token & token);

private:
  RpcClient _client;
};

}
}

#endif /* CLIENTNAMENODEPROTOCOL_H_ */
