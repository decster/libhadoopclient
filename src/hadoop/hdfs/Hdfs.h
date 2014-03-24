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

#ifndef HDFS_H_
#define HDFS_H_

#include "hadoop/common/Common.h"
#include "hadoop/common/Stream.h"
#include "hadoop/common/Security.h"
#include "hadoop/common/FileSystem.h"

namespace hadoop {
namespace hdfs {

using namespace ::hadoop::common;

class ExtendedBlockProto;
class DatanodeIDProto;
class DatanodeInfoProto;
class LocatedBlockProto;
class LocatedBlocksProto;
class ClientNamenodeProtocol;
class HdfsInputStream;
class HdfsOutputStream;


/**
 * Identifies a Block uniquely across the block pools
 */
class ExtendedBlock {
public:
  string poolId;
  uint64 blockId;
  uint64 generationStamp;
  uint64 numBytes;

  ExtendedBlock() : blockId(0), generationStamp(0), numBytes(0) {}

  bool operator==(const ExtendedBlock & rhs) const;
  string toString();
  void to(ExtendedBlockProto & proto) const;
  void from(const ExtendedBlockProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};



/**
 * This class represents the primary identifier for a Datanode.
 */
class DatanodeID {
public:
  string ipAddr;
  string hostName;
  string datanodeuuid;
  uint32 xferPort;
  uint32 infoPort;
  uint32 ipcPort;
  uint32 infoSecurePort;
  DatanodeID() :
      xferPort(0), infoPort(0), ipcPort(0), infoSecurePort(0) {
  }

  bool operator==(const DatanodeID & rhs) const;

  string toString();

  string uniqueId();

  void to(DatanodeIDProto & proto) const;
  void from(const DatanodeIDProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};



/**
 * DatanodeID with ephemeral state, eg usage information, current
 * administrative state, and the network location that is communicated
 * to clients.
 */
class DatanodeInfo {
public:
  enum AdminState {
    NORAML = 0,
    DECOMMISSION_INPROGRESS = 1,
    DECOMMISSIONED = 2,
  };

  DatanodeID id;
  uint64 capacity;
  uint64 dfsUsed;
  uint64 remaining;
  uint64 blockPoolUsed;
  uint64 lastUpdate;
  uint32 xceiverCount;
  string location;
  AdminState adminState;
  uint64 cacheCapacity;
  uint64 cacheUsed;

  DatanodeInfo() :
    capacity(0),
    dfsUsed(0),
    remaining(0),
    blockPoolUsed(0),
    lastUpdate(0),
    xceiverCount(0),
    adminState(NORAML),
    cacheCapacity(0),
    cacheUsed(0) {}

  string toString() {
    return id.toString();
  }

  void to(DatanodeInfoProto & proto) const;
  void from(const DatanodeInfoProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};



/**
 * Associates a block with the Datanodes that contain its replicas
 * and other block metadata (E.g. the file offset associated with this
 * block, whether it is corrupt, a location is cached in memory,
 * security token, etc).
 */
class LocatedBlock {
public:
  ExtendedBlock block;
  uint64 offset;
  bool corrupt;
  vector<DatanodeInfo> locations;
  Token blockToken;

  LocatedBlock() : offset(0), corrupt(false) {}

  uint64 getStart() {
    return offset;
  }

  uint64 getLength() {
    return block.numBytes;
  }

  uint64 getEnd() {
    return getStart() + getLength();
  }

  string toString();

  void to(LocatedBlockProto & proto) const;
  void from(const LocatedBlockProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};
typedef shared_ptr<LocatedBlock> LocatedBlockPtr;



/**
 * Collection of blocks with their locations and the file length.
 */
class LocatedBlocks {
public:
  uint64 fileLength;
  vector<LocatedBlockPtr> blocks;
  LocatedBlockPtr lastLocatedBlock;
  bool underConstruction;
  bool isLastBlockComplete;

  LocatedBlocks() :
      fileLength(0), underConstruction(false), isLastBlockComplete(true) {
  }

  int64 getInsertBlockIndex(uint64 offset);

  void insertRange(int64 idx, vector<LocatedBlockPtr> & newBlocks);

  string toString();

  void to(LocatedBlocksProto & proto) const;
  void from(const LocatedBlocksProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};



/**
 * Used by HdfsClient to manage lease renew
 * hide implementation in source file
 */
class RenewLeaseContext;



/**
 * Hdfs client
 * thread-safe
 */
class HdfsClient {
  friend class HdfsInputStream;
  friend class HdfsOutputStream;
private:
  mutex _lock;
  string _clientName;
  ClientNamenodeProtocol * _namenode;
  set<HdfsInputStream *> _inputStreams;
  set<HdfsOutputStream *> _outputStreams;
  unique_ptr<RenewLeaseContext> _renewLeaseContext;

  HdfsClient(HdfsClient &);
  void checkIsAbsolutePath(const string & path);
  ClientNamenodeProtocol * namenode();

  // used to check inpustream/outputstream life-cycle
  void addInpputStream(HdfsInputStream * stream);
  void removeInputStream(HdfsInputStream * stream);
  void addOutputStream(HdfsOutputStream * stream);
  void removeOutputStream(HdfsOutputStream * stream);
  bool checkStreams();

  /**
   * check whether lease renew is required, asynchronously start lease
   * renew if necessary, if there are some error occurred when renewing
   * lease, return false.
   */
  bool checkAndRenewLease();

public:
  HdfsClient(const User & user, const string & addr,
      const char * namePrefix);

  HdfsClient(const User & user, const string & addr);

  HdfsClient(const string & addr);

  ~HdfsClient();

  /**
   * Open a file in HDFS as Inputstream for read, do not delete
   * this Client when there are unclosed streams using this client
   * @param path file path
   * @param verifyChecksum verify checksum
   * @return HdfsInputStream
   */
  InputStream * open(const string & path, bool verifyChecksum=true);

  /**
   * Create/truncate/append a file and open an OutputStream for write
   * @param path file path
   * @param flag create flags
   * @param permission permissions for created file
   * @param createParent create parent dir is not exists
   * @param replication replication
   * @param blockSize block size
   * @return
   */
  OutputStream * createEx(const string & path, const CreateFlag & flag,
      const FsPermission & permission, bool createParent, uint32 replication,
      uint64 blockSize);

  /**
   * get FileSatus
   * @param path file path
   * @param status status to return
   * @return false if file not found
   */
  bool getStatus(const string & path, FileStatus & status);

  /**
   * list FileStatus
   * @param path dir path
   * @param status status array to return
   * @return true if success
   */
  bool listStatus(const string & path, vector<FileStatus> & status);

  /**
   * delete file/directory
   * @param path
   * @param recursive delete all sub-directories
   */
  void remove(const string & path, bool recursive);

  /**
   * create directory(s)
   * @param path dir path
   * @param permission dir permission
   * @param createParent like mkdir -p, create parent dirs if not exists
   */
  void mkdir(const string & path, const FsPermission & permission,
      bool createParent);

  /**
   * mv file/directory
   * @param src src path
   * @param dst dest path
   * @param overwrite overwrite if dst exists
   */
  void rename(const string & src, const string & dst, bool overwrite);

  /**
   * get blocks of a file
   * @param path file path
   * @param start range start
   * @param length range length
   * @param lbs LocatedBlocks to return
   */
  void getLocatedBlocks(const string & path, uint64 start, uint64 length,
      LocatedBlocks & lbs);

  /**
   * Get name of the client
   * @return client name
   */
  const string & getClientName();

  void clearDataEncryptionKey();

  /**
   * Get Hdfs connect/read/write/complete timeout
   * @return timeout in millisecond
   */
  int64 getTimeout();

  uint64 getDefaultBlockSize();

  /**
   * check if path exists
   * @param path
   * @return true if exists
   */
  bool exists(const string & path);

  /**
   * check if path is a file
   * @param path
   * @return
   */
  bool isFile(const string & path);

  /**
   * check if path is a directory
   * @param path
   * @return
   */
  bool isDirectory(const string & path);

};

}
}

#endif /* HDFS_H_ */
