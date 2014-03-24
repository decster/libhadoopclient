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

#include "hdfs.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include "hadoop/common/Path.h"
#include "hadoop/hdfs/Hdfs.h"
#include "hadoop/hdfs/ClientNamenodeProtocol.h"
#include "hadoop/hdfs/HdfsInputStream.h"
#include "hadoop/hdfs/HdfsOutputStream.h"

namespace hadoop {
namespace hdfs {

bool ExtendedBlock::operator==(const ExtendedBlock & rhs) const {
  return poolId == rhs.poolId && blockId == rhs.blockId;
}

string ExtendedBlock::toString() {
  return Strings::ToString(blockId);
}

void ExtendedBlock::to(ExtendedBlockProto & proto) const {
  proto.set_poolid(poolId);
  proto.set_blockid(blockId);
  proto.set_generationstamp(generationStamp);
  proto.set_numbytes(numBytes);
}

void ExtendedBlock::from(const ExtendedBlockProto & proto) {
  poolId = proto.poolid();
  blockId = proto.blockid();
  generationStamp = proto.generationstamp();
  numBytes = proto.numbytes();
}

void DatanodeID::to(DatanodeIDProto& proto) const {
  proto.set_ipaddr(ipAddr);
  proto.set_hostname(hostName);
  proto.set_datanodeuuid(datanodeuuid);
  proto.set_xferport(xferPort);
  proto.set_infoport(infoPort);
  proto.set_ipcport(ipcPort);
}

void DatanodeID::from(const DatanodeIDProto& proto) {
  ipAddr = proto.ipaddr();
  hostName = proto.hostname();
  datanodeuuid = proto.datanodeuuid();
  xferPort = proto.xferport();
  infoPort = proto.infoport();
  ipcPort = proto.ipcport();
}

void DatanodeInfo::to(DatanodeInfoProto& proto) const {
  id.to(*(proto.mutable_id()));
  proto.set_capacity(capacity);
  proto.set_dfsused(dfsUsed);
  proto.set_remaining(remaining);
  proto.set_blockpoolused(blockPoolUsed);
  proto.set_lastupdate(lastUpdate);
  proto.set_xceivercount(xceiverCount);
  proto.set_location(location);
  proto.set_adminstate((DatanodeInfoProto_AdminState) (adminState));
}

void DatanodeInfo::from(const DatanodeInfoProto& proto) {
  id.from(proto.id());
  capacity = proto.capacity();
  dfsUsed = proto.dfsused();
  remaining = proto.remaining();
  blockPoolUsed = proto.blockpoolused();
  lastUpdate = proto.lastupdate();
  xceiverCount = proto.xceivercount();
  location = proto.location();
  adminState = (DatanodeInfo::AdminState) (proto.adminstate());
}

void LocatedBlock::to(LocatedBlockProto& proto) const {
  block.to(*proto.mutable_b());
  proto.set_offset(offset);
  proto.set_corrupt(corrupt);
  proto.clear_locs();
  for (size_t i = 0; i < locations.size(); i++) {
    locations[i].to(*proto.add_locs());
  }
  blockToken.to(*proto.mutable_blocktoken());
}

void LocatedBlock::from(const LocatedBlockProto& proto) {
  block.from(proto.b());
  offset = proto.offset();
  locations.clear();
  for (int i = 0; i < proto.locs_size(); i++) {
    locations.push_back(DatanodeInfo());
    locations[i].from(proto.locs(i));
  }
  corrupt = proto.corrupt();
  blockToken.from(proto.blocktoken());
}

void LocatedBlocks::to(LocatedBlocksProto& proto) const {
  proto.set_filelength(fileLength);
  proto.clear_blocks();
  for (auto & blk : blocks) {
    blk->to(*proto.add_blocks());
  }
  proto.set_islastblockcomplete(isLastBlockComplete);
  proto.set_underconstruction(underConstruction);
  proto.clear_lastblock();
  if (lastLocatedBlock) {
    lastLocatedBlock->to(*proto.mutable_lastblock());
  }
}

void LocatedBlocks::from(const LocatedBlocksProto& proto) {
  fileLength = proto.filelength();
  blocks.clear();
  for (int i = 0; i < proto.blocks_size(); i++) {
    blocks.push_back(LocatedBlockPtr(new LocatedBlock()));
    blocks[i]->from(proto.blocks(i));
  }
  underConstruction = proto.underconstruction();
  if (proto.has_lastblock()) {
    lastLocatedBlock.reset(new LocatedBlock());
    lastLocatedBlock->from(proto.lastblock());
  }
  isLastBlockComplete = proto.islastblockcomplete();
}

Json ExtendedBlock::toJson() const {
  Json ret = Json::Dict();
  ret.putString("poolId", poolId);
  // blockId may become negative
  ret.putInt("blockId", blockId);
  ret.putInt("generationStamp", generationStamp);
  ret.putInt("numBytes", numBytes);
  return ret;
}

void ExtendedBlock::fromJson(Json & json) {
  poolId = json.getString("poolId");
  blockId = json.getInt("blockId");
  generationStamp = json.getInt("generationStamp");
  numBytes = json.getInt("numBytes");
}



bool DatanodeID::operator==(const DatanodeID & rhs) const {
  if (this == &rhs) {
    return true;
  }
  return ((datanodeuuid == rhs.datanodeuuid) && (xferPort == rhs.xferPort)
      && (ipAddr == rhs.ipAddr));
}

string DatanodeID::toString() {
  return Strings::Format("%s:%d", ipAddr.empty() ? "0.0.0.0" : ipAddr.c_str(),
      xferPort);
}

string DatanodeID::uniqueId() {
  return Strings::Format("%s:%u:%s",
      ipAddr.empty() ? "0.0.0.0" : ipAddr.c_str(), xferPort, datanodeuuid.c_str());
}



string LocatedBlock::toString() {
  string ret = Strings::Format("%llu(offset=%llu", block.blockId, offset);
  for (size_t i = 0; i < locations.size(); i++) {
    ret.append(",");
    ret.append(locations[i].toString());
  }
  ret.append(")");
  return ret;
}



int64 LocatedBlocks::getInsertBlockIndex(uint64 offset) {
  for (int64_t i = 0; i < blocks.size(); i++) {
    LocatedBlock & blk = *(blocks[i].get());
    if (blk.getStart() <= offset) {
      if (blk.getStart() + blk.getLength() > offset) {
        return i;
      }
    } else {
      return -i;
    }
  }
  return -blocks.size();
}

void LocatedBlocks::insertRange(int64 idx,
    vector<LocatedBlockPtr> & newBlocks) {
  size_t oldIdx = idx;
  size_t insStart = 0;
  size_t insEnd = 0;
  for (size_t newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size();
      newIdx++) {
    uint64_t newOff = newBlocks[newIdx]->getStart();
    uint64_t oldOff = blocks[oldIdx]->getStart();
    if (newOff < oldOff) {
      insEnd++;
    }
    else if (newOff == oldOff) {
      blocks[oldIdx] = newBlocks[newIdx];
      if (insStart < insEnd) {
        blocks.insert(blocks.begin() + oldIdx, newBlocks.begin() + insStart,
            newBlocks.begin() + insEnd);
        oldIdx += insEnd - insStart;
      }
      insStart = insEnd = newIdx + 1;
      oldIdx++;
    }
    else {
      THROW_EXCEPTION(IOException,
          "List of LocatedBlock must be sorted by startOffset");
    }
    insEnd = newBlocks.size();
    if (insStart < insEnd) {
      blocks.insert(blocks.begin() + oldIdx, newBlocks.begin() + insStart,
          newBlocks.begin() + insEnd);
    }
  }
}

string LocatedBlocks::toString() {
  string ret = Strings::Format("LocatedBlocks(length=%llu", fileLength);
  if (underConstruction) {
    ret.append(",underConstruction");
  }
  for (LocatedBlockPtr & blk : blocks) {
    Strings::Append(ret, ",\n    %8llu: %llu", blk->offset, blk->block.blockId);
  }
  if (underConstruction && lastLocatedBlock) {
    Strings::Append(ret, ",\n    %8llu: %llu", lastLocatedBlock->offset,
        lastLocatedBlock->block.blockId);
  }
  ret.append(")");
  return ret;
}

///////////////////////////////////////////////////////////

/**
 * Used by HdfsClient to manage lease renew
 */
class RenewLeaseContext {
public:
  bool errorLogged;
  int64 renewPeriod;
  int64 nextRenew;
  RenewLeaseRequestProto requestProto;
  RenewLeaseResponseProto responseProto;
  Call call;
  RenewLeaseContext(const string & clientName, int64 renewPeriod) :
      errorLogged(false),
      renewPeriod(renewPeriod),
      nextRenew(0),
      call("renewLease", requestProto, responseProto, 0) {
    requestProto.set_clientname(clientName);
  }
  bool checkAndRenewlease(RpcClient & rpcClient) {
    int64 now = Time::CurrentMonoMill();
    if (now < nextRenew) {
      // unsafe check to skip unnecessary locking
      return true;
    }
    ScopeLock sl(call.lock);
    if (now < nextRenew) {
      // need to check again to prevent race condition
      return true;
    }
    switch (call.status) {
      case Call::RPC_CALL_INIT:
        rpcClient.asyncCall(call);
        return true;
      case Call::RPC_CALL_WAITING:
        return true;
      case Call::RPC_CALL_SUCCESS:
        call.status = Call::RPC_CALL_INIT;
        nextRenew = now + renewPeriod;
        return true;
      case Call::RPC_CALL_LOCAL_ERROR:
      case Call::RPC_CALL_REMOTE_ERROR:
        if (!errorLogged) {
          LOG_WARN("Call renewLease(%s) failed, %s:%s",
              call.exceptionType.c_str(), call.exceptionTrace.c_str());
            errorLogged = true;
        }
        return false;
    }
  }
};

///////////////////////////////////////////////////////////

HdfsClient::HdfsClient(HdfsClient &) {
  // deleted constructor
}

void HdfsClient::checkIsAbsolutePath(const string & path) {
  if (!Path::IsAbsolute(path)) {
    THROW_EXCEPTION_EX(IllegalArgumentException, "Not an absolute path: %s",
        path.c_str());
  }
}

ClientNamenodeProtocol * HdfsClient::namenode() {
  return _namenode;
}

void HdfsClient::addInpputStream(HdfsInputStream* stream) {
  ScopeLock sl(_lock);
  _inputStreams.insert(stream);
}

void HdfsClient::removeInputStream(HdfsInputStream* stream) {
  ScopeLock sl(_lock);
  _inputStreams.erase(stream);
}

void HdfsClient::addOutputStream(HdfsOutputStream* stream) {
  ScopeLock sl(_lock);
  _outputStreams.insert(stream);
}

void HdfsClient::removeOutputStream(HdfsOutputStream* stream) {
  ScopeLock sl(_lock);
  _outputStreams.erase(stream);
}

bool HdfsClient::checkStreams() {
  ScopeLock sl(_lock);
  if (_inputStreams.empty() && _outputStreams.empty()) {
    return true;
  }
  LOG_WARN(
      "HdfsClient::checkStreams error: not all streams(input: %llu, output: %llu) are deleted",
      _inputStreams.size(), _outputStreams.size());
  return false;
}

bool HdfsClient::checkAndRenewLease() {
  return _renewLeaseContext->checkAndRenewlease(_namenode->getClient());
}

HdfsClient::HdfsClient(const User & user, const string & addr,
    const char * namePrefix) :
    _namenode(new ClientNamenodeProtocol(user, addr)) {
  _clientName = Strings::Format("NativeHdfsClient_%s_%u_%llu", namePrefix,
      Random::NextUInt32(), Thread::CurrentId());
  _renewLeaseContext.reset(new RenewLeaseContext(_clientName, getTimeout()/2));
}

HdfsClient::HdfsClient(const User & user, const string & addr) :
    _namenode(new ClientNamenodeProtocol(user, addr)) {
  _clientName = Strings::Format("NativeHdfsClient_%s_%u_%llu", "NONMAPREDUCE",
      Random::NextUInt32(), Thread::CurrentId());
  _renewLeaseContext.reset(new RenewLeaseContext(_clientName, getTimeout()/2));
}

HdfsClient::HdfsClient(const string & addr) :
    _namenode(new ClientNamenodeProtocol(User::GetLoginUser(), addr)) {
  _clientName = Strings::Format("NativeHdfsClient_%s_%u_%llu", "NONMAPREDUCE",
      Random::NextUInt32(), Thread::CurrentId());
  _renewLeaseContext.reset(new RenewLeaseContext(_clientName, getTimeout()/2));
}

HdfsClient::~HdfsClient() {
  delete _namenode;
  _namenode = nullptr;
  checkStreams();
}

InputStream * HdfsClient::open(const string & path, bool verifyChecksum) {
  checkIsAbsolutePath(path);
  return new HdfsInputStream(this, path, verifyChecksum);
}

OutputStream * HdfsClient::createEx(const string & path,
    const CreateFlag & flag, const FsPermission & permission, bool createParent,
    uint32 replication, uint64 blockSize) {
  checkIsAbsolutePath(path);
  // TODO: support more CreateFlag(overwrite, append)
  FileStatus st;
  if (getStatus(path, st)) {
    THROW_EXCEPTION_EX(IOException, "create %s failed: already exists");
  }
  _namenode->create(path, permission, _clientName, flag, createParent,
      replication, blockSize, st);
  return new HdfsOutputStream(this, path, st, flag,
      Checksum(ChecksumType::CHECKSUM_CRC32C));
}

bool HdfsClient::getStatus(const string & path, FileStatus & status) {
  checkIsAbsolutePath(path);
  return _namenode->getFileInfo(path, status);
}

bool HdfsClient::listStatus(const string & path, vector<FileStatus> & status) {
  checkIsAbsolutePath(path);
  DirectoryListing listing;
  string start;
  while (true) {
    if (!_namenode->getListing(path, start, false, listing)) {
      return false;
    }
    for (auto & s : listing.status) {
      status.push_back(s);
    }
    if (listing.remainingEntries <= 0) {
      break;
    }
    if (status.size() > 0) {
      start = Path::GetName(status[status.size() - 1].path);
    }
  }
  return true;
}

void HdfsClient::remove(const string & path, bool recursive) {
  checkIsAbsolutePath(path);
  _namenode->delete_(path, recursive);
}

void HdfsClient::mkdir(const string & path, const FsPermission & permission,
    bool createParent) {
  checkIsAbsolutePath(path);
  if (!_namenode->mkdirs(path, permission, createParent)) {
    THROW_EXCEPTION_EX(IOException, "HDFSFileSystem::mkdir(%s) failed",
        path.c_str());
  }
}

void HdfsClient::rename(const string & src, const string & dst,
    bool overwrite) {
  checkIsAbsolutePath(src);
  checkIsAbsolutePath(dst);
  _namenode->rename2(src, dst, overwrite);
}

void HdfsClient::getLocatedBlocks(const string & path, uint64 start,
    uint64 length, LocatedBlocks & lbs) {
  checkIsAbsolutePath(path);
  _namenode->getBlockLocations(path, start, length, lbs);
}

const string & HdfsClient::getClientName() {
  return _clientName;
}

bool HdfsClient::exists(const string & path) {
  FileStatus st;
  if (getStatus(path, st)) {
    return true;
  }
  return false;
}

bool HdfsClient::isFile(const string & path) {
  FileStatus st;
  if (getStatus(path, st)) {
    return st.isFile();
  }
  return false;
}

bool HdfsClient::isDirectory(const string & path) {
  FileStatus st;
  if (getStatus(path, st)) {
    return st.isDirectory();
  }
  return false;
}

void HdfsClient::clearDataEncryptionKey() {
  // TODO:
}

int64 HdfsClient::getTimeout() {
  return 60000;
}


uint64 HdfsClient::getDefaultBlockSize() {
  return 128 * 1024 * 1024;
}

}
}
