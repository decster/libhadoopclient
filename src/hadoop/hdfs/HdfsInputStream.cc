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

#include "HdfsInputStream.h"

namespace hadoop {
namespace hdfs {

HdfsInputStream::HdfsInputStream(const HdfsInputStream &) {
  // deleted
}

HdfsInputStream::HdfsInputStream(HdfsClient * client, const string & path,
    bool verifyChecksum) :
    _closed(true), _client(client), _path(path), _verify(verifyChecksum),
    _prefetchSize(client->getDefaultBlockSize() * 10), _retryWindowMs(3000),
    _maxBlockAcquireFailures(3), _pos(0), _fileLength(0), _blockEnd(0) {
  client->addInpputStream(this);
  openInfo();
}

HdfsInputStream::~HdfsInputStream() {
  _client->removeInputStream(this);
  try {
    close();
  } catch (std::exception & ex) {
    LOG_WARN("Delete HdfsInputStream(%s): %s", _path.c_str(),
        ex.what());
    LOG_WARN("If you want to handle this exception, please call close() explicitly before delete stream")
  }
}

void HdfsInputStream::checkOpen() {
  if (_closed) {
    THROW_EXCEPTION_EX(IOException, "HdfsInputStream::checkOpen() failed");
  }
}

void HdfsInputStream::openInfo() {
  int64 lastLen = fetchLocatedBlocksAndGetLastBlockLength();
  int32 retries = 3;
  while (retries > 0) {
    if (lastLen == -1) {
      LOG_WARN(
          "Loast block locations not available. Datanodes might not have "
          "reported blocks completely. Will retry for %d times",
          retries);
      Thread::Sleep(4000);
      lastLen = fetchLocatedBlocksAndGetLastBlockLength();
    } else {
      break;
    }
    retries--;
  }
  if (retries == 0) {
    THROW_EXCEPTION(IOException, "Could not obtain the last block locations.");
  }
  _fileLength = _locatedBlocks.fileLength + lastLen;
  _closed = false;
}

int64 HdfsInputStream::fetchLocatedBlocksAndGetLastBlockLength() {
  if (_closed) {
    _client->getLocatedBlocks(_path, 0, _prefetchSize, _locatedBlocks);
    LOG_DEBUG("newInfo=%s", _locatedBlocks.toString().c_str());
  } else {
    LocatedBlocks newInfo;
    _client->getLocatedBlocks(_path, 0, _prefetchSize, newInfo);
    LOG_DEBUG("newInfo=%s", _locatedBlocks.toString().c_str());
    size_t cmpSize = std::min(_locatedBlocks.blocks.size(),
        newInfo.blocks.size());
    for (size_t i = 0; i < cmpSize; i++) {
      LocatedBlock & lhs = *(_locatedBlocks.blocks[i]);
      LocatedBlock & rhs = *(newInfo.blocks[i]);
      if (!(lhs.block == rhs.block)) {
        THROW_EXCEPTION_EX(IOException, "Blocklist for %s has changed!",
            _path.c_str());
      }
    }
    _locatedBlocks.blocks.swap(newInfo.blocks);
    _locatedBlocks.lastLocatedBlock.swap(newInfo.lastLocatedBlock);
    _locatedBlocks.fileLength = newInfo.fileLength;
    _locatedBlocks.isLastBlockComplete = newInfo.isLastBlockComplete;
    _locatedBlocks.underConstruction = newInfo.underConstruction;
  }
  uint64 lastBlockBeingWrittenLength = 0;
  if (!_locatedBlocks.isLastBlockComplete) {
    if (_locatedBlocks.lastLocatedBlock) {
      LocatedBlock * last = _locatedBlocks.lastLocatedBlock.get();
      if (last->locations.size()==0) {
        return -1;
      }
      uint64 len = readBlockLength(*last);
      last->block.numBytes = len;
      lastBlockBeingWrittenLength = len;
    }
  }
  _currentNodeName.clear();
  _currentDatanode.clear();
  return lastBlockBeingWrittenLength;
}

uint64 HdfsInputStream::readBlockLength(LocatedBlock & locatedBlock) {
  size_t replicaNotFoundCount = locatedBlock.locations.size();
  for (DatanodeInfo & di : locatedBlock.locations) {
    LOG_WARN("current not support read under construction block");
    // TODO: impl
//      ClientDatanodeProtocol cdp;
//      uint64 n = cdp.getReplicaVisibleLength(locatedBlock.block);
//      if (n>=0) {
//        return n;
//      }
  }
  if (replicaNotFoundCount == 0) {
    return 0;
  }
  THROW_EXCEPTION_EX(IOException, "Cannot obtain block length for %s",
      locatedBlock.toString().c_str());
}

void HdfsInputStream::seek(uint64 position) {
  checkOpen();
  if (position > _fileLength) {
    THROW_EXCEPTION(IOException, "Cannot seek after EOF");
  }
  bool done  = false;
  if (_pos <= position && position <= _blockEnd) {
    uint64 diff = position - _pos;
    if (diff <= 128 * 1024) {
      try {
        _pos += _blockReader.skip(diff);
        if (_pos == position) {
          done = true;
        }
      } catch (Exception & e) {
        LOG_DEBUG("%s(%s) while seek to %llu at %s of %s from %s", e.type(),
            e.what(), position, _currentBlock.c_str(), _path.c_str(),
            _currentNodeName.c_str());
        throw;
      }
    }
  }
  if (!done) {
    _pos = position;
    _blockEnd = 0;
  }
}

uint64 HdfsInputStream::tell() {
  checkOpen();
  return _pos;
}

uint64 HdfsInputStream::getLength() {
  checkOpen();
  return _fileLength;
}

void HdfsInputStream::close() {
  if (!_closed) {
    _blockReader.close();
    _closed = true;
    LOG_DEBUG("HdfsInpputStream(path=%s) closed", _path.c_str());
  }
}

int64 HdfsInputStream::read(void * buff, uint64 length) {
  checkOpen();
  if (_pos >= _fileLength) {
    return -1;
  }
  int32 failures = 0;
  bool retryCurrent = true;
  while (true) { // retry loop
    if (_pos >= _blockEnd || _currentNodeName.empty()) {
      try {
        blockSeek(_pos, false);
        retryCurrent = true; // reset
      } catch (Exception & e) {
        if (e.type() == Exception::BlockMissingException) {
          if (++failures > _maxBlockAcquireFailures) {
            throw;
          }
          uint64 waitTime = (uint64) (_retryWindowMs * (failures - 1)
              + _retryWindowMs * failures * Random::NextFloat());
          LOG_WARN("HDFS read try all datanode failed %s time(s), "
                  "will wait for %llums and retry...",
                   failures, waitTime);
          Thread::Sleep(waitTime);
          _deadNodes.clear();
          openInfo();
          _blockEnd = 0;
          continue;
        }
        // Exception throw by getBlockAt() probably caused
        // by calling namenode RPCs
        LOG_WARN("blockSeek(%llu) failed path=%s",
                 _pos, _path.c_str());
        e.logWarn();
        _blockEnd = 0;
        throw;
      }
    }
    try {
      int32 result = _blockReader.read(buff, length);
      if (result >= 0) {
        _pos += result;
        return result;
      }
      THROW_EXCEPTION_EX(
          IOException,
          "Unexpected EOF while read %s from %s",
          _path.c_str(),
          _currentNodeName.c_str());
    } catch (Exception & e) {
      if (e.type() == Exception::ChecksumException) {
        LOG_WARN(
            "Checksum error while read from %s of %s from %s",
            _currentBlock.c_str(),
            _path.c_str(),
            _currentNodeName.c_str());
        _deadNodes.insert(_currentDatanode);
        _blockEnd = 0;
      } else {
        if (!retryCurrent) {
          LOG_WARN(
              "%s(%s) reading from %s of %s, add %s to deadnodes and retry",
              e.type(), e.what(),
              _currentBlock.c_str(),
              _path.c_str(),
              _currentNodeName.c_str());
          _deadNodes.insert(_currentDatanode);
          _blockEnd = 0;
        }
        retryCurrent = false;
      }
    }
  }
  THROW_EXCEPTION(IOException, "HdfsInputStream::read should never got here");
}

void HdfsInputStream::blockSeek(uint64 target, bool renew) {
  if (target >= _fileLength) {
    // TODO: not necessary?
    THROW_EXCEPTION_EX(IOException,
        "blockSeekTo(%llu) exceeds file length %llu", target, _fileLength);
  }
  DatanodeInfo * chosenNode = NULL;
  int32 refetchToken = 1;
  int32 refetchEncryptionKey = 1;
  bool connectionFailedOnce = false;
  while (true) {
    LocatedBlockPtr clb = getBlockAt(target, renew);
    _currentBlock = clb->block.toString();
    renew = false; // reset renew
    _pos = target;
    _blockEnd = clb->getEnd();
    uint64 offsetIntoBlock = target - clb->offset;
    DatanodeID * choosen = bestDatanode(*clb);
    if (choosen == NULL) {
      THROW_EXCEPTION_EX(BlockMissingException, "Block missing: %s",
          _currentBlock.c_str());
    }
    _currentNodeName = choosen->toString();
    _currentDatanode = choosen->uniqueId();
    try {
      uint64 readLength = clb->getLength() - offsetIntoBlock;
      LOG_DEBUG(
          "Start read block=%llu, pos=%llu, offsetInBlock=%llu, length=%llu",
          clb->block.blockId, target, offsetIntoBlock, readLength);
      _blockReader.startRead(_client->getClientName(), _path, *choosen,
          clb->block, offsetIntoBlock, readLength, _verify, nullptr, nullptr);
      if (connectionFailedOnce) {
        LOG_INFO("Successfully connected to %s for %s",
            _currentNodeName.c_str(), _currentBlock.c_str())
      }
      return;
    } catch (Exception & ex) {
      if ((ex.type() == Exception::InvalidEncryptionKeyException) && (refetchEncryptionKey > 0)) {
        LOG_INFO("Will fetch a new encryption key and retry, encryption "
            "key was invalid when connecting to %s. %s(%s)",
            _currentNodeName.c_str(), ex.type(), ex.what());
        refetchEncryptionKey--;
        _client->clearDataEncryptionKey();
      } else if ((ex.type() == Exception::InvalidBlockTokenException) && (refetchToken > 0)) {
        LOG_INFO("Will fetch a new access token and retry, "
            "access token was invalid when connecting to %s. %s(%s)",
            _currentNodeName.c_str(), ex.type(), ex.what());
        refetchToken--;
        renew = true;
      } else {
        connectionFailedOnce = true;
        LOG_WARN(
            "Connecting to %s for %s failed, add to deadNodes and continue. %s(%s)",
            _currentNodeName.c_str(), _currentBlock.c_str(), ex.type(),
            ex.what());
         _deadNodes.insert(_currentDatanode);
      }
    }
  }
  LOG_FATAL("blockSeekTo(%llu) logic error", target);
  THROW_EXCEPTION_EX(IOException, "blockSeekTo(%llu) logic error", target);
}

void HdfsInputStream::getBlockRange(uint64 offset, uint64 length,
    vector<LocatedBlockPtr> & blocks) {
  ScopeLock slock(_lock);
  if (offset >= _fileLength) {
    THROW_EXCEPTION_EX(IOException,
        "getBlockRange(): Offset %llu exceeds file length %llu", offset,
        _fileLength);
  }
  uint64 lengthOfCompleteBlk = _locatedBlocks.fileLength;
  if (offset < lengthOfCompleteBlk) {
    int64 retIdx = _locatedBlocks.getInsertBlockIndex(offset);
    uint64 insertIdx = retIdx<0?-retIdx:retIdx;
    uint64 end = std::min(offset+length, lengthOfCompleteBlk);
    uint64 curOff = offset;
    while (curOff < end) {
      LocatedBlockPtr blk;
      if (insertIdx < _locatedBlocks.blocks.size()) {
        blk = _locatedBlocks.blocks[insertIdx];
      }
      if (!blk || curOff < blk->getStart()) {
        LocatedBlocks newBlocks;
        _client->getLocatedBlocks(_path, curOff, end - curOff, newBlocks);
        _locatedBlocks.insertRange(insertIdx, newBlocks.blocks);
        continue;
      }
      blocks.push_back(blk);
      uint64 bytesRead = blk->getStart() + blk->getLength() - curOff;
      curOff += bytesRead;
      insertIdx++;
    }
  }
  if ((offset + length > lengthOfCompleteBlk)
      && _locatedBlocks.lastLocatedBlock) {
    blocks.push_back(_locatedBlocks.lastLocatedBlock);
  }
}

LocatedBlockPtr HdfsInputStream::getBlockAt(uint64 target, bool renew) {
  if (target >= _fileLength) {
    THROW_EXCEPTION_EX(IOException, "getBlockAt(%llu) exceed file length %llu",
        target, _fileLength);
  }
  if (target >= _locatedBlocks.fileLength) {
    // TODO:
    return _locatedBlocks.lastLocatedBlock;
  } else {
    int64 insertIndex = _locatedBlocks.getInsertBlockIndex(target);
    if (insertIndex < 0 || renew) {
      size_t idx = insertIndex >= 0 ? insertIndex : -insertIndex;
      LocatedBlocks lbs;
      _client->getLocatedBlocks(_path, target, _prefetchSize, lbs);
      _locatedBlocks.insertRange(idx, lbs.blocks);
      return _locatedBlocks.blocks[idx];
    } else {
      return _locatedBlocks.blocks[insertIndex];
    }
  }
}

DatanodeID * HdfsInputStream::bestDatanode(LocatedBlock & block) {
  DatanodeID * ret = NULL;
  for (DatanodeInfo & info : block.locations) {
    if (_deadNodes.find(info.id.uniqueId()) == _deadNodes.end()) {
      return &(info.id);
    }
  }
  return NULL;
}


}
}
