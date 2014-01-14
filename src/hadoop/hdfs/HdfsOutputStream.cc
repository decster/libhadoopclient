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

#include "ClientNamenodeProtocol.h"
#include "DataTransferProtocol.h"
#include "HdfsOutputStream.h"

namespace hadoop {
namespace hdfs {

string HdfsOutputStream::Packet::toString() const {
  return Strings::Format(
      "Packet(seqno=%lld offset=%llu len=%llu sync=%d last=%d)", seqno,
      offsetInBlock, dataLen(), (int) syncBlock, (int) lastPacketInBlock);
}

static const char * statusNames[] =  {
   "PIPELINE_STATUS_NORMAL",
   "PIPELINE_STATUS_TIMEOUT",
   "PIPELINE_STATUS_SOCKET_ERROR",
   "PIPELINE_STATUS_SOCKET_CLOSED",
   "PIPELINE_STATUS_BAD_SEQNO",
   "PIPELINE_STATUS_ERROR",
   "PIPELINE_STATUS_UNKNOWN",
};

const char * HdfsOutputStream::PipeLineStatusToName(PipeLineStatus status) {
  return statusNames[status > PIPELINE_STATUS_UNKNOWN ? PIPELINE_STATUS_UNKNOWN : status];
}

HdfsOutputStream::HdfsOutputStream(const HdfsOutputStream & rhs) :
    _client(rhs._client), _checksum(rhs._checksum), _createFlag(rhs._createFlag) {
  // deleted
}

HdfsOutputStream::HdfsOutputStream(HdfsClient * client, const string & path,
    const FileStatus & stat, const CreateFlag & createFlag,
    const Checksum & checksum) :
    _client(client), _path(path), _fileId(stat.fileId), _createFlag(createFlag),
    _checksum(checksum), _blockSize(stat.blockSize),
    _packetDataSize(64 * 1024), _maxQueueLength(10),
    _currentSeqno(0), _lastConfirmedSeqno(0), _curBlockBytesSend(0),
    _running(false), _fd(-1), _curPacket(nullptr),
    _unalignedData(checksum.bytesPerChecksum()),
    _pos(0), _flushSyncState(SYNCED), _nextRenewLease(0), _lastActivity(0) {
  _client->addOutputStream(this);
  if (_blockSize % _checksum.bytesPerChecksum() != 0) {
    THROW_EXCEPTION_EX(IOException,
        "blockSize(%llu) not multiples of bytesPerChecksum(%llu).",
        _blockSize, _checksum.bytesPerChecksum());
  }
  // must fill all fields, so we can get max header length
  _packetHeader.set_lastpacketinblock(false);
  _packetHeader.set_syncblock(false);
  _packetHeader.set_seqno(Packet::HEART_BEAT_SEQNO);
  _packetHeader.set_offsetinblock(0);
  _packetHeader.set_datalen(0);
  _maxHeaderLength = 6 + _packetHeader.ByteSize();
  // 256 should be enough to hold PipelineAckProto
  _readBuffer.reset(256);
  _writeBuffer = nullptr;
  _pipeLineStatus = PIPELINE_STATUS_NORMAL;
  _errorIndex = -1;
  startStreamer();
}

void HdfsOutputStream::startStreamer() {
  ScopeLock guard(_lock);
  if (!_running) {
    _running = true;
    _streamerThread = thread(&HdfsOutputStream::streamerLoop, this);
  }
}

void HdfsOutputStream::stopStreamer() {
  ScopeLock guard(_lock);
  if (_running) {
    LOG_DEBUG("stopStreamer: start")
    _running = false;
    _poller.wakeup();
    _stateChanged.notify_all();
    _streamerThread.join();
    LOG_DEBUG("stopStreamer: finish")
  }
}

void HdfsOutputStream::resetStreamerFd(int fd, bool wait) {
  UniqueLock ul(_lock);
  if (_streamerFd != fd && _pipeLineStatus==PIPELINE_STATUS_NORMAL) {
    LOG_DEBUG("reset _streamerResetFd from %d to %d", _streamerResetFd, fd);
    _streamerResetFd = fd;
    _poller.wakeup();
  }
  if (wait) {
    while (_streamerFd != fd && _pipeLineStatus==PIPELINE_STATUS_NORMAL) {
      _stateChanged.wait(ul);
    }
  }
}

void HdfsOutputStream::onError(PipeLineStatus errorStatus) {
  ScopeLock sl(_lock);
  LOG_DEBUG("set _pipeLineStatus to %s", PipeLineStatusToName(errorStatus));
  _pipeLineStatus = errorStatus;
  LOG_DEBUG("reset _streamerFd from %d to -1", _streamerFd);
  _streamerResetFd = -1;
  _streamerFd = -1;
  _stateChanged.notify_all();
}

void HdfsOutputStream::onWakeup() {
  int64 now = Time::CurrentMonoMill();
  if (now > _nextRenewLease) {
    // TODO: currently renewLease failures are not handled
    // continue to write until other error showed up
    _client->checkAndRenewLease();
    // try renew lease every 10 seconds
    _nextRenewLease = now + 10000;
  }
  if (_writeBuffer == nullptr) {
    if (_lastActivity + 30000 < now && _streamerFd >= 0) {
      // need to send heartbeat packet
      LOG_DEBUG("send heartbeat packet");
      _lastActivity = now;
    }
  }
}

void HdfsOutputStream::streamerLoop() {
  LOG_DEBUG("streamer thread started, file=%s", _path.c_str());
  _lastActivity = Time::CurrentMonoMill();
  while (_running) {
    try {
      if ((_streamerResetFd != _streamerFd)) {
        ScopeLock sl(_lock);
        if (_streamerFd != _streamerResetFd) {
          LOG_DEBUG("reset _streamerFd from %d to %d", _streamerFd, _streamerResetFd);
          _streamerFd = _streamerResetFd;
          _poller.reset(_streamerFd);
          _stateChanged.notify_all();
        }
      }
      if (_streamerFd >= 0) {
        int16 events = EVENT_READ | EVENT_CLOSE | EVENT_ERROR;
        if (!(_writeBuffer && _writeBuffer->remain())) {
          // TODO: add heartbeat packet
          trySendOnePacket();
        }
        if (_writeBuffer && _writeBuffer->remain() > 0) {
          events |= EVENT_WRITE;
        }
        _poller.update(events);
        events = _poller.poll(5000);
        if (!_running) {
          break;
        }
        if (events & EVENT_ERROR) {
          LOG_WARN("streamer thread socket(fd=%d) error",
              _poller.getFd());
          onError(PIPELINE_STATUS_SOCKET_ERROR);
          continue;
        }
        if (events & EVENT_CLOSE) {
          if (_lastAckRecieved) {
            onError(PIPELINE_STATUS_NORMAL);
          } else {
            LOG_WARN("streamer thread socket(fd=%d) closed", _poller.getFd());
            onError(PIPELINE_STATUS_SOCKET_CLOSED);
          }
          continue;
        }
        onWakeup();
        if (!_running) {
          break;
        }
        if (events & EVENT_READ) {
          _readBuffer.read(_poller.getFd());
        }
        if (_readBuffer.pos > 0) {
          tryReceiveAcks();
          if (_pipeLineStatus != PIPELINE_STATUS_NORMAL) {
            continue;
          }
        }
        if (events & EVENT_WRITE) {
          _writeBuffer->write(_poller.getFd());
          if (_writeBuffer->remain() == 0) {
            _writeBuffer = nullptr;
          }
        }
      } else {
        _poller.update(0);
        _poller.poll(5000);
        onWakeup();
      }
    } catch (Exception & e) {
      LOG_WARN("streamer thread got exception, some logic error");
      e.logWarn();
      onError(PIPELINE_STATUS_SOCKET_ERROR);
    }
  }
  LOG_DEBUG("streamer thread stopped, file=%s", _path.c_str());
}

HdfsOutputStream::Packet * HdfsOutputStream::createPacket(int64 seqno,
    uint64 offsetInBlock, uint64 preferedDataSize) {
  Packet * ret = new Packet();
  ret->syncBlock = false;
  ret->lastPacketInBlock = false;
  ret->seqno = seqno;
  ret->offsetInBlock = offsetInBlock;
  if (preferedDataSize > 0) {
    if (_unalignedData.pos > 0) {
      // append and has unaligned data, only allow 1 partial chunk
      preferedDataSize = _checksum.bytesPerChecksum() - _unalignedData.pos;
    }
    ret->dataStart = _maxHeaderLength
        + _checksum.chunks(preferedDataSize) * _checksum.checksumSize();
    ret->buff.reset(ret->dataStart + preferedDataSize);
    if (_unalignedData.remain() > 0) {
      // has some unaligned data left in last packet need to resent
      memcpy(ret->buff.data + ret->dataStart, _unalignedData.current(),
          _unalignedData.remain());
      offsetInBlock -= _unalignedData.remain();
    }
  } else {
    ret->dataStart = _maxHeaderLength;
    ret->buff.reset(ret->dataStart);
  }
  ret->buff.pos = ret->dataStart;
  return ret;
}

void HdfsOutputStream::finishCurrentPacket() {
  // serial entire packet content to packet buffer
  // buffer format:
  // PACKETLEN  PROTOBUF_OBJ_LEN   PROTOBUF   CHECKSUM DATA
  //  4 byte       2 byte        _packetHeader checksum data
  uint64 dataLen = _curPacket->dataLen();
  _packetHeader.set_datalen(dataLen);
  _packetHeader.set_lastpacketinblock(_curPacket->lastPacketInBlock);
  _packetHeader.set_offsetinblock(_curPacket->offsetInBlock);
  _packetHeader.set_seqno(_curPacket->seqno);
  _packetHeader.set_syncblock(_curPacket->syncBlock);
  char * dataPos = _curPacket->buff.data + _curPacket->dataStart;
  if (dataLen > 0) {
    // adjust _unalignedData
    uint64 end = (_curPacket->offsetInBlock + dataLen)
        % _checksum.bytesPerChecksum();
    if (end != 0) {
      int64 cp = end - _unalignedData.pos;
      if (cp <= 0 || cp >= _checksum.bytesPerChecksum() - _unalignedData.pos) {
        // must be something wrong
        LOG_FATAL("packet or unalignedData state error "
            "unalignedData(pos=%llu, limit=%llu) "
            "pos=%llu offsetInBlock=%llu dataLen=%llu",
            _unalignedData.pos, _unalignedData.limit,
            _pos, _curPacket->offsetInBlock, dataLen);
        THROW_EXCEPTION_EX(IOException, "Illegal packet and unalignedData state");
      }
      memcpy(_unalignedData.current(), dataPos + dataLen - cp, cp);
      _unalignedData.limit = _unalignedData.pos + cp;
    } else {
      _unalignedData.pos = 0;
      _unalignedData.limit = 0;
    }
  }
  uint64 checksumLen = _checksum.chunks(dataLen) * _checksum.checksumSize();
  char * checksumPos = dataPos - checksumLen;
  if (checksumLen > 0) {
    _checksum.checksum(dataPos, dataLen, checksumPos);
  }
  uint64 protoLen = _packetHeader.ByteSize();
  char * protoPos = checksumPos - protoLen;
  _packetHeader.SerializeToArray(protoPos, protoLen);
  char * protoLenPos = protoPos - 2;
  *(uint16*)protoLenPos = HToBe((uint16)protoLen);
  char * packetLenPos = protoLenPos - 4;
  *(uint32*)packetLenPos = HToBe((uint32)(4 + dataLen + checksumLen));
  _curPacket->buff.limit = _curPacket->buff.pos;
  _curPacket->buff.pos = packetLenPos - _curPacket->buff.data;
}

void HdfsOutputStream::enqueueCurrentPacket(bool wait) {
  finishCurrentPacket();
  bool failed = false;
  {
    UniqueLock ul(_lock);
    while ((_pipeLineStatus == PIPELINE_STATUS_NORMAL)
        && (_sendQueue.size() + _ackQueue.size() >= _maxQueueLength)) {
      //LOG_DEBUG("wait to enqueue");
      _stateChanged.wait(ul);
    }
    int64 waitSeqno = _curPacket->seqno;
    string packetDesc = _curPacket->toString();
    LOG_DEBUG("enqueue %s pos=%llu", packetDesc.c_str(), _pos);
    _sendQueue.push_back(_curPacket);
    _curPacket = nullptr;
    _stateChanged.notify_all();
    _poller.wakeup();
    if (wait) {
      LOG_DEBUG("wait ack: %s", packetDesc.c_str());
      while (_lastConfirmedSeqno < waitSeqno
          && (_pipeLineStatus == PIPELINE_STATUS_NORMAL)) {
        _stateChanged.wait(ul);
      }
      LOG_DEBUG("wait ack %s finished", packetDesc.c_str());
    }
    if (_pipeLineStatus != PIPELINE_STATUS_NORMAL) {
      failed = true;
    }
  }
  if (failed) {
    stopStreamer();
    clearQueue();
    if (_fd > 0) {
      ::close(_fd);
      _fd = -1;
    }
    LOG_WARN("HDFS write pipeline recovery not supported yet");
    THROW_EXCEPTION_EX(IOException, "HdfsOutputStream pipeline error: %s",
        PipeLineStatusToName(_pipeLineStatus));
    // error occurred:
    // 1. recover pipeline
    // 2. resume streamer
    // recoverBlock(Time::Timeout(_client->getTimeout()));
  }
}

void HdfsOutputStream::trySendOnePacket() {
  //LOG_DEBUG("trySendOnePacket called");
  UniqueLock ul(_lock);
  if (_sendQueue.size() > 0) {
    Packet * p = _sendQueue.front();
    _writeBuffer = &(p->buff);
    _curBlockBytesSend = std::max(_curBlockBytesSend,
        p->offsetInBlock + p->dataLen());
    _ackQueue.push_back(p);
    _sendQueue.pop_front();
    //LOG_DEBUG("StreamerThread start send %lld", p->seqno);
    _lastAckRecieved = false;
    _lastActivity = Time::CurrentMonoMill();
  }
}

void HdfsOutputStream::tryReceiveAcks() {
  _readBuffer.rewind();
  bool updated = false;
  while (_readBuffer.tryReadVarInt32Prefixed(_pipelineAck) > 0) {
    updated = true;
    int64 seqno = _pipelineAck.seqno();
    LOG_DEBUG("receive ack, seqno=%lld sendQueue=%llu ackQueue=%llu", seqno,
        _sendQueue.size(), _ackQueue.size());
    if (seqno == Packet::HEART_BEAT_SEQNO) {
      continue;
    }
    for (int i = _pipelineAck.status_size() - 1; i >= 0; i--) {
      if (_pipelineAck.status(i) != Status::SUCCESS) {
        LOG_WARN("Bad response %d for from %s", _pipelineAck.status(i),
            _targets[i].toString().c_str());
        _errorIndex = i;
        onError(PIPELINE_STATUS_ERROR);
        break;
      }
    }
    if (_pipeLineStatus == PIPELINE_STATUS_NORMAL) {
      _lock.lock();
      Packet * pk = _ackQueue.front();
      if ((pk == nullptr) || (pk->seqno != seqno)) {
        _lock.unlock();
        onError(PIPELINE_STATUS_BAD_SEQNO);
      } else {
        _curBlock->numBytes = pk->offsetInBlock + pk->dataLen();
        _lastConfirmedSeqno = seqno;
        _ackQueue.pop_front();
        if (pk->lastPacketInBlock) {
          _lastAckRecieved = true;
        }
        delete pk;
        _stateChanged.notify_all();
        _lock.unlock();
      }
    }
  }
  // move remaining data to beginning of the buffer, so make some space
  // for next read
  if (_readBuffer.remain() > 0) {
    memmove(_readBuffer.data, _readBuffer.current(), _readBuffer.remain());
  }
  _readBuffer.pos = _readBuffer.remain();
  _readBuffer.limit = _readBuffer.capacity;
  if (updated) {
    _lastActivity = Time::CurrentMonoMill();
  }
}

void HdfsOutputStream::clearQueue() {
  UniqueLock ulock(_lock);
  for (Packet * p : _sendQueue) {
    delete p;
  }
  _sendQueue.clear();
  for (Packet * p : _ackQueue) {
    delete p;
  }
  _ackQueue.clear();
}

void HdfsOutputStream::locateFollowingBlock(int64 expire, LocatedBlock & lb,
    const vector<DatanodeInfo> & excludes) {
  int64 sleepTime = 400;
  while (true) {
    try {
      _client->namenode()->addBlock(_path, _fileId, _client->getClientName(),
          _previousBlock.get(), excludes, _favoredNodes, lb);
      return;
    } catch (Exception & e) {
      if (e.type() == Exception::NotReplicatedYetException) {
        if (Time::CurrentMonoMill() < expire) {
          LOG_WARN(
              "Add block(%llu) get NotReplicatedYetException, sleep %lldms and retry...",
              lb.block.blockId, sleepTime);
          Thread::Sleep(sleepTime);
          sleepTime *= 2;
        }
      } else {
        throw;
      }
    }
  }
}

void HdfsOutputStream::startBlock(int64 expire) {
  LocatedBlock lb;
  vector<DatanodeInfo> excludes;
  locateFollowingBlock(expire, lb, excludes);
  _curBlockBytesSend = lb.block.numBytes;
  if (lb.locations.size() == 0) {
    // TODO: error
  }
  DatanodeInfo & first = lb.locations[0];
  SockAddr datanodeAddr(first.id.ipAddr, first.id.xferPort);
  int fd = NetUtil::connect(datanodeAddr, false);
  Buffer temp;
  LOG_DEBUG("Send WRITEBLOCK(block=%llu, pipelineSize=%llu minBytes=%llu maxBytes=%llu)",
      lb.block.blockId, lb.locations.size(), lb.block.numBytes, _curBlockBytesSend);
  DataTransferProtocol(temp).writeBlock(lb.block, &(lb.blockToken),
      _client->getClientName(), lb.locations, nullptr,
      (OpWriteBlockProto_BlockConstructionStage) PIPELINE_SETUP_CREATE,
      lb.locations.size(), lb.block.numBytes, _curBlockBytesSend,
      0, _checksum);
  temp.rewind();
  temp.writeAll(fd);
  BlockOpResponseProto resp;
  temp.readVarInt32Prefixed(fd, resp);
  Status replyStatus = resp.status();
//  LOG_DEBUG("Get reply status=%s", Status_Name(replyStatus).c_str());
  LOG_DEBUG("Get reply status=%d", replyStatus);
  // TODO: handle retry and recover
  if (replyStatus != Status::SUCCESS) {
    ::close(fd);
    LOG_DEBUG("abondonBlock(path=%llu block=%llu)", _path.c_str(), lb.block.blockId);
    _client->namenode()->abandonBlock(lb.block, _path, _client->getClientName());
    if (replyStatus == Status::ERROR_ACCESS_TOKEN) {
      THROW_EXCEPTION_EX(InvalidBlockTokenException,
          "Got access token error for connect ack with firstBadLink as %s",
          resp.firstbadlink().c_str());
    } else {
      THROW_EXCEPTION_EX(IOException, "Bad connect ack with firstBadLink as %s",
          resp.firstbadlink().c_str());
    }
  }
  _curBlock.reset(new ExtendedBlock(lb.block));
  _targets.swap(lb.locations);
  _curBlockBytesSend = 0;
  if (_fd != -1) {
    LOG_FATAL("fd should not be -1");
  }
  NetUtil::nonBlock(fd, true);
  _fd = fd;
  _pipeLineStatus = PIPELINE_STATUS_NORMAL;
  resetStreamerFd(_fd, true);
}

void HdfsOutputStream::recoverBlock(int64 expire) {
  // TODO: impl

  _pipeLineStatus = PIPELINE_STATUS_NORMAL;
}

void HdfsOutputStream::finishBlock() {
  if (_curBlock) {
    _curPacket = createPacket(_currentSeqno++,
        (_pos % _blockSize) ? (_pos % _blockSize) : _blockSize, 0);
    _curPacket->lastPacketInBlock = true;
    bool doSync = _createFlag.hasSyncBlock();
    _curPacket->syncBlock = doSync;
    enqueueCurrentPacket(true);
    _flushSyncState = doSync ? SYNCED : FLUSHED;
    resetStreamerFd(-1, true);
    if (_fd >= 0) {
      ::close(_fd);
      _fd = -1;
    }
    // _pipeLineStatus = PIPELINE_STATUS_NORMAL; // useless?
    _previousBlock = std::move(_curBlock);
  }
}

void HdfsOutputStream::completeFile(int64 expire) {
  int64 sleepTime = 500;
  while (true) {
    if (_client->namenode()->complete(_path, _fileId, _client->getClientName(),
        _previousBlock.get())) {
      return;
    }
    int64 now = Time::CurrentMonoMill();
    if (now >= expire) {
      THROW_EXCEPTION_EX(IOException, "Could not complete file(%s), abort",
          _path.c_str());
    }
    int64 st = std::min(sleepTime, expire - now);
    if (st > 5000) {
      LOG_WARN("Could not complete file(%s), sleep %llds and retry...",
          _path.c_str(), st / 1000);
    }
    Thread::Sleep(st);
    sleepTime *= 2;
  }
}

HdfsOutputStream::~HdfsOutputStream() {
  _client->removeOutputStream(this);
  try {
    close();
  } catch (std::exception & ex) {
    LOG_WARN("Delete HdfsOutputStream(%s): %s", _path.c_str(), ex.what());
    LOG_WARN("If you want to handle this exception, please call close() explicitly before delete stream")
  }
}

uint64 HdfsOutputStream::tell() {
  return _pos;
}

uint64 HdfsOutputStream::writeSome(const void * buff, uint64 length) {
  if (!_curPacket) {
    if (!_curBlock) {
      startBlock(Time::Timeout(_client->getTimeout()));
    }
    uint64 maxDataSize = _blockSize - (_pos % _blockSize);
    _curPacket = createPacket(_currentSeqno++, _pos % _blockSize,
        std::min(maxDataSize, _packetDataSize));
    _flushSyncState = DIRTY;
  }
  uint64 wt = _curPacket->buff.write(buff, length);
  if (_curPacket->buff.remain() == 0) {
    enqueueCurrentPacket(false);
  }
  _pos += wt;
  if (_pos % _blockSize == 0) {
    finishBlock();
  }
  return wt;
}

uint64 HdfsOutputStream::write(const void * buff, uint64 length) {
  if (!_running || length == 0) {
    return 0;
  }
  uint64 wt = 0;
  while (wt < length) {
    uint64 ret = writeSome((uint8*)buff + wt, length - wt);
    if (ret == 0) {
      break;
    }
    wt += ret;
  }
  return wt;
}

void HdfsOutputStream::flush() {
  if (_flushSyncState >= FLUSHED) {
    return;
  }
  if (!_curPacket) {
    _curPacket = createPacket(_currentSeqno++, _pos % _blockSize, 0);
  }
  enqueueCurrentPacket(true);
  _flushSyncState = FLUSHED;
}

void HdfsOutputStream::sync(uint32 flag) {
  if (_flushSyncState >= SYNCED) {
    return;
  }
  if (!_curPacket) {
    _curPacket = createPacket(_currentSeqno++, _pos % _blockSize, 0);
  }
  _curPacket->syncBlock = true;
  enqueueCurrentPacket(true);
  // TODO: if flag is set, report new length to namenode
  _flushSyncState = SYNCED;
}

void HdfsOutputStream::close() {
  if (_running) {
    if (_pos % _blockSize != 0) {
      try {
        if (_curPacket) {
          enqueueCurrentPacket(false);
        }
        finishBlock();
      } catch (Exception & e) {
        stopStreamer();
        if (_fd >= 0) {
          ::close(_fd);
          _fd = -1;
        }
        throw;
      }
    }
    stopStreamer();
    completeFile(Time::Timeout(_client->getTimeout()));
    LOG_DEBUG("HdfsOutputStream(file=%s fileId=%llu) closed", _path.c_str(),
        _fileId);
  }
}

}
}
