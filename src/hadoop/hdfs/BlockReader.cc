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

#include "DataTransferProtocol.h"
#include "BlockReader.h"

namespace hadoop {
namespace hdfs {

RemoteBlockReader::RemoteBlockReader() :
    _fd(-1), _writeBuff(1024) {
}

RemoteBlockReader::~RemoteBlockReader() {
  close();
}

int64 RemoteBlockReader::read(void * buff, uint64 length) {
  if (_readBuff.remain() == 0) {
    if (_bytesNeededToFinish > 0) {
      readNextPacket(_readBuff, false, 0);
    } else {
      return -1;
    }
  }
  if (_readBuff.remain() < length) {
    length = _readBuff.remain();
  }
  ::memcpy(buff, _readBuff.current(), length);
  _readBuff.pos += length;
  return length;
}

bool RemoteBlockReader::next(char *& buff, uint64 & length) {
  if (_readBuff.remain() == 0) {
    if (_bytesNeededToFinish > 0) {
      readNextPacket(_readBuff, false, 0);
    } else {
      return false;
    }
  }
  _nextBuffBackup = _readBuff.current();
  buff = _nextBuffBackup;
  length = _readBuff.remain();
  _readBuff.pos = _readBuff.limit;
  return true;
}

void RemoteBlockReader::backUp(uint64 count) {
  if ((_nextBuffBackup == NULL) ||
      (_readBuff.current() - count < _nextBuffBackup)) {
    THROW_EXCEPTION(
        IOException,
        "RemoteBlockReader::backUp illegal count or usage");
  }
  _nextBuffBackup = NULL;
  _readBuff.pos -= count;
}

uint64 RemoteBlockReader::skip(uint64 n) {
  uint64 need = n;
  while (need > 0) {
    if (_readBuff.remain() == 0) {
      if (_bytesNeededToFinish > 0) {
        readNextPacket(_readBuff, false, need);
      } else {
        break;
      }
    }
    if (_readBuff.remain() < need) {
      need -= _readBuff.remain();
      _readBuff.pos = _readBuff.limit;
    } else {
      _readBuff.pos += need;
      need = 0;
    }
  }
  return n - need;
}

void RemoteBlockReader::close() {
  if (_fd >= 0) {
    ::close(_fd);
    _fd = -1;
  }
  _currentDatanode.clear();
  _bytesNeededToFinish = 0;
  _readBuff.reset(0);
  _nextBuffBackup = NULL;
}

static const uint32 MAX_PACKET_SIZE = 10 * 1024 * 1024;

void RemoteBlockReader::readNextPacket(Buffer & rb, bool expectLast, uint64 skipChecksum) {
  // Each packet looks like:
  //   PLEN    HLEN      HEADER     CHECKSUMS  DATA
  //   32-bit  16-bit   <protobuf>  <variable length>
  //
  // PLEN:      Payload length
  //            = length(PLEN) + length(CHECKSUMS) + length(DATA)
  //            This length includes its own encoded length in
  //            the sum for historical reasons.
  //
  // HLEN:      Header length
  //            = length(HEADER)
  //
  // HEADER:    the actual packet header fields, encoded in protobuf
  // CHECKSUMS: the crcs for the data chunk. May be missing if
  //            checksums were not requested
  // DATA       the actual block data
  rb.reset(6);
  rb.readAll(_fd, true);
  rb.rewind();
  uint32 payloadLen = rb.readUInt32Be();
  uint32 headerLen = rb.readUInt16Be();
  if (payloadLen < 4 || payloadLen+headerLen > MAX_PACKET_SIZE) {
    THROW_EXCEPTION_EX(
        IOException,
        "Incorrect value for packet size, payloadLen=%u, headerLen=%u",
        payloadLen, headerLen);
  }
  uint32 checksumAndDataLen = payloadLen - 4;
  uint32 restLen = headerLen + checksumAndDataLen;
  rb.reset(restLen);
  rb.readAll(_fd, true);
  if (!_headerProto.ParseFromArray(rb.data, headerLen)) {
    THROW_EXCEPTION(
        IOException,
        "_headerProto.ParseFromArray failed");
  }
//  LOG_DEBUG("readNextPacket: offsetInBlock=%lld "
//      "seqno=%d dataLen=%d checksumAndDataLen=%u",
//      _headerProto.offsetinblock(), _headerProto.seqno(),
//      _headerProto.datalen(), checksumAndDataLen);
  // sanity check needed?
  if (_lastSeqNo + 1 != _headerProto.seqno()) {
    THROW_EXCEPTION_EX(IOException,
        "RemoteBlockReader seqno number not match: %lld!=%lld", _lastSeqNo + 1,
        _headerProto.seqno());
  }
  _lastSeqNo ++;
  if (expectLast) {
    if (!_headerProto.lastpacketinblock() ||
        _headerProto.datalen() != 0 ) {
      THROW_EXCEPTION_EX(IOException, "Expect empty end-of-read packet!",
          "offsetInBlock=%lld seqno=%d dataLen=%d checksumAndDataLen=%u",
          _headerProto.offsetinblock(), _headerProto.seqno(),
          _headerProto.datalen(), checksumAndDataLen);
    }
    sendReadResult();
    return;
  }
  uint64 skipCount;
  if (_headerProto.offsetinblock() < _startOffset) {
    skipCount = _startOffset - _headerProto.offsetinblock();
  } else {
    skipCount = 0;
  }
  if (_headerProto.datalen() <= 0) {
    THROW_EXCEPTION_EX(IOException,
        "RemoteBlockReader::readNextPacket got illeagal header: "
            "offsetInBlock=%lld seqno=%d dataLen=%d checksumAndDataLen=%u",
        _headerProto.offsetinblock(), _headerProto.seqno(),
        _headerProto.datalen(), checksumAndDataLen);
  }
  uint32 checksumLen = checksumAndDataLen - _headerProto.datalen();
  if (_verifyChecksum) {
    char * checksumBuff = rb.data + headerLen;
    char * dataBuff = checksumBuff + checksumLen;
    uint64 skipChunks = (skipCount + skipChecksum)
        / _checksum.bytesPerChecksum();
    uint64 skipDataLength = skipChunks*_checksum.bytesPerChecksum();
    uint64 skipChecksumLength = skipChunks*_checksum.checksumSize();
    const char * ret = _checksum.validate(
        dataBuff + skipDataLength,
        _headerProto.datalen() - skipDataLength,
        checksumBuff + skipChecksumLength);
    if (ret != NULL) {
      THROW_EXCEPTION_EX(ChecksumException, "Checksum error: blockOffset=%llu",
          _headerProto.offsetinblock() + (ret - dataBuff));
    }
  }
  _bytesNeededToFinish -= _headerProto.datalen();
  rb.pos = headerLen + checksumLen + skipCount;

  if (_bytesNeededToFinish <= 0) {
    readNextPacket(_tempBuff, true, 0);
  }
}

void RemoteBlockReader::sendReadResult() {
  if (_sentStatusCode) {
    THROW_EXCEPTION(IOException, "already send status code");
  }
  try {
    ClientReadStatusProto crsp;
    crsp.set_status(
        _verifyChecksum ? (Status::CHECKSUM_OK) : (Status::SUCCESS));
    _writeBuff.writeVarInt32Prefixed(_fd, crsp);
    _sentStatusCode = true;
  } catch (Exception & e) {
    // It's ok not to be able to send this. But something is probably wrong.
    LOG_INFO("Can not send read status to datanode: %s", e.what());
  }
}

void RemoteBlockReader::startRead(const string & clientName,
    const string & file, DatanodeID & datanode, ExtendedBlock & block,
    uint64 startOffset, uint64 length, bool verify, Token * token,
    DataEncryptionKey * encryptionKey) {
  AutoFD fd;
  if (_currentDatanode == datanode.uniqueId() && (_fd >= 0)) {
    //LOG_DEBUG("reuse previous connection: %s", datanode.toString().c_str());
    fd.swap(_fd);
  } else {
    close();
    SockAddr sockAddr(datanode.ipAddr, datanode.xferPort);
    try {
      SockAddr localAddr;
      // TODO: support timeout
      fd.swap(NetUtil::connect(sockAddr, localAddr, false));
      LOG_DEBUG("RemoteBlockReader connect to %s(localAddr=%s fd=%d)",
          sockAddr.toString().c_str(), localAddr.toString().c_str(), fd.fd);
    } catch (Exception & e) {
      LOG_WARN("Connect to datanode %s failed: %s", sockAddr.toString().c_str(),
          e.what());
      throw;
    }
  }

  _writeBuff.reset(_writeBuff.capacity);
  DataTransferProtocol(_writeBuff).readBlock(block, token, clientName,
      startOffset, length);
  _writeBuff.rewind();
  _writeBuff.writeAll(fd.fd, true);

  // get response
  ::hadoop::hdfs::BlockOpResponseProto res;
  _readBuff.readVarInt32Prefixed(fd.fd, res);
  if (res.status() != ::hadoop::hdfs::Status::SUCCESS) {
    if (res.status() == ::hadoop::hdfs::Status::ERROR_ACCESS_TOKEN) {
      THROW_EXCEPTION_EX(InvalidBlockTokenException,
          "Got access token error for OP_READ_BLOCK, file=%s block=%s",
          file.c_str(), block.toString().c_str());
    }
    else {
      THROW_EXCEPTION_EX(IOException,
          "Got error for OP_READ_BLOCK, file=%s block=%s", file.c_str(),
          block.toString().c_str());
    }
  }
  const ::hadoop::hdfs::ChecksumProto & cp =
      res.readopchecksuminfo().checksum();
  uint64 firstChunkOffset = res.readopchecksuminfo().chunkoffset();
  if (firstChunkOffset > startOffset
      || firstChunkOffset + cp.bytesperchecksum() <= startOffset) {
    THROW_EXCEPTION_EX(IOException,
        "RemoteBlockReader: error in first chunk offset(%llu) "
            "startOffset is %llu for file %s", firstChunkOffset, startOffset,
        file.c_str());
  }

  _verifyChecksum = verify;
  _checksum.reset(static_cast<ChecksumType>(cp.type()), cp.bytesperchecksum());
  _startOffset = startOffset;
  _bytesNeededToFinish = length + startOffset - firstChunkOffset;
  _lastSeqNo = -1;
  _sentStatusCode =false;
  _nextBuffBackup = NULL;
  _readBuff.reset(0);
  _fd = fd.swap(-1);
  _currentDatanode = datanode.uniqueId();
}

}
}
