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

#ifndef HDFSOUTPUTSTREAM_H_
#define HDFSOUTPUTSTREAM_H_

#include "datatransfer.pb.h"
#include "hadoop/common/Common.h"
#include "hadoop/common/Stream.h"
#include "hadoop/common/NetIO.h"
#include "hadoop/common/Security.h"
#include "hadoop/common/FileSystem.h"
#include "Hdfs.h"

namespace hadoop {
namespace hdfs {

/**
 * Stream to write a file in HDFS
 * Notice:
 *   1. use HdfsClient::create to create OutputStream
 *   2. do not close HdfsClient before close this stream
 *   3. not thread safe, use in single thread or do synchronization
 *   4. each stream will create a new thread to stream packets
 * Implementation detail:
 *   there are two threads:
 *     main thread for write
 *     streamer thread for async send packet and receive ack
 *   thread time line:
 *     main thread                 streamer thread
 *    start streamer
 *                              started, wait to resume
 *        write()
 *     start block
 *   send Op WRITEBLOCK
 *    receive response
 *    setup pipeline
 *  resume streamer thread
 *                                 resume running
 *    create packet
 *      fill data
 *    enqueue packet          dequeue packet and recv ack
 *    reach block end
 *   enqueue last block
 *  wait all packet acked
 *  pause streamer thread
 *                                     paused
 *   finish current block
 *     start next block
 *  resume streamer thread
 *                                resume running
 *                  ..............
 *     stop streamer
 *                              streamer thread ended
 *     complete file
 *
 *  This is different from java implementation, java DFSOutputstream
 *  use synchronized io thus need 2 extra threads for each stream, one
 *  for send packets and talking to namenode, one for receive acks.
 *  In this implementation, main thread handle write and talks to namenode,
 *  streamer thread only send packets/recv acks.
 */
class HdfsOutputStream : public OutputStream {
  friend class HdfsClient;
private:
  enum BlockConstructionStage {
    /**
     * The enumerates are always listed as regular stage followed by the
     * recovery stage.
     * Changing this order will make getRecoveryStage not working.
     */
    // pipeline set up for block append
    PIPELINE_SETUP_APPEND = 0,
    // pipeline set up for failed PIPELINE_SETUP_APPEND recovery
    PIPELINE_SETUP_APPEND_RECOVERY = 1,
    // data streaming
    DATA_STREAMING = 2,
    // pipeline setup for failed data streaming recovery
    PIPELINE_SETUP_STREAMING_RECOVERY = 3,
    // close the block and pipeline
    PIPELINE_CLOSE = 4,
    // Recover a failed PIPELINE_CLOSE
    PIPELINE_CLOSE_RECOVERY = 5,
    // pipeline set up for block creation
    PIPELINE_SETUP_CREATE = 6,
  };

  enum PipeLineStatus {
    PIPELINE_STATUS_NORMAL = 0,
    PIPELINE_STATUS_TIMEOUT = 1,
    PIPELINE_STATUS_SOCKET_ERROR = 2,
    PIPELINE_STATUS_SOCKET_CLOSED = 3,
    PIPELINE_STATUS_BAD_SEQNO = 4,
    PIPELINE_STATUS_ERROR = 5,
    PIPELINE_STATUS_UNKNOWN = 6,
  };
  static const char * PipeLineStatusToName(PipeLineStatus status);

  class Packet {
  public:
    const static int64 HEART_BEAT_SEQNO = -1;
    bool syncBlock;
    bool lastPacketInBlock;
    int64 seqno;
    uint64 offsetInBlock;
    uint64 dataStart;
    Buffer buff;
    /**
     * buff is pointed into like follows:
     *  (C is checksum data, D is payload data)
     *
     * [________________________DDDDDDDDDDDDD___]
     *                          ^            ^  ^
     *                          dataStart   pos limit
     *
     * Right before sending, we compute the checksum data and put it
     * immediately precede the actual data, and then insert the header into
     * the buffer immediately preceding the checksum data, so we make sure
     * to keep enough space in front of the checksum data to support
     * the largest conceivable header. Then we rewind the buff for sending:
     *
     * [_________HEADERCCCCCCCCCDDDDDDDDDDDDD___]
     *           ^                           ^
     *           pos                       limit
     */
    uint64 dataLen() const {
      return buff.pos < dataStart ?
          (buff.limit - dataStart) : // finished packing
          (buff.pos - dataStart); // still appending
    }
    string toString() const;
  };

  HdfsClient * _client;
  string _path;
  uint64 _fileId;
  Checksum _checksum;
  uint64 _blockSize;
  uint64 _packetDataSize;
  CreateFlag _createFlag;

  mutex _lock;
  Condition _stateChanged;
  std::deque<Packet *> _sendQueue;
  std::deque<Packet *> _ackQueue;
  uint64 _maxQueueLength;
  uint64 _currentSeqno;
  volatile uint64 _lastConfirmedSeqno;
  uint64 _maxHeaderLength;
  PacketHeaderProto _packetHeader;
  PipelineAckProto _pipelineAck;
  Packet * _curPacket;
  // Some data need special treatment because they do not align in
  // checksum chunks, we keep those data in _unalignedData.
  // Case 1:
  // If this stream is open for append, and start write position
  // is not at chunk boundary, the next packet will only contain one
  // partial trunk, because the Packet format prohibits multiple chunks
  // if data is not aligned.
  // Case 2:
  // If last Packet didn't reach to block boundary, some data needs to
  // be resent
  Buffer _unalignedData;

  volatile PipeLineStatus _pipeLineStatus;
  int32 _errorIndex;
  vector<DatanodeInfo> _targets;
  unique_ptr<ExtendedBlock> _curBlock;
  unique_ptr<ExtendedBlock> _previousBlock;
  vector<string> _favoredNodes;
  uint64 _curBlockBytesSend;

  // special state variable to mark last packet ack is received
  // so socket close event can be ignored
  bool _lastAckRecieved;

  volatile bool _running;
  int _fd;
  Poller _poller;
  thread _streamerThread;
  Buffer _readBuffer;
  Buffer * _writeBuffer;

  int _streamerResetFd;
  int _streamerFd;
  Condition _streamerFdChanged;

  uint64 _pos;
  enum FlushSyncState {
    DIRTY = 0,
    FLUSHED = 1,
    SYNCED = 2,
  };
  FlushSyncState _flushSyncState;

  int64 _nextRenewLease;
  int64 _lastActivity;

  // copy constructor is deleted
  HdfsOutputStream(const HdfsOutputStream &);

  HdfsOutputStream(HdfsClient * client, const string & path,
      const FileStatus & stat, const CreateFlag & createFlag,
      const Checksum & checksum);

  void startStreamer();
  void stopStreamer();
  void streamerLoop();
  void resetStreamerFd(int fd, bool wait);
  void onError(PipeLineStatus errorStatus);
  void onWakeup();

  Packet * createPacket(int64 seqno, uint64 offsetInBlock,
      uint64 preferedDataSize);

  void finishCurrentPacket();

  void enqueueCurrentPacket(bool wait);

  void trySendOnePacket();

  void tryReceiveAcks();

  void clearQueue();

  void locateFollowingBlock(int64 expire, LocatedBlock & lb,
      const vector<DatanodeInfo> & excludes);

  void startBlock(int64 expire);

  void recoverBlock(int64 expire);

  void finishBlock();

  void completeFile(int64 expire);

  uint64 writeSome(const void * buff, uint64 length);

public:
  virtual ~HdfsOutputStream();

  virtual uint64 tell();

  virtual uint64 write(const void * buff, uint64 length);

  virtual void flush();

  virtual void sync(uint32 flag);

  virtual void close();
};

}
}

#endif /* HDFSOUTPUTSTREAM_H_ */
