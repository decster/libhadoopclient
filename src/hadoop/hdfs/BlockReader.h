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

#ifndef BLOCKREADER_H_
#define BLOCKREADER_H_

#include "datatransfer.pb.h"
#include "hadoop/common/Common.h"
#include "hadoop/common/Security.h"
#include "hadoop/common/Checksum.h"
#include "hadoop/common/Buffer.h"
#include "hadoop/common/Stream.h"
#include "hadoop/common/NetIO.h"
#include "Hdfs.h"

namespace google {
namespace protobuf {
class MessageLite;
}
}


namespace hadoop {
namespace hdfs {

using namespace ::hadoop::common;

class PacketHeaderProto;
class ExtendedBlock;
class DatanodeID;
class DataEncryptionKey;

/**
 * BlockReader for HdfsInpustream to read block data
 */
class BlockReader {
public:
  BlockReader() {}
  virtual ~BlockReader() {}

  /**
   * Same interface as InputStream::read()
   * used by HDFSInputStream::read()
   * @param buff output buff, put read data to this buff
   * @param length buff length
   * @return bytes read if return value >=0, -1 if EOF or permanent failure
   */
  virtual int64 read(void * buff, uint64 length) = 0;

  /**
   * skip reading certain amount of bytes
   */
  virtual uint64 skip(uint64 n) = 0;

  /**
   * close this BlockReader, release resources
   */
  virtual void close() = 0;

  /// zero copy interfaces

  /**
   * Obtains a chunk of data from the stream.
   * @param buff output buff position
   * @param length output data length
   * @return false if error or there is no more data to return(EOF) permanently
   * It is legal for the returned buffer to have zero size, as long as repeatedly
   * calling Next() eventually yields a buffer with non-zero size.
   */
  virtual bool next(char *& buff, uint64 & length) = 0;

  /**
   * Backs up a number of bytes, so that the next call to next() returns
   * data again that was already returned by the last call to Next().  This
   * is useful when writing procedures that are only supposed to read up
   * to a certain point in the input, then return.  If next() returns a
   * buffer that goes beyond what you wanted to read, you can use backUp()
   * to return to the point where you intended to finish.
   * Preconditions:
   *   The last method called must have been next().
   *   count must be less than or equal to the size of the last buffer
   *   returned by next().
   * Postconditions:
   *   The last "length" bytes of the last buffer returned by next() will be
   *   pushed back into the stream.  Subsequent calls to next() will return
   *   the same data again before producing new data.
   * @param length of last bytes needed to be put back
   */
  virtual void backUp(uint64 length) = 0;

  // used for pread
  //virtual int32 readAll(void * buff, uint32 length);
};


/**
 * Read block data through socket
 */
class RemoteBlockReader : public BlockReader {
protected:
  int _fd;
  bool _sentStatusCode;
  bool _verifyChecksum;
  Checksum _checksum;
  Buffer _readBuff;
  Buffer _writeBuff;
  uint64 _startOffset;
  int64 _bytesNeededToFinish;
  int64 _lastSeqNo;
  hadoop::hdfs::PacketHeaderProto _headerProto;

  // state for zero copy
  char * _nextBuffBackup;
  // temp buff to read final empty packet
  Buffer _tempBuff;
  // remember current DatanodeId to reuse connection
  string _currentDatanode;
public:
  RemoteBlockReader();
  virtual ~RemoteBlockReader();

  // used for stream read
  void startRead(const string & clientName, const string & file,
      DatanodeID & datanode, ExtendedBlock & block, uint64 startOffset,
      uint64 length, bool verify, Token * token,
      DataEncryptionKey * encryptionKey);
  virtual int64 read(void * buff, uint64 length);
  virtual uint64 skip(uint64 n);
  virtual void close();
  /// zero copy interfaces
  virtual bool next(char *& buff, uint64 & length);
  virtual void backUp(uint64 length);

  // used for parallel pread
  //virtual int32 readAll(void * buff, uint32 length);

protected:
  void readNextPacket(Buffer & buff, bool expectLast, uint64 skipChecksum);
  /**
   * When the reader reaches end of the read, it sends a status response
   * (e.g. CHECKSUM_OK) to the DN. Failure to do so could lead to the DN
   * closing our connection (which we will re-open), but won't affect
   * data correctness.
   */
  void sendReadResult();

};

}
}


#endif /* BLOCKREADER_H_ */
