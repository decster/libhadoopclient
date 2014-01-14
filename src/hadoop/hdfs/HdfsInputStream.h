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

#ifndef HDFSINPUTSTREAM_H_
#define HDFSINPUTSTREAM_H_

#include "hadoop/common/Common.h"
#include "hadoop/common/Stream.h"
#include "hadoop/common/Security.h"
#include "Hdfs.h"
#include "BlockReader.h"

namespace hadoop {
namespace hdfs {

using namespace ::hadoop::common;

/**
 * HdfsInputstream, provide stream interface for reading file on HDFS
 */
class HdfsInputStream : public InputStream {
  friend class HdfsClient;
protected:
  mutex _lock;
  bool _closed;
  HdfsClient * _client;
  string _path;
  bool _verify;

  uint64 _prefetchSize;
  uint64 _retryWindowMs;
  uint32 _maxBlockAcquireFailures;

  LocatedBlocks  _locatedBlocks;
  RemoteBlockReader _blockReader;

  uint64 _pos;
  uint64 _blockEnd;
  uint64 _fileLength;

  std::set<string> _deadNodes;
  string _currentNodeName;
  string _currentDatanode;
  string _currentBlock;

private:
  // copy constructor is deleted
  HdfsInputStream(const HdfsInputStream &);

  /**
   * check if underlying fs still in service or this FileSystem
   * already closed
   */
  void checkOpen();

  /**
   *  prepare necessary fields(LocatedBlocks, file length,
   *  blockEnd, etc.), so this stream can be ready for read
   */
  void openInfo();

  /**
   * help method only used by openInfo multiple times
   */
  int64 fetchLocatedBlocksAndGetLastBlockLength();

  /**
   * read the block length from one of the datanodes
   */
  uint64 readBlockLength(LocatedBlock & locatedBlock);

  /**
   * get LocatedBlock info at target position, request from
   * namenode if not in _locatedBlocks
   * @param target position to find block
   * @param renew force fetch newest info from namenode
   *              and update _locatedBlocks
   * @return shared_ptr of the block which includes target position
   */
  LocatedBlockPtr getBlockAt(uint64 target, bool renew);

  /**
   * get LocatedBlocks for a given range,
   * including last under construction block
   * @param offset range start, included
   * @param length range end, excluded
   * @param blocks return value container
   */
  void getBlockRange(uint64 offset, uint64 length,
                     vector<LocatedBlockPtr> & blocks);

  void blockSeek(uint64 target, bool renew);

  /**
   * get first valid datanode in block.locations
   */
  DatanodeID * bestDatanode(LocatedBlock & block);

  /**
   * Constructor
   * @param client HdfsClient used by this stream. The client is
   *               required by this stream, and cannot be destroyed
   *               before this stream is closed
   * @param path file absolute path
   * @param verifyChecksum is verifychecksum
   */
  HdfsInputStream(HdfsClient * client, const string & path,
      bool verifyChecksum=true);

public:
  uint64 getPrefetchSize() const {
    return _prefetchSize;
  }

  void setPrefetchSize(uint64 prefetchSize) {
    _prefetchSize = prefetchSize;
  }

  virtual ~HdfsInputStream();

  virtual void seek(uint64 position);

  virtual uint64 tell();

  virtual uint64 getLength();

  virtual int64 read(void * buff, uint64 length);

  virtual void close();

};


}
}

#endif /* HDFSINPUTSTREAM_H_ */
