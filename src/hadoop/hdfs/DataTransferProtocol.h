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

#ifndef DATATRANSFERPROTOCOL_H_
#define DATATRANSFERPROTOCOL_H_

#include "datatransfer.pb.h"
#include "hadoop/common/Common.h"
#include "Hdfs.h"

namespace hadoop {
namespace hdfs {

using namespace ::hadoop::common;


class DataTransferProtocol {
private:
  Buffer & _dst;
public:
  enum Op {
    WRITE_BLOCK = 80,
    READ_BLOCK = 81,
    READ_METADATA = 82,
    REPLACE_BLOCK = 83,
    COPY_BLOCK = 84,
    BLOCK_CHECKSUM = 85,
    TRANSFER_BLOCK = 86,
    REQUEST_SHORT_CIRCUIT_FDS = 87,
  };

  /**
   * Version for data transfers between clients and datanodes
   * This should change when serialization of DatanodeInfo, not just
   * when protocol changes. It is not very obvious.
   */
  static const int16 DATA_TRANSFER_VERSION = 28;

  DataTransferProtocol(Buffer & dst) : _dst(dst) {}

  /**
   * Read a block.
   * @param blk the block being read.
   * @param blockToken security token for accessing the block.
   * @param clientName client's name.
   * @param blockOffset offset of the block.
   * @param length maximum number of bytes for this read.
   */
  void readBlock(ExtendedBlock & blk, Token * token,
      const string & clientName, uint64 blockOffset, uint64 length);

  /**
   * Write a block to a datanode pipeline.
   *
   * @param blk the block being written.
   * @param token security token for accessing the block.
   * @param clientName client's name.
   * @param targets target datanodes in the pipeline.
   * @param source source datanode.
   * @param stage pipeline stage.
   * @param pipelineSize the size of the pipeline.
   * @param minBytesRcvd minimum number of bytes received.
   * @param maxBytesRcvd maximum number of bytes received.
   * @param latestGenerationStamp the latest generation stamp of the block.
   */
  void writeBlock(ExtendedBlock & blk, Token * token,
      const string & clientName, vector<DatanodeInfo> & targets,
      DatanodeInfo * source, OpWriteBlockProto_BlockConstructionStage stage,
      uint32 pipelineSize, uint64 minBytesRcvd, uint64 maxBytesRcvd,
      uint64 latestGenerationStamp, Checksum & requestedChecksum);

  /**
   * Get block checksum (MD5 of CRC32).
   *
   * @param dst dest buffer
   * @param blk a block.
   * @param blockToken security token for accessing the block.
   * @throws IOException
   */
  void blockChecksum(ExtendedBlock & blk, Token & blockToken);

  /**
   * Request short circuit access file descriptors from a DataNode.
   *
   * @param dst dest buffer
   * @param blk The block to get file descriptors for.
   * @param blockToken Security token for accessing the block.
   * @param maxVersion Maximum version of the block data the client
   *                   can understand.
   */
  void requestShortCircuitFds(ExtendedBlock & blk,
      Token & blockToken, int maxVersion);

private:
  void writeOp(Op op, ::google::protobuf::MessageLite & msg);
};

}
}

#endif /* DATATRANSFERPROTOCOL_H_ */
