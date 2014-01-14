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

namespace hadoop {
namespace hdfs {

void DataTransferProtocol::readBlock(ExtendedBlock & blk, Token * token,
    const string & clientName, uint64 blockOffset, uint64 length) {
  OpReadBlockProto proto;
  ClientOperationHeaderProto * header = proto.mutable_header();
  header->set_clientname(clientName);
  BaseHeaderProto * baseHeader = header->mutable_baseheader();
  blk.to(*baseHeader->mutable_block());
  if (token) {
    token->to(*baseHeader->mutable_token());
  }
  proto.set_offset(blockOffset);
  proto.set_len(length);
  writeOp(READ_BLOCK, proto);
}

void DataTransferProtocol::writeBlock(ExtendedBlock& blk,
    Token * token, const string& clientName,
    vector<DatanodeInfo>& targets,
    DatanodeInfo * source, OpWriteBlockProto_BlockConstructionStage stage,
    uint32 pipelineSize, uint64 minBytesRcvd, uint64 maxBytesRcvd,
    uint64 latestGenerationStamp, Checksum& requestedChecksum) {
  OpWriteBlockProto proto;
  ClientOperationHeaderProto * header = proto.mutable_header();
  header->set_clientname(clientName);
  BaseHeaderProto * baseHeader = header->mutable_baseheader();
  blk.to(*baseHeader->mutable_block());
  if (token) {
    token->to(*baseHeader->mutable_token());
  }
  proto.set_stage(stage);
  proto.set_pipelinesize(pipelineSize);
  proto.set_minbytesrcvd(minBytesRcvd);
  proto.set_maxbytesrcvd(maxBytesRcvd);
  proto.set_latestgenerationstamp(latestGenerationStamp);
  ChecksumProto * cs = proto.mutable_requestedchecksum();
  cs->set_type((ChecksumTypeProto)requestedChecksum.type());
  cs->set_bytesperchecksum(requestedChecksum.bytesPerChecksum());
  writeOp(WRITE_BLOCK, proto);
}

void DataTransferProtocol::blockChecksum(ExtendedBlock& blk,
    Token& blockToken) {
}

void DataTransferProtocol::requestShortCircuitFds(
    ExtendedBlock& blk, Token& blockToken, int maxVersion) {
}

void DataTransferProtocol::writeOp(Op op,
    ::google::protobuf::MessageLite& msg) {
  // 2 byte(version) + 1 byte(op) + 5 byte(varint32) + msg size
  int maxSize = msg.ByteSize() + 8;
  if (_dst.remain() < maxSize) {
    _dst.expandLimit(maxSize - _dst.remain());
  }
  _dst.writeUInt16Be(DATA_TRANSFER_VERSION);
  _dst.writeUInt8((uint8)op);
  _dst.writeVarInt32Prefixed(msg);
}


}
}
