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
#include "ClientNamenodeProtocol.h"

namespace hadoop {
namespace hdfs {

static void convert(const HdfsFileStatusProto & src, FileStatus & dst,
    const string & parent) {
  dst.type = (FileType) src.filetype();
  // HdfsFileStatusProto.path is name only
  // status.name maybe empty if src is a file, see
  // https://issues.apache.org/jira/browse/HDFS-1743
  if (src.path().empty()) {
    dst.path = parent;
  }
  else {
    dst.path = Path::JoinPath(parent, src.path());
  }
  dst.length = src.length();
  if (src.has_symlink()) {
    dst.symlink = src.symlink();
  }
  else {
    dst.symlink.clear();
  }
  dst.owner = src.owner();
  dst.group = src.group();
  dst.permission.mask = (uint16_t) (src.permission().perm());
  dst.blockReplication = src.block_replication();
  dst.blockSize = src.blocksize();
  dst.modificationTime = src.modification_time();
  dst.accessTime = src.access_time();
  dst.fileId = src.fileid();
  dst.childrenNum = src.childrennum();
}

static void convert(const DirectoryListingProto & src, DirectoryListing & dst,
    const string & parent) {
  dst.status.clear();
  dst.locations.clear();
  for (int i = 0; i < src.partiallisting_size(); i++) {
    dst.status.push_back(FileStatus());
    convert(src.partiallisting(i), dst.status[i], parent);
  }
  dst.remainingEntries = src.remainingentries();
}

///////////////////////////////////////////////////////////

RpcClient & ClientNamenodeProtocol::getClient() {
  return _client;
}

void ClientNamenodeProtocol::getBlockLocations(const string & src,
    uint64 offset, uint64 length, LocatedBlocks & blocks) {
  static string method("getBlockLocations");
  GetBlockLocationsRequestProto req;
  GetBlockLocationsResponseProto res;
  req.set_src(src);
  req.set_offset(offset);
  req.set_length(length);
  _client.call(method, req, res);
  if (res.has_locations()) {
    // TODO: check if java return null?
    blocks.from(res.locations());
  }
}

void ClientNamenodeProtocol::getServerDefaults(FsServerDefaults & defaults) {
  static string method("getServerDefaults");
  GetServerDefaultsRequestProto req;
  GetServerDefaultsResponseProto res;
  _client.call(method, req, res);
  const FsServerDefaultsProto & src = res.serverdefaults();
  defaults.blockSize = src.blocksize();
  defaults.bytesPerChecksum = src.bytesperchecksum();
  defaults.writePacketSize = src.writepacketsize();
  defaults.replication = src.replication();
  defaults.fileBufferSize = src.filebuffersize();
  defaults.encryptDataTransfer = src.encryptdatatransfer();
  defaults.trashInterval = src.trashinterval();
  defaults.checksumType = (ChecksumType) src.checksumtype();
}

void ClientNamenodeProtocol::create(const string & src, FsPermission masked,
    const string & clientName, CreateFlag flag, bool createParent,
    uint16_t replication, uint64 blockSize, FileStatus & st) {
  static string method("create");
  CreateRequestProto req;
  CreateResponseProto res;
  req.set_src(src);
  req.mutable_masked()->set_perm(masked.mask);
  req.set_clientname(clientName);
  req.set_createflag(flag.flag);
  req.set_createparent(createParent);
  req.set_replication(replication);
  req.set_blocksize(blockSize);
  _client.call(method, req, res);
  convert(res.fs(), st, string());
  if (st.path.empty()) {
    st.path = src;
  }
}

void ClientNamenodeProtocol::append(const string & src,
    const string & clientName, LocatedBlock & block) {
  // TODO:
}

bool ClientNamenodeProtocol::setReplication(const string & src,
    uint16_t replicaiton) {
  static string method("setReplication");
  SetReplicationRequestProto req;
  SetReplicationResponseProto res;
  req.set_src(src);
  req.set_replication(replicaiton);
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::setPermission(const string & src,
    FsPermission permission) {
  static string method("setPermission");
  SetPermissionRequestProto req;
  SetPermissionResponseProto res;
  req.set_src(src);
  req.mutable_permission()->set_perm(permission.mask);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::setOwner(const string & src, const string & user,
    const string & group) {
  static string method("setPermission");
  SetOwnerRequestProto req;
  SetOwnerResponseProto res;
  req.set_src(src);
  req.set_username(user);
  req.set_groupname(group);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::abandonBlock(ExtendedBlock & block,
    const string & src, const string & holder) {
  static string method("abandonBlock");
  AbandonBlockRequestProto req;
  AbandonBlockResponseProto res;
  block.to(*req.mutable_b());
  req.set_src(src);
  req.set_holder(holder);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::addBlock(const string & src, uint64 fileId,
    const string & clientName, ExtendedBlock * previous,
    const vector<DatanodeInfo> & excludeNodes, const vector<string> favorNodes,
    LocatedBlock & locatedBlock) {
  static string method("addBlock");
  AddBlockRequestProto req;
  AddBlockResponseProto res;
  req.set_src(src);
  req.set_fileid(fileId);
  req.set_clientname(clientName);
  if (previous != NULL) {
    previous->to(*(req.mutable_previous()));
  }
  for (auto & node : excludeNodes) {
    node.to(*req.add_excludenodes());
  }
  for (auto & node : favorNodes) {
    req.add_favorednodes(node);
  }
  _client.call(method, req, res);
  locatedBlock.from(res.block());
}

void ClientNamenodeProtocol::getAdditionalDatanode(const string & src,
    ExtendedBlock & blk, vector<DatanodeInfo> & existings,
    vector<DatanodeInfo> & excludes, uint32_t numAdditionalNodes,
    const string & clientName, LocatedBlock & locatedBlock) {
  static string method("getAdditionalDatanode");
  GetAdditionalDatanodeRequestProto req;
  GetAdditionalDatanodeResponseProto res;
  req.set_src(src);
  blk.to(*req.mutable_blk());
  for (size_t i = 0; i < existings.size(); i++) {
    DatanodeInfoProto * dstInfo = req.add_existings();
    existings[i].to(*dstInfo);
  }
  for (size_t i = 0; i < excludes.size(); i++) {
    DatanodeInfoProto * dstInfo = req.add_excludes();
    excludes[i].to(*dstInfo);
  }
  req.set_numadditionalnodes(numAdditionalNodes);
  req.set_clientname(clientName);
  _client.call(method, req, res);
  locatedBlock.from(res.block());
}

bool ClientNamenodeProtocol::complete(const string & src, uint64 fileId,
    const string & clientName, ExtendedBlock * last) {
  static string method("complete");
  CompleteRequestProto req;
  CompleteResponseProto res;
  req.set_src(src);
  req.set_fileid(fileId);
  req.set_clientname(clientName);
  if (last != NULL) {
    ExtendedBlockProto * dstLast = req.mutable_last();
    last->to(*dstLast);
  }
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::reportBadBlocks(vector<LocatedBlock> & blocks) {
  static string method("reportBadBlocks");
  ReportBadBlocksRequestProto req;
  ReportBadBlocksResponseProto res;
  for (size_t i = 0; i < blocks.size(); i++) {
    LocatedBlockProto * dst = req.add_blocks();
    blocks[i].to(*dst);
  }
  _client.call(method, req, res);
}

bool ClientNamenodeProtocol::rename(const string & src, const string & dst) {
  static string method("rename");
  RenameRequestProto req;
  RenameResponseProto res;
  req.set_src(src);
  req.set_dst(dst);
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::concat(const string & trg,
    const vector<string> & srcs) {
  // TODO:
}

void ClientNamenodeProtocol::rename2(const string & src, const string & dst,
    bool overwrite) {
  static string method("rename2");
  Rename2RequestProto req;
  Rename2ResponseProto res;
  req.set_src(src);
  req.set_dst(dst);
  req.set_overwritedest(overwrite);
  _client.call(method, req, res);
}

bool ClientNamenodeProtocol::delete_(const string & src, bool recursive) {
  static string method("delete");
  DeleteRequestProto req;
  DeleteResponseProto res;
  req.set_src(src);
  req.set_recursive(recursive);
  _client.call(method, req, res);
  return res.result();
}

bool ClientNamenodeProtocol::mkdirs(const string & src, FsPermission masked,
    bool createParent) {
  static string method("mkdirs");
  MkdirsRequestProto req;
  MkdirsResponseProto res;
  req.set_src(src);
  req.mutable_masked()->set_perm(masked.mask);
  req.set_createparent(createParent);
  _client.call(method, req, res);
  return res.result();
}

bool ClientNamenodeProtocol::getListing(const string & src,
    const string & startAfter, bool needLocation, DirectoryListing & listing) {
  static string method("getListing");
  GetListingRequestProto req;
  GetListingResponseProto res;
  req.set_src(src);
  req.set_startafter(startAfter);
  req.set_needlocation(needLocation);
  _client.call(method, req, res);
  if (res.has_dirlist()) {
    convert(res.dirlist(), listing, src);
    return true;
  }
  else {
    return false;
  }
}

void ClientNamenodeProtocol::renewLease(const string & clientName) {
  static string method("renewLease");
  RenewLeaseRequestProto req;
  RenewLeaseResponseProto res;
  req.set_clientname(clientName);
  _client.call(method, req, res);
}

bool ClientNamenodeProtocol::recoverLease(const string & src,
    const string & clientName) {
  static string method("recoverLease");
  RecoverLeaseRequestProto req;
  RecoverLeaseResponseProto res;
  req.set_src(src);
  req.set_clientname(clientName);
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::getFsStats(FileSystemStats & stats) {
  static string method("getFsStats");
  GetFsStatusRequestProto req;
  GetFsStatsResponseProto res;
  _client.call(method, req, res);
  stats.capacity = res.capacity();
  stats.used = res.used();
  stats.remaining = res.remaining();
  stats.under_replicated = res.under_replicated();
  stats.corrupt_blocks = res.corrupt_blocks();
  stats.missing_blocks = res.missing_blocks();
  stats.blockPoolUsed = 0;
}

void ClientNamenodeProtocol::getDatanodeReport(DatanodeReportType type,
    vector<DatanodeInfo> & datanodes) {
  static string method("getDatanodeReport");
  GetDatanodeReportRequestProto req;
  GetDatanodeReportResponseProto res;
  req.set_type((DatanodeReportTypeProto) type);
  _client.call(method, req, res);
  for (int i = 0; i < res.di_size(); i++) {
    datanodes.push_back(DatanodeInfo());
    datanodes[i].from(res.di(i));
  }
}

uint64 ClientNamenodeProtocol::getPreferredBlockSize(
    const string & filename) {
  static string method("getPreferredBlockSize");
  GetPreferredBlockSizeRequestProto req;
  GetPreferredBlockSizeResponseProto res;
  req.set_filename(filename);
  _client.call(method, req, res);
  return res.bsize();
}

bool ClientNamenodeProtocol::setSafeMode(SafeModeAction action) {
  static string method("setSafeMode");
  SetSafeModeRequestProto req;
  SetSafeModeResponseProto res;
  req.set_action((SafeModeActionProto) action);
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::saveNamespace() {
  static string method("saveNamespace");
  SaveNamespaceRequestProto req;
  SaveNamespaceResponseProto res;
  _client.call(method, req, res);
}

uint64 ClientNamenodeProtocol::rollEdits() {
  static string method("rollEdits");
  RollEditsRequestProto req;
  RollEditsResponseProto res;
  _client.call(method, req, res);
  return res.newsegmenttxid();
}

bool ClientNamenodeProtocol::restoreFailedStorage(const string & arg) {
  static string method("restoreFailedStorage");
  RestoreFailedStorageRequestProto req;
  RestoreFailedStorageResponseProto res;
  req.set_arg(arg);
  _client.call(method, req, res);
  return res.result();
}

void ClientNamenodeProtocol::refreshNodes() {
  static string method("refreshNodes");
  RefreshNodesRequestProto req;
  RefreshNodesResponseProto res;
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::finalizeUpgrade() {
  static string method("finalizeUpgrade");
  FinalizeUpgradeRequestProto req;
  FinalizeUpgradeResponseProto res;
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::metaSave(const string & filename) {
  static string method("metaSave");
  MetaSaveRequestProto req;
  MetaSaveResponseProto res;
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::setBalancerBandwidth(uint64 bandwidth) {
  static string method("setBalancerBandwidth");
  SetBalancerBandwidthRequestProto req;
  req.set_bandwidth(bandwidth);
  SetBalancerBandwidthResponseProto res;
  _client.call(method, req, res);
}

bool ClientNamenodeProtocol::getFileInfo(const string & src,
    FileStatus & status) {
  static string method("getFileInfo");
  GetFileInfoRequestProto req;
  GetFileInfoResponseProto res;
  req.set_src(src);
  _client.call(method, req, res);
  if (res.has_fs()) {
    convert(res.fs(), status, src);
    return true;
  }
  else {
    return false;
  }
}

bool ClientNamenodeProtocol::getFileLinkInfo(const string & src,
    FileStatus & status) {
  static string method("getFileLinkInfo");
  GetFileLinkInfoRequestProto req;
  GetFileLinkInfoResponseProto res;
  req.set_src(src);
  _client.call(method, req, res);
  if (res.has_fs()) {
    convert(res.fs(), status, src);
    return true;
  }
  else {
    return false;
  }
}

void ClientNamenodeProtocol::getContentSummary(const string & path,
    ContentSummary & summary) {
  static string method("getContentSummary");
  GetContentSummaryRequestProto req;
  GetContentSummaryResponseProto res;
  req.set_path(path);
  _client.call(method, req, res);
  const ContentSummaryProto & proto = res.summary();
  summary.length = proto.length();
  summary.fileCount = proto.filecount();
  summary.directoryCount = proto.directorycount();
  summary.quota = proto.quota();
  summary.spaceConsumed = proto.spaceconsumed();
  summary.spaceQuota = proto.spacequota();
}

void ClientNamenodeProtocol::setQuota(const string & path,
    int64_t namespaceQuota, int64_t diskspaceQuota) {
  static string method("setQuota");
  SetQuotaRequestProto req;
  SetQuotaResponseProto res;
  req.set_path(path);
  req.set_namespacequota(namespaceQuota);
  req.set_diskspacequota(diskspaceQuota);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::fsync(const string & src, const string & client) {
  static string method("fsync");
  FsyncRequestProto req;
  FsyncResponseProto res;
  req.set_src(src);
  req.set_client(client);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::setTimes(const string & src, uint64 mtime,
    uint64 atime) {
  static string method("setTimes");
  SetTimesRequestProto req;
  SetTimesResponseProto res;
  req.set_src(src);
  req.set_mtime(mtime);
  req.set_atime(atime);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::createSymlink(const string & target,
    const string & link, FsPermission dirPerm, bool createParent) {
  static string method("createSymlink");
  CreateSymlinkRequestProto req;
  CreateSymlinkResponseProto res;
  req.set_target(target);
  req.set_link(link);
  req.mutable_dirperm()->set_perm(dirPerm.mask);
  req.set_createparent(createParent);
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::getLinkTarget(const string & path,
    string & target) {
  static string method("getLinkTarget");
  GetLinkTargetRequestProto req;
  GetLinkTargetResponseProto res;
  req.set_path(path);
  _client.call(method, req, res);
  target = res.targetpath();
}

void ClientNamenodeProtocol::updateBlockForPipeline(ExtendedBlock & block,
    const string & clientName, LocatedBlock & updated) {
  static string method("updateBlockForPipeline");
  UpdateBlockForPipelineRequestProto req;
  UpdateBlockForPipelineResponseProto res;
  block.to(*req.mutable_block());
  req.set_clientname(clientName);
  _client.call(method, req, res);
  updated.from(res.block());
}

void ClientNamenodeProtocol::updatePipeline(const string & clientName,
    ExtendedBlock & oldBlock, ExtendedBlock & newBlock,
    vector<DatanodeID> & newNodes) {
  static string method("updatePipeline");
  UpdatePipelineRequestProto req;
  UpdatePipelineResponseProto res;
  req.set_clientname(clientName);
  oldBlock.to(*req.mutable_oldblock());
  newBlock.to(*req.mutable_newblock());
  for (size_t i = 0; i < newNodes.size(); i++) {
    newNodes[i].to(*req.add_newnodes());
  }
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::getDelegationToken(const string & renewer,
    Token & token) {
  static string method("getDelegationToken");
  GetDelegationTokenRequestProto req;
  GetDelegationTokenResponseProto res;
  req.set_renewer(renewer);
  _client.call(method, req, res);
  token.from(res.token());
}

void ClientNamenodeProtocol::renewDelegationToken(Token & token) {
  static string method("renewDelegationToken");
  RenewDelegationTokenRequestProto req;
  RenewDelegationTokenResponseProto res;
  token.to(*req.mutable_token());
  _client.call(method, req, res);
}

void ClientNamenodeProtocol::cancelDelegationToken(Token & token) {
  static string method("cancelDelegationToken");
  CancelDelegationTokenRequestProto req;
  CancelDelegationTokenResponseProto res;
  token.to(*req.mutable_token());
  _client.call(method, req, res);
}

}
}
