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

#include "gtest/gtest.h"
#include "hadoop/hdfs/Hdfs.h"

namespace hadoop {
namespace hdfs {

TEST(Hdfs, HdfsClient) {
  const char * hdfsaddr = getenv("TEST_HDFS_ADDR");
  if (hdfsaddr == nullptr) {
    return;
  }
  HdfsClient client(hdfsaddr);
  LOG("clientName: %s", client.getClientName().c_str());
  FileStatus st;
  if (client.getStatus("/aa", st)) {
    client.remove("/aa", true);
  }
  client.mkdir("/aa/bb/cc", FsPermission::DirDefault, true);
  vector<FileStatus> fss;
  if (client.listStatus("/aa", fss)) {
    for (auto & fs : fss) {
      LOG("entry: %s", fs.toString().c_str());
    }
    ASSERT_EQ(1, fss.size());
    ASSERT_STREQ("/aa/bb", fss[0].path.c_str());
  }
}

TEST(Hdfs, HdfsInputStream) {
  const char * hdfsaddr = getenv("TEST_HDFS_ADDR");
  if (hdfsaddr == nullptr) {
    return;
  }
  HdfsClient client(hdfsaddr);
  FileStatus status;
  if (!client.getStatus("/test.dat", status)) {
    LOG("Hdfs.HdfsInputStream skipped because /test.dat does not exists");
    return;
  }
  LOG("test.dat: %s", status.toString().c_str());
  unique_ptr<InputStream> fin(client.open("/test.dat", true));
  Buffer buff(11111);
  uint64 total = 0;
  while (true) {
    int64 rd = fin->read(buff);
    if (rd <= 0) {
      break;
    }
    total += rd;
    buff.reset();
  }
  LOG("Total read: %llu", total);
  ASSERT_EQ(status.length, total);
}

TEST(Hdfs, HdfsOutputStream) {
  const char * hdfsaddr = getenv("TEST_HDFS_ADDR");
  if (hdfsaddr == nullptr) {
    return;
  }
  string path = "/writetest.dat";
  HdfsClient client(hdfsaddr);
  if (client.exists(path)) {
    client.remove(path, true);
  }
  unique_ptr<OutputStream> fout(
      client.createEx(path, CreateFlag::Default,
          FsPermission::FileDefault, false, 1, 2560000));
  string rbytes;
  Random::NextBytes(1111, rbytes);
  uint64 total = 11110000;
  uint64 totalwt = 0;
  while (totalwt < total) {
    uint64 wt = fout->write(rbytes.data(), rbytes.size());
    ASSERT_EQ(rbytes.size(), wt);
    totalwt += wt;
  }
  ASSERT_EQ(totalwt, total);
}

TEST(Hdfs, HdfsOutputStreamBig) {
  const char * hdfsaddr = getenv("TEST_HDFS_ADDR");
  if (hdfsaddr == nullptr) {
    return;
  }
  string path = "/writetest100.dat";
  HdfsClient client(hdfsaddr);
  if (client.exists(path)) {
    client.remove(path, true);
  }
  unique_ptr<OutputStream> fout(
      client.createEx(path, CreateFlag::Default,
          FsPermission::FileDefault, false, 1, 25600000));
  string rbytes;
  Random::NextBytes(1111, rbytes);
  size_t total = 100 * 1024 * 1024;
  size_t totalwt = 0;
  while (totalwt < total) {
    size_t wtmax = std::min(rbytes.size(), total - totalwt);
    uint64 wt = fout->write(rbytes.data(), wtmax);
    ASSERT_EQ(wtmax, wt);
    totalwt += wt;
  }
  ASSERT_EQ(totalwt, total);
}

}
}
