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
#include "hadoop/common/Security.h"
#include "hadoop/yarn/Yarn.h"

namespace hadoop {
namespace yarn {

TEST(YarnClient, CreateKill) {
  const char * yarnaddr = getenv("TEST_HDFS_ADDR");
  if (yarnaddr == nullptr) {
    return;
  }
  YarnClient client(User::GetLoginUser(), yarnaddr);
  Application * app = client.create();
  LOG_INFO("ApplicationId: %s", app->getId().toString().c_str());
  Thread::Sleep(1000);
  app->setName("testapp");
  app->setType("normaltype");
  client.submit(*app);
  Thread::Sleep(1000);
  bool killed = client.kill(*app);
  LOG_INFO("Killed: %s", Strings::ToString(killed).c_str());
}

}
}
