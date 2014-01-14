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

#ifndef YARNIMPL_H_
#define YARNIMPL_H_

#include "yarn_protos.pb.h"
#include "yarn_service_protos.pb.h"
#include "hadoop/common/RpcClient.h"
#include "yarn.h"

namespace hadoop {
namespace yarn {

using namespace common;

void convert(const ApplicationId & from, ApplicationIdProto & to);

void convert(const ApplicationIdProto & from, ApplicationId & to);

void convert(const ApplicationAttemptId & from, ApplicationAttemptIdProto & to);

void convert(const ApplicationAttemptIdProto & from, ApplicationAttemptId & to);

void convert(const Resource & from, ResourceProto & to);

void convert(const ResourceProto & from, Resource & to);


class ContainerLaunchContextImpl {
public:
  map<string, string> localResources;
  map<string, string> environment;
  vector<string> commands;
  map<string, string> serviceData;
  ApplicationACLMap acls;
  string tokens;
  void to(ContainerLaunchContextProto & proto);
};



/**
 * ApplicationSubmissionContext represents all of the
 * information needed by the ResourceManager to launch
 * the ApplicationMaster for an application.
 */
class ApplicationSubmissionContext {
public:
  ApplicationId applicationId;
  string applicationName;
  string queue;
  int32 priority;
  ContainerLaunchContext amContainer;
  bool isUnmanagedAM;
  bool cancelTokensWhenComplete;
  int32 maxAppAttempts;
  Resource resource;
  string applicationType;
  void to(ApplicationSubmissionContextProto & proto);
};



class ApplicationResourceUsageReportImpl {
public:
  int32 num_used_containers;
  int32 num_reserved_containers;
  Resource used_resources;
  Resource reserved_resources;
  Resource needed_resources;
  void from(const ApplicationResourceUsageReportProto & proto);
};



class ApplicationReportImpl {
public:
  ApplicationReportProto report;
  unique_ptr<ApplicationId> appId;
  unique_ptr<ApplicationAttemptId> attemptId;
  unique_ptr<ApplicationResourceUsageReport> resourceUsage;
  unique_ptr<Token> clientAmToken;
  unique_ptr<Token> amRmToken;
  void from(const ApplicationReportProto & proto);
};



class ApplicationImpl {
public:
  ApplicationSubmissionContext context;
};



class YarnClientImpl {
private:
  RpcClient client;
public:
  YarnClientImpl(const User & user, const string & addr);
  ~YarnClientImpl();
  ApplicationImpl * create();
  void submit(ApplicationImpl & app);
  bool kill(ApplicationImpl & app);
  ApplicationReportImpl * getReport(ApplicationImpl & app);
};

}
}

#endif /* YARNIMPL_H_ */
