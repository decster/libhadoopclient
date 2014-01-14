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

#include "YarnImpl.h"

namespace hadoop {
namespace yarn {

void convert(const ApplicationId & from, ApplicationIdProto & to) {
  to.set_id(from.id);
  to.set_cluster_timestamp(from.clusterTimestamp);
}

void convert(const ApplicationIdProto & from, ApplicationId & to) {
  to.clusterTimestamp = from.cluster_timestamp();
  to.id = from.id();
}

void convert(const ApplicationAttemptId & from,
    ApplicationAttemptIdProto & to) {
  convert(from.applicationId, *to.mutable_application_id());
  to.set_attemptid(from.id);
}

void convert(const ApplicationAttemptIdProto & from,
    ApplicationAttemptId & to) {
  convert(from.application_id(), to.applicationId);
  to.id = from.attemptid();
}

void convert(const Resource & from, ResourceProto & to) {
  to.set_memory(from.memory);
  to.set_virtual_cores(from.cores);
}

void convert(const ResourceProto & from, Resource & to) {
  to.memory = from.memory();
  to.cores = from.virtual_cores();
}

void ContainerLaunchContextImpl::to(ContainerLaunchContextProto & proto) {
  proto.Clear();
  for (auto & kv : localResources) {
    // TODO
  }
  for (auto & kv : environment) {
    auto kvp = proto.add_environment();
    kvp->set_key(kv.first);
    kvp->set_value(kv.second);
  }
  for (auto & v : commands) {
    proto.add_command(v);
  }
  for (auto & kv : serviceData) {
    auto kvp = proto.add_service_data();
    kvp->set_key(kv.first);
    kvp->set_value(kv.second);
  }
  for (auto & kv : acls) {
    auto kvp = proto.add_application_acls();
    kvp->set_accesstype((ApplicationAccessTypeProto)kv.first);
    kvp->set_acl(kv.second);
  }
  if (!tokens.empty()) {
    proto.set_tokens(tokens);
  }
}

void ApplicationSubmissionContext::to(
    ApplicationSubmissionContextProto & proto) {
  proto.Clear();
  convert(applicationId, *proto.mutable_application_id());
  proto.set_application_name(applicationName);
  proto.set_queue(queue);
  proto.mutable_priority()->set_priority(priority);
  amContainer.impl()->to(*proto.mutable_am_container_spec());
  proto.set_unmanaged_am(isUnmanagedAM);
  proto.set_cancel_tokens_when_complete(cancelTokensWhenComplete);
  proto.set_maxappattempts(maxAppAttempts);
  convert(resource, *proto.mutable_resource());
  proto.set_applicationtype(applicationType);
}

void ApplicationResourceUsageReportImpl::from(
    const ApplicationResourceUsageReportProto & proto) {
  num_used_containers = proto.num_used_containers();
  num_reserved_containers = proto.num_used_containers();
  convert(proto.used_resources(), used_resources);
  convert(proto.reserved_resources(), reserved_resources);
  convert(proto.needed_resources(), needed_resources);
}

void ApplicationReportImpl::from(const ApplicationReportProto & proto) {
  report.CopyFrom(proto);
  if (report.has_applicationid()) {
    appId.reset(new ApplicationId());
    convert(report.applicationid(), *appId);
  }
  if (report.has_currentapplicationattemptid()) {
    attemptId.reset(new ApplicationAttemptId());
    convert(report.currentapplicationattemptid(), *attemptId);
  }
  if (report.has_app_resource_usage()) {
    ApplicationResourceUsageReportImpl * impl =
        new ApplicationResourceUsageReportImpl();
    impl->from(report.app_resource_usage());
    resourceUsage.reset(new ApplicationResourceUsageReport(impl));
  }
  if (report.has_client_to_am_token()) {
    clientAmToken.reset(new Token());
    clientAmToken->from(report.client_to_am_token());
  }
  if (report.has_am_rm_token()) {
    amRmToken.reset(new Token());
    amRmToken->from(report.am_rm_token());
  }
}

YarnClientImpl::YarnClientImpl(const User & user, const string & addr) :
    client("org.apache.hadoop.yarn.api.ApplicationClientProtocolPB", 1, user,
        addr) {
}

YarnClientImpl::~YarnClientImpl() {
}

ApplicationImpl * YarnClientImpl::create() {
  GetNewApplicationRequestProto req;
  GetNewApplicationResponseProto res;
  client.call("getNewApplication", req, res);
  ApplicationImpl * ret = new ApplicationImpl();
  convert(res.application_id(), ret->context.applicationId);
  return ret;
}

void YarnClientImpl::submit(ApplicationImpl & app) {
  SubmitApplicationRequestProto req;
  SubmitApplicationResponseProto res;
  app.context.to(*req.mutable_application_submission_context());
  client.call("submitApplication", req, res);
}

bool YarnClientImpl::kill(ApplicationImpl & app) {
  KillApplicationRequestProto req;
  KillApplicationResponseProto res;
  convert(app.context.applicationId, *req.mutable_application_id());
  client.call("forceKillApplication", req, res);
  return res.is_kill_completed();
}

ApplicationReportImpl * YarnClientImpl::getReport(ApplicationImpl & app) {
  GetApplicationReportRequestProto req;
  GetApplicationReportResponseProto res;
  convert(app.context.applicationId, *req.mutable_application_id());
  client.call("getApplicationReport", req, res);
  if (!res.has_application_report()) {
    return nullptr;
  }
  ApplicationReportImpl * impl = new ApplicationReportImpl;
  impl->from(res.application_report());
  return impl;
}

}
}

