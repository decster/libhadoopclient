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


#include "Yarn.h"
#include "YarnImpl.h"

namespace hadoop {
namespace yarn {

void ApplicationId::fromString(const string & s) {
  vector<string> fields;
  Strings::Split(s, "_", fields, false);
  if (fields.size() != 3) {
    THROW_EXCEPTION(IllegalArgumentException, "Illegal ApplicationId format");
  }
  clusterTimestamp = Strings::toInt(fields[1]);
  id = Strings::toInt(fields[2]);
}

string ApplicationId::toString() const {
  return Strings::Format("application_%lld_%04d", clusterTimestamp, id);
}

void ApplicationAttemptId::fromString(const string & s) {
  vector<string> fields;
  Strings::Split(s, "_", fields, false);
  if (fields.size() != 4) {
    THROW_EXCEPTION(IllegalArgumentException,
        "Illegal ApplicationAttemptId format");
  }
  applicationId.clusterTimestamp = Strings::toInt(fields[1]);
  applicationId.id = Strings::toInt(fields[2]);
  id = Strings::toInt(fields[3]);
}

string ApplicationAttemptId::toString() const {
  return Strings::Format("appattempt_%lld_%04d_%06d",
      applicationId.clusterTimestamp, applicationId.id, id);
}

void ContainerId::fromString(const string & s) {
  vector<string> fields;
  Strings::Split(s, "_", fields, false);
  if (fields.size() != 5) {
    THROW_EXCEPTION(IllegalArgumentException, "Illegal ContainerId format");
  }
  attemptId.applicationId.clusterTimestamp = Strings::toInt(fields[1]);
  attemptId.applicationId.id = Strings::toInt(fields[2]);
  attemptId.id = Strings::toInt(fields[3]);
  id = Strings::toInt(fields[4]);
}

string ContainerId::toString() const {
  return Strings::Format("container_%lld_%04d_%02d_%06d",
      attemptId.applicationId.clusterTimestamp, attemptId.applicationId.id,
      attemptId.id, id);
}

///////////////////////////////////////////////////////////

Resource::Resource() :
    memory(0), cores(0) {
}

Resource::Resource(int32 memory, int32 cores) :
    memory(memory), cores(cores) {
}

Resource::~Resource() {
}

bool Resource::operator==(Resource& rhs) const {
  return (memory == rhs.memory) && (cores == rhs.cores);
}

///////////////////////////////////////////////////////////


ContainerLaunchContext::ContainerLaunchContext(ContainerLaunchContext &) :
    _impl(nullptr) {
}

ContainerLaunchContext::ContainerLaunchContext() {
  _impl = new ContainerLaunchContextImpl();
}

ContainerLaunchContext::~ContainerLaunchContext() {
  delete (ContainerLaunchContextImpl*) _impl;
}

map<string, string>& ContainerLaunchContext::localResources() {
  return ((ContainerLaunchContextImpl *) _impl)->localResources;
}

map<string, string> & ContainerLaunchContext::environment() {
  return ((ContainerLaunchContextImpl *) _impl)->environment;
}

vector<string>& ContainerLaunchContext::commands() {
  return ((ContainerLaunchContextImpl *) _impl)->commands;
}

map<string, string>& ContainerLaunchContext::serviceData() {
  return ((ContainerLaunchContextImpl *) _impl)->serviceData;
}

ApplicationACLMap& ContainerLaunchContext::acls() {
  return ((ContainerLaunchContextImpl *) _impl)->acls;
}

string& ContainerLaunchContext::tokens() {
  return ((ContainerLaunchContextImpl *) _impl)->tokens;
}

///////////////////////////////////////////////////////////

Application::Application(Application& rhs) :
    _impl(nullptr) {
}

Application::Application(ApplicationImpl * impl) : _impl(impl) {
}

Application::~Application() {
  delete _impl;
  _impl = nullptr;
}

const ApplicationId& Application::getId() {
  return _impl->context.applicationId;
}

const string& Application::getName() {
  return _impl->context.applicationName;
}

void Application::setName(const string& name) {
  _impl->context.applicationName = name;
}

const string& Application::getType() {
  return _impl->context.applicationType;
}

void Application::setType(const string& type) {
  _impl->context.applicationType = type;
}

const string& Application::getQueue() {
  return _impl->context.queue;
}

void Application::setQueue(const string& queue) {
  _impl->context.queue = queue;
}

int32 Application::getPriority() {
  return _impl->context.priority;
}

void Application::setPriority(int32 priority) {
  _impl->context.priority = priority;
}

ContainerLaunchContext & Application::amContainer() {
  return _impl->context.amContainer;
}

///////////////////////////////////////////////////////////

ApplicationResourceUsageReport::ApplicationResourceUsageReport(
    ApplicationResourceUsageReportImpl * impl) :
    _impl(impl) {
}

ApplicationResourceUsageReport::~ApplicationResourceUsageReport() {
  delete _impl;
  _impl = nullptr;
}

const int32 ApplicationResourceUsageReport::numUsedContainers() {
  return _impl->num_used_containers;
}

const int32 ApplicationResourceUsageReport::numReservedContainers() {
  return _impl->num_reserved_containers;
}

const Resource & ApplicationResourceUsageReport::usedResources() {
  return _impl->used_resources;
}

const Resource & ApplicationResourceUsageReport::reservedResources() {
  return _impl->reserved_resources;
}

const Resource & ApplicationResourceUsageReport::neededResources() {
  return _impl->needed_resources;
}

///////////////////////////////////////////////////////////

ApplicationReport::ApplicationReport(ApplicationReport &) :
    _impl(nullptr) {
}

ApplicationReport::ApplicationReport(ApplicationReportImpl * impl) :
    _impl(impl) {
}

ApplicationReport::~ApplicationReport() {
  delete _impl;
  _impl = nullptr;
}

const ApplicationId * ApplicationReport::applicationId() {
  return _impl->appId.get();
}

const ApplicationAttemptId * ApplicationReport::attemptId() {
  return _impl->attemptId.get();
}

const string & ApplicationReport::user() {
  return _impl->report.user();
}

const string & ApplicationReport::queue() {
  return _impl->report.queue();
}

const string & ApplicationReport::name() {
  return _impl->report.name();
}

const string & ApplicationReport::type() {
  return _impl->report.applicationtype();
}

const string & ApplicationReport::host() {
  return _impl->report.host();
}

const int32 ApplicationReport::rpcProt() {
  return _impl->report.rpc_port();
}

const string & ApplicationReport::trackingUrl() {
  return _impl->report.trackingurl();
}

const string & ApplicationReport::originalTrackingUrl() {
  return _impl->report.originaltrackingurl();
}

const string & ApplicationReport::diagnostics() {
  return _impl->report.diagnostics();
}

int64 ApplicationReport::startTime() {
  return _impl->report.starttime();
}

int64 ApplicationReport::finishTime() {
  return _impl->report.finishtime();
}

float ApplicationReport::progress() {
  return _impl->report.progress();
}

YarnApplicationState ApplicationReport::state() {
  return (YarnApplicationState)_impl->report.yarn_application_state();
}

FinalApplicationStatus ApplicationReport::finalStatus() {
  return (FinalApplicationStatus)_impl->report.final_application_status();
}

ApplicationResourceUsageReport * ApplicationReport::resourceUsage() {
  return _impl->resourceUsage.get();
}

const Token * ApplicationReport::clientAmToken() {
  return _impl->clientAmToken.get();
}

const Token * ApplicationReport::amRmToken() {
  return _impl->amRmToken.get();
}

///////////////////////////////////////////////////////////

YarnClient::YarnClient(YarnClient& rhs) : _impl(nullptr) {
}

YarnClient::YarnClient(const User & user, const string & addr) :
  _impl(new YarnClientImpl(user, addr)) {
}

YarnClient::~YarnClient() {
  delete _impl;
  _impl = nullptr;
}

Application * YarnClient::create() {
  return new Application(_impl->create());
}

void YarnClient::submit(Application & app) {
  _impl->submit(*app._impl);
}

bool YarnClient::kill(Application & app) {
  return _impl->kill(*app._impl);
}

ApplicationReport * YarnClient::getReport(Application & app) {
  return new ApplicationReport(_impl->getReport(*app._impl));
}

}
}
