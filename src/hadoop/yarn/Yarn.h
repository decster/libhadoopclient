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

#ifndef YARN_H_
#define YARN_H_

#include "hadoop/common/Common.h"
#include "hadoop/common/Security.h"

namespace hadoop {
namespace yarn {

using namespace common;

/**
 * ApplicationId represents the globally unique
 * identifier for an application.
 */
class ApplicationId {
public:
  int64 clusterTimestamp;
  int32 id;
  ApplicationId() : clusterTimestamp(0), id(0) {}
  ApplicationId(int64 clusterTimestamp, int32 id) :
    clusterTimestamp(clusterTimestamp), id(id) {}
  ApplicationId(const string & s) {
    fromString(s);
  }
  void fromString(const string & s);
  string toString() const;
};



/**
 * ApplicationAttemptId denotes the particular attempt of an
 * ApplicationMaster for a given ApplicationId.
 */
class ApplicationAttemptId {
public:
  ApplicationId applicationId;
  int32 id;
  ApplicationAttemptId() : id(0) {}
  ApplicationAttemptId(int64 clusterTimestamp, int32 appId, int32 attemptId) :
      applicationId(clusterTimestamp, appId), id(attemptId) {}
  ApplicationAttemptId(const string & s) {
    fromString(s);
  }
  void fromString(const string & s);
  string toString() const;
};



/**
 * ContainerId represents a globally unique identifier
 * for a Container in the cluster
 */
class ContainerId {
public:
  ApplicationAttemptId attemptId;
  int32 id;
  ContainerId() : id(0) {}
  ContainerId(int64 clusterTimestamp, int32 appId, int32 attemptId,
      int32 containerId) :
      attemptId(clusterTimestamp, appId, attemptId), id(containerId) {}
  ContainerId(const string & s) {
    fromString(s);
  }
  void fromString(const string & s);
  string toString() const;
};



/**
 * Resource models a set of computer resources in the cluster.
 * Currently it models both memory(MB) and CPU(vcores)
 */
class Resource {
public:
  int32 memory;
  int32 cores;
  Resource();
  Resource(int32 memory, int32 cores);
  ~Resource();
  bool operator==(Resource & rhs) const;
};



/**
 * Application access types.
 */
enum ApplicationAccessType {
  APPLICATIONACCESSTYPE_VIEW_APP = 1,
  APPLICATIONACCESSTYPE_MODIFY_APP = 2,
};

typedef map<ApplicationAccessType, string> ApplicationACLMap;



/**
 * ContainerLaunchContext represents all of the information
 * needed by the NodeManager to launch a container
 */
class ContainerLaunchContextImpl;
class ContainerLaunchContext {
private:
  ContainerLaunchContextImpl * _impl;
  ContainerLaunchContext(ContainerLaunchContext &);
public:
  ContainerLaunchContext();
  ~ContainerLaunchContext();

  ContainerLaunchContextImpl * impl() {
    return _impl;
  }

  map<string, string> & localResources();
  map<string, string> & environment();
  vector<string> & commands();
  map<string, string> & serviceData();
  ApplicationACLMap & acls();
  string & tokens();
};



class ApplicationResourceUsageReportImpl;
class ApplicationResourceUsageReport {
private:
  ApplicationResourceUsageReportImpl * _impl;
  ApplicationResourceUsageReport(ApplicationResourceUsageReport &);
public:
  ApplicationResourceUsageReport(ApplicationResourceUsageReportImpl * impl);
  ~ApplicationResourceUsageReport();

  ApplicationResourceUsageReportImpl * impl() {
    return _impl;
  }

  const int32 numUsedContainers();
  const int32 numReservedContainers();
  const Resource & usedResources();
  const Resource & reservedResources();
  const Resource & neededResources();
};

enum YarnApplicationState {
  YARNAPPLICATIONSTATE_NEW = 1,
  YARNAPPLICATIONSTATE_NEW_SAVING = 2,
  YARNAPPLICATIONSTATE_SUBMITTED = 3,
  YARNAPPLICATIONSTATE_ACCEPTED = 4,
  YARNAPPLICATIONSTATE_RUNNING = 5,
  YARNAPPLICATIONSTATE_FINISHED = 6,
  YARNAPPLICATIONSTATE_FAILED = 7,
  YARNAPPLICATIONSTATE_KILLED = 8,
};

enum FinalApplicationStatus {
  FINALAPPLICATIONSTATUS_APP_UNDEFINED = 0,
  FINALAPPLICATIONSTATUS_APP_SUCCEEDED = 1,
  FINALAPPLICATIONSTATUS_APP_FAILED = 2,
  FINALAPPLICATIONSTATUS_APP_KILLED = 3,
};

/**
 * ApplicationReport is a report of an application.
 */
class ApplicationReportImpl;
class ApplicationReport {
private:
  ApplicationReportImpl * _impl;
  ApplicationReport(ApplicationReport &);
public:
  ApplicationReport(ApplicationReportImpl * impl);

  ~ApplicationReport();

  ApplicationReportImpl * impl() {
    return _impl;
  }

  const ApplicationId * applicationId();
  const ApplicationAttemptId * attemptId();
  const string & user();
  const string & queue();
  const string & name();
  const string & type();
  const string & host();
  const int32 rpcProt();
  const string & trackingUrl();
  const string & originalTrackingUrl();
  const string & diagnostics();
  int64 startTime();
  int64 finishTime();
  float progress();
  YarnApplicationState state();
  FinalApplicationStatus finalStatus();
  ApplicationResourceUsageReport * resourceUsage();
  const Token * clientAmToken();
  const Token * amRmToken();
};

/**
 * Yarn application
 */
class ApplicationImpl;
class Application {
private:
  friend class YarnClient;
  ApplicationImpl * _impl;
  Application(Application & rhs);
  Application(ApplicationImpl * impl);
public:
  ~Application();

  ApplicationImpl * impl() {
    return _impl;
  }

  // application submission context properties
  const ApplicationId & getId();

  const string & getName();
  void setName(const string & name);

  const string & getType();
  void setType(const string & type);

  const string & getQueue();
  void setQueue(const string & queue);

  int32 getPriority();
  void setPriority(int32 priority);

  ContainerLaunchContext & amContainer();
};

/**
 * Yarn client
 */
class YarnClientImpl;
class YarnClient {
private:
  YarnClientImpl * _impl;
  YarnClient(YarnClient & rhs);
public:
  YarnClient(const User & user, const string & addr);

  ~YarnClient();

  Application * create();

  void submit(Application & app);

  bool kill(Application & app);

  ApplicationReport * getReport(Application & app);
};

/**
 * ResourceRequest represents the request made by an
 * application to the ResourceManager to obtain various
 * Container allocations.
 */
class ResourceRequest {
public:
  int32 priority;
  string resourceName;
  Resource capability;
  int32 numContainers;
  bool relaxLocality;
};

/**
 * Detail of exception
 */
class SerializedException {
public:
  string message;
  string trace;
  string className;
  unique_ptr<SerializedException> cause;
};

}
}

#endif /* YARN_H_ */
