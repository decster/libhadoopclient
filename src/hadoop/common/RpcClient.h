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

#ifndef RPCCLIENT_H_
#define RPCCLIENT_H_

#include <deque>
#include <unordered_map>
#include "common.h"
#include "NetIO.h"
#include "Security.h"
#include "Buffer.h"

namespace google {
namespace protobuf {
class Message;
}
}

namespace hadoop {
namespace common {

using ::google::protobuf::MessageLite;
class IpcConnectionContextProto;
class RpcRequestHeaderProto;
class RpcResponseHeaderProto;
class RequestHeaderProto;

class Call {
public:
  enum Status {
    RPC_CALL_INIT = 0,
    RPC_CALL_WAITING = 1,
    RPC_CALL_SUCCESS = 2,
    RPC_CALL_REMOTE_ERROR = 3,
    RPC_CALL_LOCAL_ERROR = 4,
  };

public:
  Status status;
  int32 id;
  int32 retry;
  string methodName;
  string protocol;
  MessageLite & request;
  MessageLite & response;
  string exceptionType;
  string exceptionTrace;
  mutex lock;
  Condition finish;

  Call(const string & methodName, MessageLite & request, MessageLite & response, int retry):
      status(RPC_CALL_INIT), id(0), retry(retry), methodName(methodName), request(request),
      response(response) {}
  bool finished();
  void waitFinish();
  void changeStatus(Status status);
  static const char * statusToString(Status st);
};

/**
 * RpcClient
 */
class RpcClient {
private:
  static const uint8 RPC_VERSION = 9;
  static const int32 RPC_SERVICE_CLASS_DEFAULT = 0;
  static const int32 AUTHORIZATION_FAILED_CALL_ID = -1;
  static const int32 INVALID_CALL_ID = -2;
  static const int32 CONNECTION_CONTEXT_CALL_ID = -3;
  static const int32 INVALID_RETRY_COUNT = -1;

  enum RPCAuthMethod {
    AUTH_SIMPLE = 80,
    AUTH_KERBEROS = 81,
    AUTH_DIGEST = 82,
  };

  enum RPCSerializationType {
    RPC_SERIALIZATION_PROTOBUF = 0,
  };

  enum AuthProtocol {
    AUTHPROTOCOL_NONE = 0,
    AUTHPROTOCOL_SASL = -33,
  };

  struct RPCConnectionHeader {
    char hrpc[4];
    uint8 version;
    uint8 serviceClass;
    uint8 authProtocol;
  };

private:
  string _clientId;
  string _protocol;
  uint64 _version;
  User _user;
  SockAddr _addr;
  RPCAuthMethod _authMethod;
  int64 _pingIntervalMs;
  int64 _maxIdleMs;
  uint32 _currentRetry;
  uint32 _maxRetry;

  mutex _lock;
  bool _running;
  Poller _poller;
  thread _clientThread;
  Buffer _readBuffer;
  Buffer _writeBuffer;
  // temporary states
  int64 _nextTimeout;
  bool _connectionContextSent;
  void (RpcClient::*_handler)(Buffer & rb, Buffer & wb);

  mutex _callLock;
  std::deque<Call*> _callQueue;
  std::unordered_map<int32, Call *> _waitingCalls;
  int _callIdCounter;

  RPCConnectionHeader _connectionHeader;
  // using pointers to hide protobuf header files
  unique_ptr<IpcConnectionContextProto> _connectionContext;
  unique_ptr<RpcRequestHeaderProto> _rpcRequestHeader;
  unique_ptr<RpcResponseHeaderProto> _rpcResponseHeader;
  unique_ptr<RequestHeaderProto> _requestHeader;

public:
  RpcClient(RpcClient &) = delete;
  /**
   * Constructor
   * @param protocol rpc protocol
   * @param version protocol version
   * @param user user for the rpc client authentication
   * @param addr server address
   * @param pingIntervalMs ping interval, set to 0 to disable ping
   *                       (not supported)
   * @param maxIdleMs client will temporary close connection if idle
   *                  for maxIdleMs(not supported)
   * @param maxRetry connection timeout retry(not supported)
   */
  RpcClient(const string & protocol, uint64 version, const User & user,
      const string & addr, uint64 pingIntervalMs = 60000,
      uint64 maxIdleMs = 10000, uint32 maxRetry = 45);

  ~RpcClient();

  /**
   * user can be changed if currently not connected
   * @param user new user
   */
  void setUser(const User & user);

  /**
   * server address can be changed if currently not connected
   * @param addr new address
   */
  void setAddr(const string & addr);

  /**
   * Invoke a protobuf based rpc call synchronously
   * internally this method uses asyncCall and wait the call to finish
   * @param methodName call method name
   * @param request protobuf request
   * @param response protobuf response
   * @param retry retry count, leaving it to default 0 disables retry
   *              (currently not supported)
   */
  void call(const string & methodName, MessageLite & request, MessageLite & response,
      int retry = 0);

  /**
   * Invoke a call asynchronously, caller has to check finish status manually after
   * @param pcall call object
   */
  void asyncCall(Call & pcall);

protected:
  void startClient();
  void stopClient();
  void clientLoop();
  void setupConnection(int fd);
  void authHandler(Buffer & rb, Buffer & wb);
  void rpcHandler(Buffer & rb, Buffer & wb);
  void sendConnectionContext(Buffer & wb);
  void sendRpcRequest(Buffer & wb);
  void handleRpcResponse(Buffer & rb);
  void clearCallQueue();
  void clearWaitingCalls();
  void setupProtos();
  void setupConnectionHeader();

  static void pack(Buffer & buff, MessageLite * header, MessageLite * msg,
      MessageLite * msg2 = NULL);
};

}
}

#endif /* RPCCLIENT_H_ */
