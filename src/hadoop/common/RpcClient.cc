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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <random>
#include "ProtobufRpcEngine.pb.h"
#include "IpcConnectionContext.pb.h"
#include "RpcHeader.pb.h"
#include "RpcClient.h"

namespace hadoop {
namespace common {

bool Call::finished() {
  ScopeLock ul(lock);
  return status > RPC_CALL_WAITING;
}

void Call::waitFinish() {
  UniqueLock ul(lock);
  while (status <= RPC_CALL_WAITING) {
    finish.wait(ul);
  }
}

void Call::changeStatus(Status status) {
  UniqueLock sl(lock);
  this->status = status;
  if (status >= RPC_CALL_SUCCESS) {
    this->finish.notify_all();
  }
}

const char * Call::statusToString(Status st) {
  switch (st) {
    case RPC_CALL_INIT:
      return "RPC_CALL_INIT";
    case RPC_CALL_WAITING:
      return "RPC_CALL_WAITING";
    case RPC_CALL_SUCCESS:
      return "RPC_CALL_SUCCESS";
    case RPC_CALL_REMOTE_ERROR:
      return "RPC_CALL_REMOTE_ERROR";
    case RPC_CALL_LOCAL_ERROR:
      return "RPC_CALL_LOCAL_ERROR";
    default:
      return "UNKOWN";
  }
}

RpcClient::RpcClient(const string & protocol, uint64 version,
    const User & user, const string & addr, uint64 pingIntervalMs,
    uint64 maxIdleMs, uint32 maxRetry) :
    _protocol(protocol),
    _version(version),
    _user(user),
    _addr(addr),
    _authMethod(AUTH_SIMPLE),
    _pingIntervalMs(pingIntervalMs),
    _maxIdleMs(maxIdleMs),
    _currentRetry(0),
    _maxRetry(maxRetry),
    _running(false),
    _readBuffer(1024),
    _writeBuffer(1024),
    _handler(nullptr),
    _callIdCounter(0),
    _connectionContext(new IpcConnectionContextProto()),
    _rpcRequestHeader(new RpcRequestHeaderProto()),
    _rpcResponseHeader(new RpcResponseHeaderProto()),
    _requestHeader(new RequestHeaderProto()) {
  Random::NextUUID(_clientId);
  setupConnectionHeader();
  startClient();
}

RpcClient::~RpcClient() {
  stopClient();
}

void RpcClient::setUser(const User & user) {
  _user = user;
}

void RpcClient::setAddr(const string & addr) {
  _addr.parse(addr);
}

void RpcClient::startClient() {
  ScopeLock guard(_lock);
  if (_running == false) {
    _running = true;
    _clientThread = thread(&RpcClient::clientLoop, this);
  }
}

void RpcClient::stopClient() {
  lock_guard<mutex> guard(_lock);
  if (_running == true) {
    _running = false;
    _poller.wakeup();
    _clientThread.join();
  }
}

void RpcClient::clientLoop() {
  int16 events = 0;
  int fd = -1;
  bool connecting = false;
  while (_running) {
    try {
      //LOG_INFO("events: %s", EventToString(events).c_str());
      if (fd == -1) {
        // closed, wait or reconnect if new call comes
        _nextTimeout = Time::CurrentMonoMill() + 20000;
        if (!_callQueue.empty()) {
          fd = NetUtil::connect(_addr, true);
          connecting = true;
          setupConnection(fd);
        }
      } else {
        if (events & (EVENT_CLOSE | EVENT_ERROR)) {
          close(fd);
          _poller.reset(-1);
          fd = -1;
          events = 0;
          if (connecting) {
            // connection failed
            LOG_WARN("connect to %s failed, stop RpcClient", _addr.toString().c_str());
            break;
          }
          if (events & EVENT_ERROR) {
            LOG_WARN("rpc socket io error, stop RpcClient");
            break;
          }
          continue;
        }
        // connecting/connected, do framing decoding and writing
        if (events & EVENT_READ) {
          _readBuffer.read(fd);
          if (_readBuffer.pos == 4 && _readBuffer.limit == 4) {
            // read frame length complete, start to read frame
            _readBuffer.pos = 0;
            uint32 total = _readBuffer.readUInt32Be();
            //LOG_INFO("Got new frame, length=%u", total);
            _readBuffer.expandLimit(total);
            if (total > 0) {
              _readBuffer.read(fd);
            }
          }
        }
        if (events & EVENT_WRITE) {
          if (connecting) {
            connecting = false;
          }
          _writeBuffer.write(fd);
        }
        (this->*_handler)(_readBuffer, _writeBuffer);
      }
      if (_writeBuffer.remain() > 0) {
        _poller.update(EVENT_READ | EVENT_WRITE | EVENT_CLOSE | EVENT_ERROR);
      } else {
        _poller.update(EVENT_READ | EVENT_CLOSE | EVENT_ERROR);
      }
      int64 timeout = _nextTimeout - Time::CurrentMonoMill();
      events = _poller.poll(timeout > 0 ? timeout : 0);
    } catch (Exception & ex) {
      ex.logWarn();
      if (fd >= 0) {
        close(fd);
        _poller.reset(-1);
        fd = -1;
      }
      clearWaitingCalls();
    } catch (...) {
      LOG_WARN("Unknown error thrown in rpc client thread, exit");
      break;
    }
  }
  clearWaitingCalls();
  clearCallQueue();
}

void RpcClient::setupConnection(int fd) {
  setupProtos();
  NetUtil::tcpNoDelay(fd, true);
  NetUtil::tcpKeepAlive(fd, true);
  // setup _poller
  _poller.reset(fd);
  // prepare to write connection header
  _writeBuffer.reset(7);
  memcpy(_writeBuffer.data, &_connectionHeader, 7);
  // prepare to read frame
  _readBuffer.reset(4);
  _connectionContextSent = false;
  if (_authMethod == AUTH_SIMPLE) {
    _handler = &RpcClient::rpcHandler;
  } else {
    _handler = &RpcClient::authHandler;
  }
}


void RpcClient::authHandler(Buffer & rb, Buffer & wb) {
}

void RpcClient::rpcHandler(Buffer & rb, Buffer & wb) {
  if (wb.remain() == 0) { // can send new frame
    if (!_connectionContextSent) {
      sendConnectionContext(wb);
    } else {
      sendRpcRequest(wb);
    }
  }
  if (rb.remain() == 0) { // current frame fully read
    handleRpcResponse(rb);
  }
}

void RpcClient::sendConnectionContext(Buffer & wb) {
  RpcRequestHeaderProto * header = _rpcRequestHeader.get();
//  header->set_rpckind(RPC_PROTOCOL_BUFFER);
//  header->set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
//  header->set_clientid(_clientId);
  header->set_callid(CONNECTION_CONTEXT_CALL_ID);
  header->set_retrycount(INVALID_RETRY_COUNT);
  pack(wb, _rpcRequestHeader.get(), _connectionContext.get());
  _connectionContextSent = true;
}

void RpcClient::sendRpcRequest(Buffer & wb) {
  Call * pc = nullptr;
  _callLock.lock();
  if (!_callQueue.empty()) {
    pc = _callQueue.front();
    _waitingCalls.insert(std::make_pair(pc->id, pc));
    _callQueue.pop_front();
  }
  _callLock.unlock();
  if (!pc) {
    //LOG_DEBUG("nothing to send");
    return;
  }
  RpcRequestHeaderProto * header = _rpcRequestHeader.get();
//  header->set_rpckind(RPC_PROTOCOL_BUFFER);
//  header->set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
//  header->set_clientid(_clientId);
  header->set_callid(pc->id);
  header->set_retrycount(pc->retry);
  RequestHeaderProto * rheader = _requestHeader.get();
  rheader->set_methodname(pc->methodName);
  rheader->set_declaringclassprotocolname(pc->protocol.empty() ? _protocol : pc->protocol);
  rheader->set_clientprotocolversion(_version);
  pack(wb, header, rheader, &pc->request);
}

void RpcClient::handleRpcResponse(Buffer & rb) {
  rb.pos = 4;
  RpcResponseHeaderProto & header = *_rpcResponseHeader.get();
  rb.readVarInt32Prefixed(header);
  int32 callId = header.callid();
  Call * pc = nullptr;
  if (callId >= 0) {
    _callLock.lock();
    auto itr = _waitingCalls.find(callId);
    if (itr != _waitingCalls.end()) {
      pc = itr->second;
      _waitingCalls.erase(itr);
    }
    _callLock.unlock();
    if (!pc) {
      THROW_EXCEPTION_EX(IOException,
          "Logic error, cannot find RPC call(id=%d) in _waitingCalls", callId);
    }
  }
  switch (header.status()) {
  case RpcResponseHeaderProto::SUCCESS:
    if (!pc) {
      THROW_EXCEPTION_EX(IOException,
          "Illegal RPC Response, cannot find RPC call(id=%d)", callId);
    }
    rb.readVarInt32Prefixed(pc->response);
    pc->changeStatus(Call::RPC_CALL_SUCCESS);
    break;
  case RpcResponseHeaderProto::ERROR:
    if (!pc) {
      THROW_EXCEPTION_EX(IOException,
          "Illegal RPC Response, cannot find RPC call(id=%d)", callId);
    }
    LOG_WARN("Get error from RPC response: %s",
        RpcResponseHeaderProto_RpcErrorCodeProto_Name(
            header.errordetail()).c_str());
    if (header.has_exceptionclassname()) {
      pc->exceptionType = header.exceptionclassname();
    }
    if (header.has_errordetail()) {
      pc->exceptionTrace = header.errormsg();
    }
    pc->changeStatus(Call::RPC_CALL_REMOTE_ERROR);
    break;
  case RpcResponseHeaderProto::FATAL:
    THROW_EXCEPTION_EX(IOException,
        "Get fatal error from RPC response: %s, close connection",
            RpcResponseHeaderProto_RpcErrorCodeProto_Name(
                header.errordetail()).c_str());
  default:
    THROW_EXCEPTION_EX(IOException,
        "Unknown RPC status(%d) in rpc response",
            header.status());
  }
  // prepare to read next frame
  rb.reset(4);
}


void RpcClient::call(const string & methodName, MessageLite & request,
    MessageLite & response, int retry) {
  Call pcall(methodName, request, response, retry);
  asyncCall(pcall);
  pcall.waitFinish();
  LOG_DEBUG("Call %s(id=%d) finished, status=%s", methodName.c_str(), pcall.id,
      Call::statusToString(pcall.status));
  switch (pcall.status) {
  case Call::RPC_CALL_SUCCESS:
    break;
  case Call::RPC_CALL_REMOTE_ERROR:
    LOG_WARN("RpcCall %s get remote exception: %s\n%s\n",
        methodName.c_str(),
        pcall.exceptionType.c_str(),
        pcall.exceptionTrace.c_str());
    throw Exception(Exception::JavaToNative(pcall.exceptionType), AT,
        pcall.exceptionTrace.c_str());
    break;
  case Call::RPC_CALL_LOCAL_ERROR:
    LOG_WARN("RpcCall %s get local exception", methodName.c_str());
    THROW_EXCEPTION_EX(IOException, "RpcCall %s local error", methodName.c_str());
    break;
  default:
    THROW_EXCEPTION(IOException, "should never got here");
    break;
  }
}

void RpcClient::asyncCall(Call & pcall) {
  ScopeLock slock(_callLock);
  if (!_running) {
    THROW_EXCEPTION_EX(IOException, "Call %s failed, RpcClient already stopped",
        pcall.methodName.c_str());
  }
  bool wasEmpty = _callQueue.empty();
  pcall.id = _callIdCounter++;
  pcall.status = Call::RPC_CALL_WAITING;
  LOG_DEBUG("Call %s(id=%d, retry=%d)", pcall.methodName.c_str(), pcall.id,
      pcall.retry);
  _callQueue.push_back(&pcall);
  if (wasEmpty) {
    _poller.wakeup();
  }
}

void RpcClient::clearCallQueue() {
  ScopeLock sl(_callLock);
  if (_callQueue.size() > 0) {
    LOG_DEBUG("clear callQueue, size=%llu", _callQueue.size());
    for (auto pcall : _callQueue) {
      LOG_WARN("actively cancel pending RPC(%s)", pcall->methodName.c_str());
      pcall->changeStatus(Call::RPC_CALL_LOCAL_ERROR);
    }
    _callQueue.clear();
  }
}

void RpcClient::clearWaitingCalls() {
  ScopeLock sl(_callLock);
  if (_waitingCalls.size() > 0) {
    LOG_DEBUG("clear _waitingCalls, size=%llu", _waitingCalls.size());
    for (auto itr : _waitingCalls) {
      Call * pcall = itr.second;
      LOG_WARN("actively cancel sent RPC(%s)", pcall->methodName.c_str());
      pcall->changeStatus(Call::RPC_CALL_LOCAL_ERROR);
    }
    _waitingCalls.clear();
  }
}

void RpcClient::setupProtos() {
  _connectionContext->set_protocol(_protocol);
  UserInformationProto * userInfo = _connectionContext->mutable_userinfo();
  if (_authMethod == AUTH_SIMPLE) {
    userInfo->set_effectiveuser(_user.getName());
    userInfo->clear_realuser();
  } else {
    THROW_EXCEPTION(UnsupportException, "hadoop security not supported");
  }
  _rpcRequestHeader->set_rpckind(RPC_PROTOCOL_BUFFER);
  _rpcRequestHeader->set_rpcop(RpcRequestHeaderProto::RPC_FINAL_PACKET);
  _rpcRequestHeader->set_clientid(_clientId);
  _requestHeader->set_declaringclassprotocolname(_protocol);
  _requestHeader->set_clientprotocolversion(_version);
}

void RpcClient::setupConnectionHeader() {
  _connectionHeader.hrpc[0] = 'h';
  _connectionHeader.hrpc[1] = 'r';
  _connectionHeader.hrpc[2] = 'p';
  _connectionHeader.hrpc[3] = 'c';
  _connectionHeader.version = RPC_VERSION;
  _connectionHeader.serviceClass = RPC_SERVICE_CLASS_DEFAULT;
  _connectionHeader.authProtocol = AUTHPROTOCOL_NONE;
}

void RpcClient::pack(Buffer & buff, MessageLite * header, MessageLite * msg, MessageLite * msg2) {
  uint64 maxlen = 4 + 5 + header->ByteSize() + 5 + msg->ByteSize();
  if (msg2 != nullptr) {
    maxlen += 5 + msg2->ByteSize();
  }
  buff.reset(maxlen);
  buff.pos = 4; // skip total length field, write it later
  buff.writeVarInt32Prefixed(*header);
  buff.writeVarInt32Prefixed(*msg);
  if (msg2 != nullptr) {
    buff.writeVarInt32Prefixed(*msg2);
  }
  uint32 total = buff.pos - 4;
  *(uint32*)(buff.data) = HToBe(total);
  buff.rewind();
}

}
}
