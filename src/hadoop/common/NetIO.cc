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

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <fcntl.h>
#include <algorithm>
#include "NetIO.h"

namespace hadoop {
namespace common {


SockAddr::SockAddr(const string & addr) {
  parse(addr);
}

void SockAddr::parse(const string & addr) {
  size_t pos = addr.rfind(':');
  if (pos == string::npos) {
    ip.clear();
    port = 0;
  }
  ip = addr.substr(0, pos);
  port = Strings::toInt(addr.substr(pos + 1));
}

string SockAddr::toString() const {
  if (port == 0 || ip.empty()) {
    return "";
  }
  else {
    return Strings::Format("%s:%d", ip.c_str(), port);
  }
}

int NetUtil::server(int port) {
  SockAddr addr("0.0.0.0", port);
  int ret = server(addr);
  if (port == 0) {
    port = addr.port;
  }
  return ret;
}

int NetUtil::server(SockAddr & local) {
  int ret = 0;
  if ((ret = ::socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    THROW_EXCEPTION_EX(IOException, "creating socket error: %s",
                       strerror(errno));
  }
  struct sockaddr_in sa;
  memset(&sa, 0, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(local.port);
  sa.sin_addr.s_addr = htonl(INADDR_ANY);
  if (!local.ip.empty()) {
    if (inet_aton(local.ip.c_str(), &sa.sin_addr) == 0) {
      close(ret);
      THROW_EXCEPTION_EX(IOException, "inet_aton(%s) error", local.ip.c_str());
    }
  }
  if (::bind(ret, (sockaddr*) &sa, sizeof(sa)) == -1) {
    close(ret);
    THROW_EXCEPTION_EX(IOException, "bind(%s) to socket %d failed",
                       local.toString().c_str(), ret);
  }
  if (::listen(ret, 511) == -1) {
    close(ret);
    THROW_EXCEPTION_EX(IOException, "listen(%d, 511) error: %s",
                       ret, strerror(errno));
  }
  if (sa.sin_port == 0) {
    // get real port
    socklen_t slen = sizeof(sa);
    ::getsockname(ret, (sockaddr*)&sa, &slen);
    local.port = ntohs(sa.sin_port);
  }
  return ret;
}

int NetUtil::accept(int sock, SockAddr & remote) {
  struct sockaddr_in sa;
  memset(&sa, 0, sizeof(sa));
  socklen_t salen = sizeof(sa);
  int ret = 0;
  while (true) {
    ret = ::accept(sock, (sockaddr*) &sa, &salen);
    if (ret == -1) {
      if (errno == EINTR) {
        continue;
      }
      else {
        THROW_EXCEPTION_EX(IOException, "accept error: %s", strerror(errno));
      }
    } else {
      break;
    }
  }
  remote.ip = inet_ntoa(sa.sin_addr);
  remote.port = ntohs(sa.sin_port);
  return ret;
}

int NetUtil::connect(const SockAddr & remote, bool nonBlock) {
  SockAddr local;
  return connect(remote, local, nonBlock);
}

int NetUtil::connect(const SockAddr & remote, SockAddr & local, bool nonBlock) {
  int ret = 0;
  // create socket
  if ((ret = ::socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    THROW_EXCEPTION_EX(IOException, "creating socket error: %s",
        strerror(errno));
  }
  // set nonblocking
  if (nonBlock) {
    if (!NetUtil::nonBlock(ret)) {
      close(ret);
      THROW_EXCEPTION_EX(IOException,
          "can't use nonblocking connect for socket %d, close", ret);
    }
  }
  // get remote sockaddr
  struct sockaddr_in sa;
  memset(&sa, 0, sizeof(sa));
  sa.sin_family = AF_INET;
  sa.sin_port = htons(remote.port);
  if (inet_aton(remote.ip.c_str(), &sa.sin_addr) == 0) {
    struct hostent *he;
    he = ::gethostbyname(remote.ip.c_str());
    if (he == nullptr) {
      close(ret);
      THROW_EXCEPTION_EX(IOException, "can't resolve: %s", remote.ip.c_str());
    }
    memcpy(&sa.sin_addr, he->h_addr, sizeof(struct in_addr));
  }
  // connect
  if (::connect(ret, (struct sockaddr*) &sa, sizeof(sa)) == -1) {
    if ((errno == EINPROGRESS) && nonBlock) {
      return ret;
    }
    close(ret);
    THROW_EXCEPTION_EX(IOException, "connect to %s error: %s",
                       remote.toString().c_str(), strerror(errno));
  }
  struct sockaddr_in saLocal;
  memset(&saLocal, 0, sizeof(saLocal));
  socklen_t saLocalLen = sizeof(saLocal);
  if (::getsockname(ret, (sockaddr*)&saLocal, &saLocalLen) == 0) {
    local.ip = inet_ntoa(saLocal.sin_addr);
    local.port = ntohs(saLocal.sin_port);
  }
  else {
    // already connected, ignore error
    LOG("Get socket remote addr failed after connect to %s",
        remote.toString().c_str());
    local.ip = "127.0.0.1";
    local.port = 0;
  }
  return ret;
}

bool NetUtil::nonBlock(int fd, bool nonBlock) {
  int flags;
  if ((flags = fcntl(fd, F_GETFL)) == -1) {
    LOG("Error fcntl(F_GETFL): %s", strerror(errno));
    return false;
  }
  if (nonBlock) {
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      LOG("Error set O_NONBLOCK: %s", strerror(errno));
      return false;
    }
  } else {
    if (fcntl(fd, F_SETFL, flags & (~O_NONBLOCK)) == -1) {
      LOG("Error unset O_NONBLOCK: %s", strerror(errno));
      return false;
    }
  }
  return true;
}

void NetUtil::tcpNoDelay(int fd, bool noDelay) {
  int op = noDelay ? 1 : 0;
  setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &op, sizeof(op));
}

void NetUtil::tcpKeepAlive(int fd, bool keepAlive) {
  int op = keepAlive ? 1 : 0;
  setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &op, sizeof(op));
}

void NetUtil::tcpSendBuffer(int fd, uint32 size) {
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
}

void NetUtil::tcpRecvBuffer(int fd, uint32 size) {
  setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, sizeof(size));
}

bool NetUtil::resolve(const string & host, string & ip) {
  struct sockaddr_in sa;
  memset(&sa, 0, sizeof(sa));
  if (inet_aton(host.c_str(), &sa.sin_addr) == 0) {
    struct hostent *he;
    he = ::gethostbyname(host.c_str());
    if (he == nullptr) {
      return false;
    }
    memcpy(&sa.sin_addr, he->h_addr, sizeof(struct in_addr));
    ip = inet_ntoa(sa.sin_addr);
  }
  else {
    ip = host;
  }
  return true;
}


string EventToString(int16 event) {
  string ret;
  if (event & EVENT_READ) {
    ret.append("read");
  }
  if (event & EVENT_WRITE) {
    if (!ret.empty()) {
      ret.append(",");
    }
    ret.append("write");
  }
  if (event & EVENT_CLOSE) {
    if (!ret.empty()) {
      ret.append(",");
    }
    ret.append("close");
  }
  if (event & EVENT_ERROR) {
    if (!ret.empty()) {
      ret.append(",");
    }
    ret.append("error");
  }
  return ret;
}


#ifdef __MACH__ // macosx using poll


Poller::Poller(int fd) {
  if (pipe(_wakefd) != 0) {
    _wakefd[0] = -1;
    _wakefd[1] = -1;
    throw std::runtime_error(strerror(errno));
  }
  NetUtil::nonBlock(_wakefd[0]);
  NetUtil::nonBlock(_wakefd[1]);
  _polls[0].fd = _wakefd[0];
  _polls[0].events = EVENT_READ;
  _polls[0].revents = 0;
  _polls[1].fd = fd;
  _polls[1].events = 0;
  _polls[1].revents = 0;
}

Poller::~Poller() {
  if (_wakefd[0] != 0) {
    close(_wakefd[0]);
  }
  if (_wakefd[1] != 0) {
    close(_wakefd[1]);
  }
}

void Poller::reset(int fd) {
  LOG_DEBUG("Poller::reset(orig=%d, new=%d) ", _polls[1].fd, fd);
  _polls[1].fd = fd;
  _polls[1].events = 0;
  _polls[1].revents = 0;
}

int Poller::getFd() {
  return _polls[1].fd;
}

int16 Poller::get() {
  return _polls[1].events;
}

void Poller::update(int16 event) {
  _polls[1].events = event;
}

void Poller::add(int16 event) {
  _polls[1].events |= event;
}

void Poller::remove(int16 event) {
  _polls[1].events &= (~event);
}

int16 Poller::poll(int32 timeout) {
  //LOG_INFO("poll(fd=[%d, %d(%s)], timeout=%d)", _polls[0].fd,
  //    _polls[1].fd, EventToString(_polls[1].events).c_str(), timeout);
  int ret = ::poll(_polls, _polls[1].fd >= 0 ? 2 : 1, timeout);
  //LOG_INFO("poll ret=%d", ret);
  if (ret < 0) {
    // error
    if (errno == EAGAIN || errno == EINTR) {
      return 0;
    } else {
      THROW_EXCEPTION_EX(IOException, "poll failed: %s", strerror(errno));
    }
  } else if (ret == 0) {
    // timeout
    return 0;
  }
  if (_polls[0].revents & EVENT_READ) {
    char buff[8];
    while (::read(_polls[0].fd, buff, 8) > 0);
  }
  return _polls[1].fd >= 0 ? (_polls[1].revents & _polls[1].events) : 0;
}

void Poller::wakeup() {
  int64_t event = 1;
  write(_wakefd[1], (void*)&event, sizeof(event));
}

#else // linux using epoll & eventfd

Poller::Poller() {

}

Poller::~Poller() {

}


#endif


class IOLoop {
  mutex _lock;
  Condition _fdChanged;
  volatile bool _errorOccurred;
  volatile bool _running;
  volatile int _streamerFd;
  volatile int _resetStreamerFd;
  Poller _poller;

  void resetFd(int fd, bool wait) {
    if (wait) {
      UniqueLock ul(_lock);
      if (_streamerFd != fd) {
        _resetStreamerFd = fd;
        _poller.wakeup();
        while (_streamerFd != fd && !_errorOccurred) {
          _fdChanged.wait(ul);
        }
      }
    } else {
      _resetStreamerFd = fd;
      _poller.wakeup();
    }
  }

  void loop() {
    _streamerFd = _poller.getFd();
    while (_running) {
      if ((_streamerFd == -1 && _resetStreamerFd >= 0 && !_errorOccurred) || (_streamerFd >= 0 && _resetStreamerFd <= 0)) {
        ScopeLock sl(_lock);
        if (_resetStreamerFd != _streamerFd) {
          _streamerFd = _resetStreamerFd;
          _poller.reset(_streamerFd);
          _fdChanged.notify_all();
        }
      }
      int16 events = _poller.poll(10000);
      if (events & (EVENT_ERROR | EVENT_CLOSE)) {
        _errorOccurred = true;
        _streamerFd = -1;
        _poller.reset(-1);
        continue;
      }
      // read
      // write
    }
  }
};

}
}
