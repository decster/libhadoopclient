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

#ifndef NETIO_H_
#define NETIO_H_

#include <sys/poll.h>
#include "Common.h"
#include "Buffer.h"

namespace hadoop {
namespace common {


enum SocketType {
  SOCKET_TCP = 0,
  SOCKET_UNIXDOMAIN = 1
};

/**
 * IPv4 socket address
 */
class SockAddr {
public:
  string ip;
  int port;

  SockAddr() : ip("127.0.0.1"), port(0) {}
  SockAddr(const string & ip, const int port) : ip(ip), port(port) {}
  SockAddr(const string & addr);
  SockAddr(int port) : ip("127.0.0.1"), port(port) {}
  void parse(const string & addr);
  string toString() const;

  SockAddr & operator=(const SockAddr & rhs) {
    ip = rhs.ip;
    port = rhs.port;
    return *this;
  }

  bool operator==(const SockAddr & rhs) const {
    return (ip == rhs.ip) && (port == rhs.port);
  }

  bool operator<(const SockAddr & rhs) const {
    if (ip == rhs.ip) {
      return port < rhs.port;
    } else {
      return ip < rhs.ip;
    }
  }
};

/**
 * Auto-close managed file descriptor
 */
class AutoFD {
public:
  int fd;
  AutoFD() : fd(-1) {}
  AutoFD(int fd): fd(fd) {}
  ~AutoFD() {
    if (fd >= 0) {
      ::close(fd);
    }
  }
  int swap(int newfd) {
    int ret = fd;
    fd = newfd;
    return ret;
  }
  operator int() {
    return fd;
  }
};

/**
 * A set of utility functions to deal with sockets, using raw FDs
 * is preferred, rather than wrap them to class.
 */
class NetUtil {
public:
  /**
   * Create a TCP server on the specified port
   * @return server socket FD
   */
  static int server(int port);

  /**
   * Create a TCP server on the specified address(both ip and port)
   * if local.ip equals 0, this method will choose a available port
   * automatically and assign back to local.ip
   * @return server socket FD
   */
  static int server(SockAddr & local);

  /**
   * Server side accept a connection
   * @param sock sever socket FD
   * @param remote get remote side socket address when return
   * @return connection socket FD
   */
  static int accept(int sock, SockAddr & remote);

  /**
   * Client side connect to server
   * TODO: support timeout
   * @param remote server address
   * @param nonBlock whether using nonBlocking connect
   * @return connection socket FD
   */
  static int connect(const SockAddr & remote, bool nonBlock = false);

  /**
   * Client side connect to server
   * @param remote server address
   * @param local get local side socket address when return
   * @nonBlock whether using nonBlocking connect
   * @return connection socket FD
   */
  static int connect(const SockAddr & remote, SockAddr & local, bool nonBlock = false);

  /**
   * Get IP from hostname
   * @return true if succeed
   */
  static bool resolve(const string & host, string & ip);

  // Set FD options
  static bool nonBlock(int fd, bool nonBlock=true);
  static void tcpNoDelay(int fd, bool noDelay);
  static void tcpKeepAlive(int fd, bool keepAlive);
  static void tcpSendBuffer(int fd, uint32 size);
  static void tcpRecvBuffer(int fd, uint32 size);
};

/**
 * IO events, borrow poll/epoll macros
 */
const int16 EVENT_READ = POLLIN | POLLPRI;
const int16 EVENT_WRITE = POLLOUT;
const int16 EVENT_CLOSE = POLLHUP;
const int16 EVENT_ERROR = POLLERR;
const int16 EVENT_ALL = EVENT_READ | EVENT_WRITE | EVENT_CLOSE | EVENT_ERROR;

string EventToString(int16 event);



//#ifdef __MACH__
#define _HADOOP_USE_POLL_
//#endif

/**
 * An IO multiplex poller, using poll and pipe on macosx,
 * using epoll and eventfd on linux
 */
class Poller {
public:
  explicit Poller(int fd = -1);
  ~Poller();
  void reset(int fd);
  int getFd();
  int16 get();
  void update(int16 event);
  void add(int16 event);
  void remove(int16 event);
  int16 poll(int32 timeoutMs);
  void wakeup();
private:
#ifdef _HADOOP_USE_POLL_ // macosx using poll
  pollfd _polls[2];
  int _wakefd[2];
#else // linux using epoll

#endif
};

}
}

#endif /* NETIO_H_ */
