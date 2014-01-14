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

#include "Common.h"
#include "Stream.h"

namespace hadoop {
namespace common {

void InputStream::seek(uint64 position) {
  THROW_EXCEPTION(UnsupportException, "seek not support");
}

uint64 InputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

uint64 InputStream::getLength() {
  THROW_EXCEPTION(UnsupportException, "getLength not support");
}

int64 InputStream::read(void * buff, uint64 length) {
  // default empty stream
  return -1;
}


int64 InputStream::read(Buffer & buff) {
  int64 rd = read(buff.current(), buff.remain());
  if (rd > 0) {
    buff.pos += rd;
  }
  return rd;
}

int64 InputStream::readFully(void * buff, uint64 length) {
  int64 ret = 0;
  while (ret < length) {
    int64 rd = read((char*)buff+ret, length-ret);
    if (rd < 0) {
      return ret > 0 ? ret : -1;
    } else if (rd == 0) {
      return ret;
    } else {
      ret += rd;
    }
  }
  return ret;
}

void InputStream::close() {
}

/////////////////////////////////////////////////////////////

uint64 OutputStream::tell() {
  THROW_EXCEPTION(UnsupportException, "tell not support");
}

uint64 OutputStream::write(const void * buff, uint64 length) {
  // default act as /dev/null
  return length;
}

uint64 OutputStream::write(Buffer & buff) {
  uint64 wt = write(buff.current(), buff.remain());
  buff.pos += wt;
  return wt;
}

void OutputStream::flush() {
}

void OutputStream::sync(uint32 flag) {
  flush();
}

void OutputStream::close() {
}

/////////////////////////////////////////////////////////////

int64 FDInputStream::read(void * buff, uint64 length) {
  if (length == 0) {
    return 0;
  }
  int64 ret = (int64)::read(_fd, buff, length);
  if (ret > 0) {
    return ret;
  } else if (ret == 0) {
    return -1; // EOF
  } else {
    if (errno == EAGAIN) {
      return 0; // non-blocking
    }
    // error
    THROW_EXCEPTION_EX(IOException, "read(%d, %p, %u) failed: %s",
                       _fd, buff, length, strerror(errno));
  }
}

void FDInputStream::close() {
  if (_fd >= 0) {
    ::close(_fd);
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

uint64 FDOutputStream::write(const void * buff, uint64 length) {
  if (length==0) {
    return 0;
  }
  int64 ret = (int64)::write(_fd, buff, length);
  if (ret >= 0) {
    return ret;
  } else {
    if (errno == EAGAIN) {
      return 0;
    }
    THROW_EXCEPTION_EX(IOException, "write(%d, %p, %u) failed: %s", _fd, buff,
        length, strerror(errno));
  }
}

void FDOutputStream::sync(uint32 flag) {
  if (fsync(_fd) != 0) {
    THROW_EXCEPTION_EX(IOException, "fsync(%d) failed: %s", _fd,
        strerror(errno));
  }
}

void FDOutputStream::close() {
  if (_fd >= 0) {
    ::close(_fd);
    _fd = -1;
  }
}

/////////////////////////////////////////////////////////////

ChecksumInputStream::ChecksumInputStream(InputStream * stream,
                                         ChecksumType type) :
  FilterInputStream(stream),
  _type(type),
  _limit(-1) {
  resetChecksum();
}

void ChecksumInputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

uint32 ChecksumInputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

int64 ChecksumInputStream::read(void * buff, uint64 length) {
  if (_limit < 0) {
    int64 ret = _stream->read(buff, length);
    if (ret > 0) {
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  } else if (_limit == 0) {
    return -1;
  } else {
    int64 rd = _limit < length ? _limit : length;
    int64 ret = _stream->read(buff, rd);
    if (ret > 0) {
      _limit -= ret;
      Checksum::update(_type, _checksum, buff, ret);
    }
    return ret;
  }
}

///////////////////////////////////////////////////////////

ChecksumOutputStream::ChecksumOutputStream(OutputStream * stream,
    ChecksumType type) :
  FilterOutputStream(stream),
  _type(type) {
  resetChecksum();
}

void ChecksumOutputStream::resetChecksum() {
  _checksum = Checksum::init(_type);
}

uint32 ChecksumOutputStream::getChecksum() {
  return Checksum::getValue(_type, _checksum);
}

uint64 ChecksumOutputStream::write(const void * buff, uint64 length) {
  uint64 wt = _stream->write(buff, length);
  if (wt > 0) {
    Checksum::update(_type, _checksum, buff, wt);
  }
  return wt;
}

}
}
