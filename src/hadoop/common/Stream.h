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

#ifndef STREAMS_H_
#define STREAMS_H_

#include "Common.h"
#include "Checksum.h"
#include "Buffer.h"

namespace hadoop {
namespace common {

class OutputStream;

/**
 * InputStream abstraction for file, socket, string stream etc.
 * Support blocking and nonblocking semantics
 */
class InputStream {
public:
  InputStream() {}

  virtual ~InputStream() {}

  /**
   * Seek stream to position if possible for this kind of stream
   * or throw UnsupportException
   * @param position to seek to, offset to stream start
   */
  virtual void seek(uint64 position);

  /**
   * Get stream position if possible for this kind of stream
   * or throw UnsupportException
   * @return current stream position
   */
  virtual uint64 tell();

  /**
   * Get stream length if possible for this kind of stream
   * or throw UnsupportException
   * @return stream total length
   */
  virtual uint64 getLength();

  /**
   * Try read some number of bytes from this stream to buff,
   * read at most length bytes.
   * @param buff dest buff
   * @param length dest buff length
   * @return bytes read, -1 if reach EOF. if length is 0, or the
   *         underlying stream is non-blocking while no data
   *         available, this method may return 0.
   */
  virtual int64 read(void * buff, uint64 length);

  virtual int64 read(Buffer & buff);

  /**
   * Like read, but try read as much as possible to buff, until EOF
   * or no data temporary available in non-blocking mode
   * @param buff dest buff
   * @param length dest buff length
   * @return number of bytes read or -1, see method read
   */
  virtual int64 readFully(void * buff, uint64 length);

  /**
   *  Close this stream and release any resources managed by this
   *  stream
   */
  virtual void close();

};



/**
 * OutputStream abstraction for file, socket, string stream etc.
 * Support blocking and nonblocking semantics.
 * By default OutputStream accept all writes, just like
 * write to /dev/null
 */
class OutputStream {
public:
  OutputStream() {}

  virtual ~OutputStream() {}

  /**
   * Get stream position if possible for this kind of stream
   * or throw UnsupportException
   * @return current stream position
   */
  virtual uint64 tell();

  /**
   * Blocking write certain number of bytes
   * throws IOException is any error
   * @param buff source buff
   * @param length source buff length
   */
  virtual uint64 write(const void * buff, uint64 length);

  virtual uint64 write(Buffer & buff);

  /**
   * flush stream, blocking write buffer data if any
   */
  virtual void flush();

  virtual void sync(uint32 flag);

  virtual void close();

};

/**
 * InputStream for file descriptor, such as file or socket
 */
class FDInputStream : public InputStream {
protected:
  int _fd;
public:
  FDInputStream() : _fd(-1) {}

  FDInputStream(int fd) : _fd(fd) {}

  virtual ~FDInputStream() {}

  virtual int64 read(void * buff, uint64 length);

  virtual void close();

  int getFd() {
    return _fd;
  }
  void setFd(int fd) {
    _fd = fd;
  }
};



/**
 * OutputStream for file descriptor, such as file or socket
 */
class FDOutputStream : public OutputStream {
protected:
  int _fd;
public:
  FDOutputStream() : _fd(-1) {}

  FDOutputStream(int fd) : _fd(fd) {}

  virtual ~FDOutputStream() {}

  virtual uint64 write(const void * buff, uint64 length);

  virtual void sync(uint32 flag);

  virtual void close();

  int getFd() {
    return _fd;
  }
  void setFd(int fd) {
    _fd = fd;
  }
};



/**
 * A FilterInputStream contains some other InputStream, which it
 * uses as basic source of data
 */
class FilterInputStream : public InputStream {
protected:
  InputStream * _stream;
public:
  FilterInputStream(InputStream * stream) : _stream(stream) {}

  virtual ~FilterInputStream() {}

  void setStream(InputStream * stream) {
    _stream = stream;
  }

  InputStream * getStream() {
    return _stream;
  }

  virtual void seek(uint64 position) {
    _stream->seek(position);
  }

  virtual uint64 tell() {
    return _stream->tell();
  }

  virtual uint64 getLength() {
    return _stream->getLength();
  }

  virtual int64 read(void * buff, uint64 length) {
    return _stream->read(buff, length);
  }
};



/**
 * A FilterInputStream contains some other InputStream, which it
 * uses as basic source of data
 */
class FilterOutputStream : public OutputStream {
protected:
  OutputStream * _stream;
public:
  FilterOutputStream(OutputStream * stream) : _stream(stream) {}

  virtual ~FilterOutputStream() {}

  void setStream(OutputStream * stream) {
    _stream = stream;
  }

  OutputStream * getStream() {
    return _stream;
  }

  virtual uint64 tell() {
    return _stream->tell();
  }

  virtual uint64 write(const void * buff, uint64 length) {
    return _stream->write(buff, length);
  }

  virtual void flush() {
    _stream->flush();
  }

  virtual void sync(uint32 flag) {
    _stream->sync(flag);
  }

  virtual void close() {
    flush();
    _stream->close();
  }
};



class LimitInputStream : public FilterInputStream {
protected:
  int64 _limit;
public:
  LimitInputStream(InputStream * stream, int64 limit) :
      FilterInputStream(stream), _limit(limit) {
  }

  virtual ~LimitInputStream() {}

  int64 getLimit() {
    return _limit;
  }

  void setLimit(int64 limit) {
    _limit = limit;
  }

  virtual int64 read(void * buff, uint64 length) {
    if (_limit < 0) {
      return _stream->read(buff, length);
    } else if (_limit == 0) {
      return -1;
    } else {
      int64 rd = _limit < length ? _limit : length;
      int64 ret = _stream->read(buff, rd);
      if (ret > 0) {
        _limit -= ret;
      }
      return ret;
    }
  }
};



class ChecksumInputStream : public FilterInputStream {
protected:
  ChecksumType _type;
  uint32 _checksum;
  int64 _limit;
public:
  ChecksumInputStream(InputStream * stream, ChecksumType type);

  virtual ~ChecksumInputStream() {}

  int64 getLimit() {
    return _limit;
  }

  void setLimit(int64 limit) {
    _limit = limit;
  }

  void resetChecksum();

  uint32 getChecksum();

  virtual int64 read(void * buff, uint64 length);
};



class ChecksumOutputStream : public FilterOutputStream {
protected:
  ChecksumType _type;
  uint32 _checksum;
public:
  ChecksumOutputStream(OutputStream * stream, ChecksumType type);

  virtual ~ChecksumOutputStream() {}

  void resetChecksum();

  uint32 getChecksum();

  virtual uint64 write(const void * buff, uint64 length);
};

}
}

#endif /* STREAMS_H_ */
