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

#ifndef JSON_H_
#define JSON_H_

#include "json-c/json.h"
#include "Common.h"

namespace hadoop {
namespace common {

/**
 * Wrapper for json-c with extra utility method
 */
class Json {
private:
  json_object * _obj;
public:
  Json();
  Json(Json & json);
  Json(Json && json);
  Json(json_object * obj, bool copy=false);
  Json(const string & jsonstr);
  Json(const char * jsonstr);
  ~Json();
  void reset();
  json_type getType();
  json_object * getObject();
  bool isNull();
  // creation
  static Json Dict();
  static Json Array();
  // primitive operation
  bool getBool();
  int64 getInt();
  double getDouble();
  const char * getString();
  // dict operation
  Json & put(const char * key, Json & value);
  Json & putBool(const char * key, bool value);
  Json & putInt(const char * key, int64 value);
  Json & putDouble(const char * key, double value);
  Json & putString(const char * key, const char * value);
  Json & putString(const char * key, const string & value);
  Json get(const char * key);
  bool has(const char * key);
  bool getBool(const char * key);
  int64 getInt(const char * key);
  double getDouble(const char * key);
  const char * getString(const char * key);
  // array operation
  uint32 size();
  Json & append(Json & value);
  Json & appendBool(bool value);
  Json & appendInt(int64 value);
  Json & appendDouble(double value);
  Json & appendString(const char * value);
  Json & appendString(const string & value);
  Json arrayGet(uint32 idx);
  bool arrayGetBool(uint32 idx);
  int64 arrayGetInt(uint32 idx);
  double arrayGetDouble(uint32 idx);
  const char * arrayGetString(uint32 idx);
  // serialization
  Json & fromString(const string & str);
  Json & fromString(const char * str);
  void toString(string & str);
  const char * toString();
};

}
}

#endif /* JSON_H_ */
