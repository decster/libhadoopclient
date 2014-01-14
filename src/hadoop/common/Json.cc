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

#include "Json.h"

namespace hadoop {
namespace common {

Json::Json() : _obj(nullptr) {
}

Json::Json(Json & json) {
  _obj = json_object_get(json._obj);
}

Json::Json(Json && json) {
  _obj = json._obj;
  json._obj = nullptr;
}

Json::Json(json_object * obj, bool copy) {
  if (copy) {
    _obj = json_object_get(obj);
  } else {
    _obj = obj;
  }
}

Json::Json(const string & jsonstr) : _obj(nullptr) {
  fromString(jsonstr);
}

Json::Json(const char * jsonstr) : _obj(nullptr) {
  fromString(jsonstr);
}

Json::~Json() {
  reset();
}

void Json::reset() {
  if (_obj) {
    json_object_put(_obj);
    _obj = nullptr;
  }
}

json_type Json::getType() {
  return json_object_get_type(_obj);
}

json_object * Json::getObject() {
  if (_obj) {
    return json_object_get(_obj);
  } else {
    return nullptr;
  }
}

bool Json::isNull() {
  return _obj == nullptr;
}

Json Json::Dict() {
  return Json(json_object_new_object());
}

Json Json::Array() {
  return Json(json_object_new_array());
}

bool Json::getBool() {
  if (json_object_is_type(_obj, json_type_boolean)) {
    return json_object_get_boolean(_obj);
  } else {
    THROW_EXCEPTION(IOException, "getBool failed, type mismatch");
  }
}

int64 Json::getInt() {
  if (json_object_is_type(_obj, json_type_int)) {
    return json_object_get_int64(_obj);
  } else {
    THROW_EXCEPTION(IOException, "getInt failed, type mismatch");
  }
}

double Json::getDouble() {
  if (json_object_is_type(_obj, json_type_double)) {
    return json_object_get_double(_obj);
  } else {
    THROW_EXCEPTION(IOException, "getDouble failed, type mismatch");
  }
}

const char* Json::getString() {
  if (json_object_is_type(_obj, json_type_string)) {
    return json_object_get_string(_obj);
  } else if (json_object_is_type(_obj, json_type_null)) {
    return nullptr;
  } else {
    THROW_EXCEPTION(IOException, "getString failed, type mismatch");
  }
}

Json & Json::put(const char * key, Json & value) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object_object_add(_obj, key, json_object_get(value._obj));
  } else {
    THROW_EXCEPTION(IOException, "put failed, not a json dict");
  }
  return *this;
}

Json & Json::putBool(const char* key, bool value) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object_object_add(_obj, key,
        json_object_new_boolean(value ? TRUE : FALSE));
  } else {
    THROW_EXCEPTION(IOException, "put failed, not a json dict");
  }
  return *this;
}

Json & Json::putInt(const char* key, int64 value) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object_object_add(_obj, key,
        json_object_new_int64(value));
  } else {
    THROW_EXCEPTION(IOException, "put failed, not a json dict");
  }
  return *this;
}

Json & Json::putDouble(const char* key, double value) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object_object_add(_obj, key,
        json_object_new_double(value));
  } else {
    THROW_EXCEPTION(IOException, "put failed, not a json dict");
  }
  return *this;
}

Json & Json::putString(const char* key, const char* value) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object_object_add(_obj, key,
        json_object_new_string(value));
  } else {
    THROW_EXCEPTION(IOException, "put failed, not a json dict");
  }
  return *this;
}

Json & Json::putString(const char * key, const string & value) {
  return putString(key, value.c_str());
}

Json Json::get(const char* key) {
  if (json_object_is_type(_obj, json_type_object)) {
    return Json(json_object_object_get(_obj, key), true);
  } else {
    THROW_EXCEPTION(IOException, "get failed, not a json dict");
  }
}

bool Json::has(const char * key) {
  if (json_object_is_type(_obj, json_type_object)) {
    json_object * v = json_object_object_get(_obj, key);
    return !json_object_is_type(v, json_type_null);
  } else {
    THROW_EXCEPTION(IOException, "has failed, not a json dict");
  }
}

bool Json::getBool(const char* key) {
  return get(key).getBool();
}

int64 Json::getInt(const char* key) {
  return get(key).getInt();
}

double Json::getDouble(const char* key) {
  return get(key).getDouble();
}

const char* Json::getString(const char* key) {
  return get(key).getString();
}

uint32 Json::size() {
  if (json_object_is_type(_obj, json_type_array)) {
    return json_object_array_length(_obj);
  } else {
    THROW_EXCEPTION(IOException, "get size failed, not a json array");
  }
}

Json & Json::append(Json & value) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object_array_add(_obj, json_object_get(value._obj));
  } else {
    THROW_EXCEPTION(IOException, "append failed, not a json array");
  }
  return *this;
}

Json & Json::appendBool(bool value) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object_array_add(_obj, json_object_new_boolean(value ? TRUE : FALSE));
  } else {
    THROW_EXCEPTION(IOException, "append failed, not a json array");
  }
  return *this;
}

Json & Json::appendInt(int64 value) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object_array_add(_obj, json_object_new_int64(value));
  } else {
    THROW_EXCEPTION(IOException, "append failed, not a json array");
  }
  return *this;
}

Json & Json::appendDouble(double value) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object_array_add(_obj, json_object_new_double(value));
  } else {
    THROW_EXCEPTION(IOException, "append failed, not a json array");
  }
  return *this;
}

Json & Json::appendString(const char* value) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object_array_add(_obj, json_object_new_string(value));
  } else {
    THROW_EXCEPTION(IOException, "append failed, not a json array");
  }
  return *this;
}

Json & Json::appendString(const string & value) {
  return appendString(value.c_str());
}

Json Json::arrayGet(uint32 idx) {
  if (json_object_is_type(_obj, json_type_array)) {
    json_object * value = json_object_array_get_idx(_obj, idx);
    return Json(value, true);
  } else {
    THROW_EXCEPTION(IOException, "get failed, not a json array");
  }
}

bool Json::arrayGetBool(uint32 idx) {
  return arrayGet(idx).getBool();
}

int64 Json::arrayGetInt(uint32 idx) {
  return arrayGet(idx).getInt();
}

double Json::arrayGetDouble(uint32 idx) {
  return arrayGet(idx).getDouble();
}

const char* Json::arrayGetString(uint32 idx) {
  return arrayGet(idx).getString();
}

Json & Json::fromString(const string& str) {
  return fromString(str.c_str());
}

Json & Json::fromString(const char * str) {
  reset();
  _obj = json_tokener_parse(str);
  if (!_obj) {
    THROW_EXCEPTION(IOException, "parse json failed");
  }
  return *this;
}

void Json::toString(string& str) {
  str = toString();
}

const char * Json::toString() {
  if (_obj) {
    return json_object_to_json_string(_obj);
  } else {
    return "null";
  }
}

}
}
