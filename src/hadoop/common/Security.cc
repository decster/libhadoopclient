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

#include <unistd.h>
#include "Security.pb.h"
#include "Json.h"
#include "Security.h"
#include <sasl/sasl.h>
#include <sasl/saslutil.h>

namespace hadoop {
namespace common {

void Token::to(TokenProto & proto) const {
  proto.set_identifier(identifier);
  proto.set_password(password);
  proto.set_kind(kind);
  proto.set_service(service);
}

void Token::from(const TokenProto & proto) {
  identifier = proto.identifier();
  password = proto.password();
  kind = proto.kind();
  service = proto.service();
}

Json Token::toJson() const {
  Json ret = Json::Dict();
  ret.putString("identifier", identifier);
  ret.putString("password", password);
  ret.putString("kind", kind);
  ret.putString("service", service);
  return ret;
}

void Token::fromJson(Json & json) {
  identifier = json.getString("identifier");
  password = json.getString("password");
  kind = json.getString("kind");
  service = json.getString("service");
}



User::User(const User & from) {
  _name = from._name;
}

User::User(const string & name) : _name(name) {

}

const string & User::getName() const {
  return _name;
}

User::~User() {

}

const User & User::GetLoginUser() {
  static User loginUser(getlogin());
  return loginUser;
}

}
}
