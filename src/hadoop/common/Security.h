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

#ifndef SECURITY_H_
#define SECURITY_H_

#include "Common.h"

namespace hadoop {
namespace common {

class Json;
class TokenProto;

class Token {
public:
  string identifier;
  string password;
  string kind;
  string service;
  void to(TokenProto & proto) const;
  void from(const TokenProto & proto);
  Json toJson() const;
  void fromJson(Json & json);
};



class User {
protected:
  string _name;
public:
  User(const User & from);
  User(const string & name);
  ~User();
  const string & getName() const;

  static const User & GetLoginUser();
};

}
}

#endif /* SECURITY_H_ */
