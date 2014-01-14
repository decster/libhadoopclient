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

#include "gtest/gtest.h"
#include "hadoop/common/Json.h"

namespace hadoop {
namespace common {

TEST(Json, construct) {
  Json a("{\"key\":111}");
  ASSERT_EQ(111, a.getInt("key"));
  Json b("{\"key\":null}");
  ASSERT_EQ(json_type_null, b.get("key").getType());
  Json c(a);
  ASSERT_EQ(111, c.getInt("key"));
  Json d(std::move(a));
  ASSERT_EQ(111, d.getInt("key"));
  ASSERT_EQ(json_type_null, a.getType());
}

TEST(Json, dict) {
  Json a = Json::Dict();
  a.putInt("key1", 1).putBool("key2", true).putDouble("key3", 1.1);
  ASSERT_EQ(1, a.getInt("key1"));
  ASSERT_EQ(true, a.getBool("key2"));
  ASSERT_EQ(1.1, a.getDouble("key3"));
}

TEST(Json, array) {
  Json a = Json::Array();
  a.appendInt(1).appendBool(true).appendDouble(1.1);
  ASSERT_EQ(1, a.arrayGetInt(0U));
  ASSERT_EQ(true, a.arrayGetBool(1));
  ASSERT_EQ(1.1, a.arrayGetDouble(2));
}

}
}


