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
#include "hadoop/common/Common.h"

namespace hadoop {
namespace common {

TEST(Strings, Replace) {
  ASSERT_EQ(string("aa,,bb,,cc,,dd"),
      Strings::ReplaceAll("aa++bb++cc++dd", "++", ",,"));
  ASSERT_EQ(string("aa-bb-cc-dd"),
      Strings::ReplaceAll("aa++bb++cc++dd", "++", "-"));
  ASSERT_EQ(string("-bb-cc-"),
      Strings::ReplaceAll("++bb++cc++", "++", "-"));
}

TEST(Strings, Base64) {
  for (int i=0;i<2000;i++) {
    for (int j=0;j<5;j++) {
      string s;
      Random::NextBytes(i, s);
      string b64s = Strings::toBase64(s);
      string dec = Strings::fromBase64(b64s);
      ASSERT_EQ(s.length(), dec.length());
      ASSERT_EQ(s, dec);
    }
  }
}


}
}



