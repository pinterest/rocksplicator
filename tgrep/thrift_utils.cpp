/// Copyright 2017 Pinterest Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
/// http://www.apache.org/licenses/LICENSE-2.0

/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

//
// @author bol (bol@pinterest.com)
//

#include "tgrep/thrift_utils.h"

DEFINE_bool(print_binary, false, "Print binary string");

namespace tgrep {

const char* msgTypeName(const MessageType type) {
  switch (type) {
  case MessageType::T_CALL:
    return "CALL";
  case MessageType::T_REPLY:
    return "REPLY";
  case MessageType::T_EXCEPTION:
    return "EXCEPTION";
  case MessageType::T_ONEWAY:
    return "ONEWAY";
  default:
    return "UNKNOWN";
  }
}

const char* dataTypeName(const TType type) {
  switch (type) {
  case TType::T_STOP:
    return "stop";
  case TType::T_VOID:
    return "void";
  case TType::T_BOOL:
    return "bool";
  case TType::T_BYTE:
    return "byte/i8";
  case TType::T_I16:
    return "i16";
  case TType::T_I32:
    return "i32";
  case TType::T_U64:
    return "u64";
  case TType::T_I64:
    return "i64";
  case TType::T_DOUBLE:
    return "double";
  case TType::T_STRING:
    return "string/utf7";
  case TType::T_STRUCT:
    return "struct";
  case TType::T_MAP:
    return "map";
  case TType::T_SET:
    return "set";
  case TType::T_LIST:
    return "list";
  case TType::T_UTF8:
    return "utf8";
  case TType::T_UTF16:
    return "utf16";
  case TType::T_STREAM:
    return "stream";
  case TType::T_FLOAT:
    return "float";
  default:
    return "unknown";
  }
}

bool asciiString(const std::string& str) {
  uint8_t b;
  for (const auto c : str) {
    b = c;
    if (b < 0x20 || b > 0x7f) {
      return false;
    }
  }
  return true;
}

} // namespace tgrep
