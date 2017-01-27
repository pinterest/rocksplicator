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


#pragma once

#include <ostream>
#include <string>

#include <thrift/lib/cpp/protocol/TProtocol.h>
#include <thrift/lib/cpp2/protocol/Protocol.h>

using apache::thrift::MessageType;
using apache::thrift::protocol::TType;

// TODO (bol): escape non-printable chars
DECLARE_bool(print_binary);

namespace tgrep {

const char* msgTypeName(const MessageType type);

const char* dataTypeName(const TType type);

bool asciiString(const std::string& str);

template <class Protocol_>
void print(std::ostream& os,
           Protocol_& prot,
           TType arg_type,
           const std::string& start = "",
           const std::string& end = "\n") {
  switch (arg_type) {
    case TType::T_BOOL:
    {
      bool boolv;
      prot.readBool(boolv);
      os << start << boolv << end;
      break;
    }
    case TType::T_BYTE:
    {
      int8_t bytev;
      prot.readByte(bytev);
      os << start << bytev << end;
      break;
    }
    case TType::T_I16:
    {
      int16_t i16;
      prot.readI16(i16);
      os << start << i16 << end;
      break;
    }
    case TType::T_I32:
    {
      int32_t i32;
      prot.readI32(i32);
      os << start << i32 << end;
      break;
    }
    case TType::T_I64:
    {
      int64_t i64;
      prot.readI64(i64);
      os << start << i64 << end;
      break;
    }
    case TType::T_DOUBLE:
    {
      double dub;
      prot.readDouble(dub);
      os << start << dub << end;
      break;
    }
    case TType::T_FLOAT:
    {
      float flt;
      prot.readFloat(flt);
      os << start << flt << end;
      break;
    }
    case TType::T_STRING:
    {
      std::string str;
      prot.readBinary(str);
      if (FLAGS_print_binary || asciiString(str)) {
        os << start << str << end;
      } else {
        os << start << "<Binary Data> of " << str.size() << " bytes" << end;
      }
      break;
    }
    case TType::T_STRUCT:
    {
      std::string name;
      int16_t fid;
      TType ftype;
      prot.readStructBegin(name);
      os << start << "{" << std::endl;
      std::string indent = start + "\t";
      while (true) {
        prot.readFieldBegin(name, ftype, fid);
        if (ftype == TType::T_STOP) {
          break;
        }
        os << indent << fid << ": " << dataTypeName(ftype) << std::endl;
        print(os, prot, ftype, indent);
        prot.readFieldEnd();
      }
      os << start << "}" << end;
      prot.readStructEnd();
      break;
    }
    case TType::T_MAP:
    {
      TType keyType;
      TType valType;
      uint32_t size;
      prot.readMapBegin(keyType, valType, size);
      std::string indent = start + "\t";
      os << start << "{" << std::endl;
      for (uint32_t i = 0; i < size; ++i) {
        print(os, prot, keyType, indent, " ->\n");
        print(os, prot, valType, indent, ",\n");
      }
      os << start << "}" << end;
      prot.readMapEnd();
      break;
    }
    case TType::T_SET:
    {
      TType elemType;
      uint32_t i, size;
      prot.readSetBegin(elemType, size);
      std::string indent = start + "\t";
      os << start << "{" << std::endl;
      for (i = 0; i < size; i++) {
        print(os, prot, elemType, indent, ",\n");
      }
      os << start << "}" << end;
      prot.readSetEnd();
      break;
    }
    case TType::T_LIST:
    {
      TType elemType;
      uint32_t i, size;
      prot.readListBegin(elemType, size);
      std::string indent = start + "\t";
      os << start << "[" << std::endl;
      for (i = 0; i < size; i++) {
        print(os, prot, elemType, indent, ",\n");
      }
      os << start << "]" << end;
      prot.readListEnd();
      break;
    }
    default:
      os << start << "Unknown type: " << arg_type << end;
  }
}

} // namespace tgrep
