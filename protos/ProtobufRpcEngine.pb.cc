// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: ProtobufRpcEngine.proto

#define INTERNAL_SUPPRESS_PROTOBUF_FIELD_DEPRECATION
#include "ProtobufRpcEngine.pb.h"

#include <algorithm>

#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/once.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/wire_format_lite_inl.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/generated_message_reflection.h>
#include <google/protobuf/reflection_ops.h>
#include <google/protobuf/wire_format.h>
// @@protoc_insertion_point(includes)

namespace hadoop {
namespace common {

namespace {

const ::google::protobuf::Descriptor* RequestHeaderProto_descriptor_ = NULL;
const ::google::protobuf::internal::GeneratedMessageReflection*
  RequestHeaderProto_reflection_ = NULL;

}  // namespace


void protobuf_AssignDesc_ProtobufRpcEngine_2eproto() {
  protobuf_AddDesc_ProtobufRpcEngine_2eproto();
  const ::google::protobuf::FileDescriptor* file =
    ::google::protobuf::DescriptorPool::generated_pool()->FindFileByName(
      "ProtobufRpcEngine.proto");
  GOOGLE_CHECK(file != NULL);
  RequestHeaderProto_descriptor_ = file->message_type(0);
  static const int RequestHeaderProto_offsets_[3] = {
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(RequestHeaderProto, methodname_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(RequestHeaderProto, declaringclassprotocolname_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(RequestHeaderProto, clientprotocolversion_),
  };
  RequestHeaderProto_reflection_ =
    new ::google::protobuf::internal::GeneratedMessageReflection(
      RequestHeaderProto_descriptor_,
      RequestHeaderProto::default_instance_,
      RequestHeaderProto_offsets_,
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(RequestHeaderProto, _has_bits_[0]),
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(RequestHeaderProto, _unknown_fields_),
      -1,
      ::google::protobuf::DescriptorPool::generated_pool(),
      ::google::protobuf::MessageFactory::generated_factory(),
      sizeof(RequestHeaderProto));
}

namespace {

GOOGLE_PROTOBUF_DECLARE_ONCE(protobuf_AssignDescriptors_once_);
inline void protobuf_AssignDescriptorsOnce() {
  ::google::protobuf::GoogleOnceInit(&protobuf_AssignDescriptors_once_,
                 &protobuf_AssignDesc_ProtobufRpcEngine_2eproto);
}

void protobuf_RegisterTypes(const ::std::string&) {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedMessage(
    RequestHeaderProto_descriptor_, &RequestHeaderProto::default_instance());
}

}  // namespace

void protobuf_ShutdownFile_ProtobufRpcEngine_2eproto() {
  delete RequestHeaderProto::default_instance_;
  delete RequestHeaderProto_reflection_;
}

void protobuf_AddDesc_ProtobufRpcEngine_2eproto() {
  static bool already_here = false;
  if (already_here) return;
  already_here = true;
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  ::google::protobuf::DescriptorPool::InternalAddGeneratedFile(
    "\n\027ProtobufRpcEngine.proto\022\rhadoop.common"
    "\"k\n\022RequestHeaderProto\022\022\n\nmethodName\030\001 \002"
    "(\t\022\"\n\032declaringClassProtocolName\030\002 \002(\t\022\035"
    "\n\025clientProtocolVersion\030\003 \002(\004B<\n\036org.apa"
    "che.hadoop.ipc.protobufB\027ProtobufRpcEngi"
    "neProtos\240\001\001", 211);
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedFile(
    "ProtobufRpcEngine.proto", &protobuf_RegisterTypes);
  RequestHeaderProto::default_instance_ = new RequestHeaderProto();
  RequestHeaderProto::default_instance_->InitAsDefaultInstance();
  ::google::protobuf::internal::OnShutdown(&protobuf_ShutdownFile_ProtobufRpcEngine_2eproto);
}

// Force AddDescriptors() to be called at static initialization time.
struct StaticDescriptorInitializer_ProtobufRpcEngine_2eproto {
  StaticDescriptorInitializer_ProtobufRpcEngine_2eproto() {
    protobuf_AddDesc_ProtobufRpcEngine_2eproto();
  }
} static_descriptor_initializer_ProtobufRpcEngine_2eproto_;

// ===================================================================

#ifndef _MSC_VER
const int RequestHeaderProto::kMethodNameFieldNumber;
const int RequestHeaderProto::kDeclaringClassProtocolNameFieldNumber;
const int RequestHeaderProto::kClientProtocolVersionFieldNumber;
#endif  // !_MSC_VER

RequestHeaderProto::RequestHeaderProto()
  : ::google::protobuf::Message() {
  SharedCtor();
}

void RequestHeaderProto::InitAsDefaultInstance() {
}

RequestHeaderProto::RequestHeaderProto(const RequestHeaderProto& from)
  : ::google::protobuf::Message() {
  SharedCtor();
  MergeFrom(from);
}

void RequestHeaderProto::SharedCtor() {
  _cached_size_ = 0;
  methodname_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  declaringclassprotocolname_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  clientprotocolversion_ = GOOGLE_ULONGLONG(0);
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
}

RequestHeaderProto::~RequestHeaderProto() {
  SharedDtor();
}

void RequestHeaderProto::SharedDtor() {
  if (methodname_ != &::google::protobuf::internal::kEmptyString) {
    delete methodname_;
  }
  if (declaringclassprotocolname_ != &::google::protobuf::internal::kEmptyString) {
    delete declaringclassprotocolname_;
  }
  if (this != default_instance_) {
  }
}

void RequestHeaderProto::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* RequestHeaderProto::descriptor() {
  protobuf_AssignDescriptorsOnce();
  return RequestHeaderProto_descriptor_;
}

const RequestHeaderProto& RequestHeaderProto::default_instance() {
  if (default_instance_ == NULL) protobuf_AddDesc_ProtobufRpcEngine_2eproto();
  return *default_instance_;
}

RequestHeaderProto* RequestHeaderProto::default_instance_ = NULL;

RequestHeaderProto* RequestHeaderProto::New() const {
  return new RequestHeaderProto;
}

void RequestHeaderProto::Clear() {
  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (has_methodname()) {
      if (methodname_ != &::google::protobuf::internal::kEmptyString) {
        methodname_->clear();
      }
    }
    if (has_declaringclassprotocolname()) {
      if (declaringclassprotocolname_ != &::google::protobuf::internal::kEmptyString) {
        declaringclassprotocolname_->clear();
      }
    }
    clientprotocolversion_ = GOOGLE_ULONGLONG(0);
  }
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
  mutable_unknown_fields()->Clear();
}

bool RequestHeaderProto::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!(EXPRESSION)) return false
  ::google::protobuf::uint32 tag;
  while ((tag = input->ReadTag()) != 0) {
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // required string methodName = 1;
      case 1: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_methodname()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->methodname().data(), this->methodname().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(18)) goto parse_declaringClassProtocolName;
        break;
      }

      // required string declaringClassProtocolName = 2;
      case 2: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_declaringClassProtocolName:
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_declaringclassprotocolname()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->declaringclassprotocolname().data(), this->declaringclassprotocolname().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(24)) goto parse_clientProtocolVersion;
        break;
      }

      // required uint64 clientProtocolVersion = 3;
      case 3: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_VARINT) {
         parse_clientProtocolVersion:
          DO_((::google::protobuf::internal::WireFormatLite::ReadPrimitive<
                   ::google::protobuf::uint64, ::google::protobuf::internal::WireFormatLite::TYPE_UINT64>(
                 input, &clientprotocolversion_)));
          set_has_clientprotocolversion();
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectAtEnd()) return true;
        break;
      }

      default: {
      handle_uninterpreted:
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_END_GROUP) {
          return true;
        }
        DO_(::google::protobuf::internal::WireFormat::SkipField(
              input, tag, mutable_unknown_fields()));
        break;
      }
    }
  }
  return true;
#undef DO_
}

void RequestHeaderProto::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // required string methodName = 1;
  if (has_methodname()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->methodname().data(), this->methodname().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      1, this->methodname(), output);
  }

  // required string declaringClassProtocolName = 2;
  if (has_declaringclassprotocolname()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->declaringclassprotocolname().data(), this->declaringclassprotocolname().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      2, this->declaringclassprotocolname(), output);
  }

  // required uint64 clientProtocolVersion = 3;
  if (has_clientprotocolversion()) {
    ::google::protobuf::internal::WireFormatLite::WriteUInt64(3, this->clientprotocolversion(), output);
  }

  if (!unknown_fields().empty()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        unknown_fields(), output);
  }
}

::google::protobuf::uint8* RequestHeaderProto::SerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // required string methodName = 1;
  if (has_methodname()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->methodname().data(), this->methodname().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        1, this->methodname(), target);
  }

  // required string declaringClassProtocolName = 2;
  if (has_declaringclassprotocolname()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->declaringclassprotocolname().data(), this->declaringclassprotocolname().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        2, this->declaringclassprotocolname(), target);
  }

  // required uint64 clientProtocolVersion = 3;
  if (has_clientprotocolversion()) {
    target = ::google::protobuf::internal::WireFormatLite::WriteUInt64ToArray(3, this->clientprotocolversion(), target);
  }

  if (!unknown_fields().empty()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        unknown_fields(), target);
  }
  return target;
}

int RequestHeaderProto::ByteSize() const {
  int total_size = 0;

  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    // required string methodName = 1;
    if (has_methodname()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->methodname());
    }

    // required string declaringClassProtocolName = 2;
    if (has_declaringclassprotocolname()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->declaringclassprotocolname());
    }

    // required uint64 clientProtocolVersion = 3;
    if (has_clientprotocolversion()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::UInt64Size(
          this->clientprotocolversion());
    }

  }
  if (!unknown_fields().empty()) {
    total_size +=
      ::google::protobuf::internal::WireFormat::ComputeUnknownFieldsSize(
        unknown_fields());
  }
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = total_size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
  return total_size;
}

void RequestHeaderProto::MergeFrom(const ::google::protobuf::Message& from) {
  GOOGLE_CHECK_NE(&from, this);
  const RequestHeaderProto* source =
    ::google::protobuf::internal::dynamic_cast_if_available<const RequestHeaderProto*>(
      &from);
  if (source == NULL) {
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
    MergeFrom(*source);
  }
}

void RequestHeaderProto::MergeFrom(const RequestHeaderProto& from) {
  GOOGLE_CHECK_NE(&from, this);
  if (from._has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (from.has_methodname()) {
      set_methodname(from.methodname());
    }
    if (from.has_declaringclassprotocolname()) {
      set_declaringclassprotocolname(from.declaringclassprotocolname());
    }
    if (from.has_clientprotocolversion()) {
      set_clientprotocolversion(from.clientprotocolversion());
    }
  }
  mutable_unknown_fields()->MergeFrom(from.unknown_fields());
}

void RequestHeaderProto::CopyFrom(const ::google::protobuf::Message& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void RequestHeaderProto::CopyFrom(const RequestHeaderProto& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool RequestHeaderProto::IsInitialized() const {
  if ((_has_bits_[0] & 0x00000007) != 0x00000007) return false;

  return true;
}

void RequestHeaderProto::Swap(RequestHeaderProto* other) {
  if (other != this) {
    std::swap(methodname_, other->methodname_);
    std::swap(declaringclassprotocolname_, other->declaringclassprotocolname_);
    std::swap(clientprotocolversion_, other->clientprotocolversion_);
    std::swap(_has_bits_[0], other->_has_bits_[0]);
    _unknown_fields_.Swap(&other->_unknown_fields_);
    std::swap(_cached_size_, other->_cached_size_);
  }
}

::google::protobuf::Metadata RequestHeaderProto::GetMetadata() const {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::Metadata metadata;
  metadata.descriptor = RequestHeaderProto_descriptor_;
  metadata.reflection = RequestHeaderProto_reflection_;
  return metadata;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace common
}  // namespace hadoop

// @@protoc_insertion_point(global_scope)
