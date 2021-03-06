// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: IpcConnectionContext.proto

#define INTERNAL_SUPPRESS_PROTOBUF_FIELD_DEPRECATION
#include "IpcConnectionContext.pb.h"

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

const ::google::protobuf::Descriptor* UserInformationProto_descriptor_ = NULL;
const ::google::protobuf::internal::GeneratedMessageReflection*
  UserInformationProto_reflection_ = NULL;
const ::google::protobuf::Descriptor* IpcConnectionContextProto_descriptor_ = NULL;
const ::google::protobuf::internal::GeneratedMessageReflection*
  IpcConnectionContextProto_reflection_ = NULL;

}  // namespace


void protobuf_AssignDesc_IpcConnectionContext_2eproto() {
  protobuf_AddDesc_IpcConnectionContext_2eproto();
  const ::google::protobuf::FileDescriptor* file =
    ::google::protobuf::DescriptorPool::generated_pool()->FindFileByName(
      "IpcConnectionContext.proto");
  GOOGLE_CHECK(file != NULL);
  UserInformationProto_descriptor_ = file->message_type(0);
  static const int UserInformationProto_offsets_[2] = {
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UserInformationProto, effectiveuser_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UserInformationProto, realuser_),
  };
  UserInformationProto_reflection_ =
    new ::google::protobuf::internal::GeneratedMessageReflection(
      UserInformationProto_descriptor_,
      UserInformationProto::default_instance_,
      UserInformationProto_offsets_,
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UserInformationProto, _has_bits_[0]),
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(UserInformationProto, _unknown_fields_),
      -1,
      ::google::protobuf::DescriptorPool::generated_pool(),
      ::google::protobuf::MessageFactory::generated_factory(),
      sizeof(UserInformationProto));
  IpcConnectionContextProto_descriptor_ = file->message_type(1);
  static const int IpcConnectionContextProto_offsets_[2] = {
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(IpcConnectionContextProto, userinfo_),
    GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(IpcConnectionContextProto, protocol_),
  };
  IpcConnectionContextProto_reflection_ =
    new ::google::protobuf::internal::GeneratedMessageReflection(
      IpcConnectionContextProto_descriptor_,
      IpcConnectionContextProto::default_instance_,
      IpcConnectionContextProto_offsets_,
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(IpcConnectionContextProto, _has_bits_[0]),
      GOOGLE_PROTOBUF_GENERATED_MESSAGE_FIELD_OFFSET(IpcConnectionContextProto, _unknown_fields_),
      -1,
      ::google::protobuf::DescriptorPool::generated_pool(),
      ::google::protobuf::MessageFactory::generated_factory(),
      sizeof(IpcConnectionContextProto));
}

namespace {

GOOGLE_PROTOBUF_DECLARE_ONCE(protobuf_AssignDescriptors_once_);
inline void protobuf_AssignDescriptorsOnce() {
  ::google::protobuf::GoogleOnceInit(&protobuf_AssignDescriptors_once_,
                 &protobuf_AssignDesc_IpcConnectionContext_2eproto);
}

void protobuf_RegisterTypes(const ::std::string&) {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedMessage(
    UserInformationProto_descriptor_, &UserInformationProto::default_instance());
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedMessage(
    IpcConnectionContextProto_descriptor_, &IpcConnectionContextProto::default_instance());
}

}  // namespace

void protobuf_ShutdownFile_IpcConnectionContext_2eproto() {
  delete UserInformationProto::default_instance_;
  delete UserInformationProto_reflection_;
  delete IpcConnectionContextProto::default_instance_;
  delete IpcConnectionContextProto_reflection_;
}

void protobuf_AddDesc_IpcConnectionContext_2eproto() {
  static bool already_here = false;
  if (already_here) return;
  already_here = true;
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  ::google::protobuf::DescriptorPool::InternalAddGeneratedFile(
    "\n\032IpcConnectionContext.proto\022\rhadoop.com"
    "mon\"\?\n\024UserInformationProto\022\025\n\reffective"
    "User\030\001 \001(\t\022\020\n\010realUser\030\002 \001(\t\"d\n\031IpcConne"
    "ctionContextProto\0225\n\010userInfo\030\002 \001(\0132#.ha"
    "doop.common.UserInformationProto\022\020\n\010prot"
    "ocol\030\003 \001(\tB\?\n\036org.apache.hadoop.ipc.prot"
    "obufB\032IpcConnectionContextProtos\240\001\001", 275);
  ::google::protobuf::MessageFactory::InternalRegisterGeneratedFile(
    "IpcConnectionContext.proto", &protobuf_RegisterTypes);
  UserInformationProto::default_instance_ = new UserInformationProto();
  IpcConnectionContextProto::default_instance_ = new IpcConnectionContextProto();
  UserInformationProto::default_instance_->InitAsDefaultInstance();
  IpcConnectionContextProto::default_instance_->InitAsDefaultInstance();
  ::google::protobuf::internal::OnShutdown(&protobuf_ShutdownFile_IpcConnectionContext_2eproto);
}

// Force AddDescriptors() to be called at static initialization time.
struct StaticDescriptorInitializer_IpcConnectionContext_2eproto {
  StaticDescriptorInitializer_IpcConnectionContext_2eproto() {
    protobuf_AddDesc_IpcConnectionContext_2eproto();
  }
} static_descriptor_initializer_IpcConnectionContext_2eproto_;

// ===================================================================

#ifndef _MSC_VER
const int UserInformationProto::kEffectiveUserFieldNumber;
const int UserInformationProto::kRealUserFieldNumber;
#endif  // !_MSC_VER

UserInformationProto::UserInformationProto()
  : ::google::protobuf::Message() {
  SharedCtor();
}

void UserInformationProto::InitAsDefaultInstance() {
}

UserInformationProto::UserInformationProto(const UserInformationProto& from)
  : ::google::protobuf::Message() {
  SharedCtor();
  MergeFrom(from);
}

void UserInformationProto::SharedCtor() {
  _cached_size_ = 0;
  effectiveuser_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  realuser_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
}

UserInformationProto::~UserInformationProto() {
  SharedDtor();
}

void UserInformationProto::SharedDtor() {
  if (effectiveuser_ != &::google::protobuf::internal::kEmptyString) {
    delete effectiveuser_;
  }
  if (realuser_ != &::google::protobuf::internal::kEmptyString) {
    delete realuser_;
  }
  if (this != default_instance_) {
  }
}

void UserInformationProto::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* UserInformationProto::descriptor() {
  protobuf_AssignDescriptorsOnce();
  return UserInformationProto_descriptor_;
}

const UserInformationProto& UserInformationProto::default_instance() {
  if (default_instance_ == NULL) protobuf_AddDesc_IpcConnectionContext_2eproto();
  return *default_instance_;
}

UserInformationProto* UserInformationProto::default_instance_ = NULL;

UserInformationProto* UserInformationProto::New() const {
  return new UserInformationProto;
}

void UserInformationProto::Clear() {
  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (has_effectiveuser()) {
      if (effectiveuser_ != &::google::protobuf::internal::kEmptyString) {
        effectiveuser_->clear();
      }
    }
    if (has_realuser()) {
      if (realuser_ != &::google::protobuf::internal::kEmptyString) {
        realuser_->clear();
      }
    }
  }
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
  mutable_unknown_fields()->Clear();
}

bool UserInformationProto::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!(EXPRESSION)) return false
  ::google::protobuf::uint32 tag;
  while ((tag = input->ReadTag()) != 0) {
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // optional string effectiveUser = 1;
      case 1: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_effectiveuser()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->effectiveuser().data(), this->effectiveuser().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(18)) goto parse_realUser;
        break;
      }

      // optional string realUser = 2;
      case 2: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_realUser:
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_realuser()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->realuser().data(), this->realuser().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
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

void UserInformationProto::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // optional string effectiveUser = 1;
  if (has_effectiveuser()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->effectiveuser().data(), this->effectiveuser().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      1, this->effectiveuser(), output);
  }

  // optional string realUser = 2;
  if (has_realuser()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->realuser().data(), this->realuser().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      2, this->realuser(), output);
  }

  if (!unknown_fields().empty()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        unknown_fields(), output);
  }
}

::google::protobuf::uint8* UserInformationProto::SerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // optional string effectiveUser = 1;
  if (has_effectiveuser()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->effectiveuser().data(), this->effectiveuser().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        1, this->effectiveuser(), target);
  }

  // optional string realUser = 2;
  if (has_realuser()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->realuser().data(), this->realuser().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        2, this->realuser(), target);
  }

  if (!unknown_fields().empty()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        unknown_fields(), target);
  }
  return target;
}

int UserInformationProto::ByteSize() const {
  int total_size = 0;

  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    // optional string effectiveUser = 1;
    if (has_effectiveuser()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->effectiveuser());
    }

    // optional string realUser = 2;
    if (has_realuser()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->realuser());
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

void UserInformationProto::MergeFrom(const ::google::protobuf::Message& from) {
  GOOGLE_CHECK_NE(&from, this);
  const UserInformationProto* source =
    ::google::protobuf::internal::dynamic_cast_if_available<const UserInformationProto*>(
      &from);
  if (source == NULL) {
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
    MergeFrom(*source);
  }
}

void UserInformationProto::MergeFrom(const UserInformationProto& from) {
  GOOGLE_CHECK_NE(&from, this);
  if (from._has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (from.has_effectiveuser()) {
      set_effectiveuser(from.effectiveuser());
    }
    if (from.has_realuser()) {
      set_realuser(from.realuser());
    }
  }
  mutable_unknown_fields()->MergeFrom(from.unknown_fields());
}

void UserInformationProto::CopyFrom(const ::google::protobuf::Message& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void UserInformationProto::CopyFrom(const UserInformationProto& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool UserInformationProto::IsInitialized() const {

  return true;
}

void UserInformationProto::Swap(UserInformationProto* other) {
  if (other != this) {
    std::swap(effectiveuser_, other->effectiveuser_);
    std::swap(realuser_, other->realuser_);
    std::swap(_has_bits_[0], other->_has_bits_[0]);
    _unknown_fields_.Swap(&other->_unknown_fields_);
    std::swap(_cached_size_, other->_cached_size_);
  }
}

::google::protobuf::Metadata UserInformationProto::GetMetadata() const {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::Metadata metadata;
  metadata.descriptor = UserInformationProto_descriptor_;
  metadata.reflection = UserInformationProto_reflection_;
  return metadata;
}


// ===================================================================

#ifndef _MSC_VER
const int IpcConnectionContextProto::kUserInfoFieldNumber;
const int IpcConnectionContextProto::kProtocolFieldNumber;
#endif  // !_MSC_VER

IpcConnectionContextProto::IpcConnectionContextProto()
  : ::google::protobuf::Message() {
  SharedCtor();
}

void IpcConnectionContextProto::InitAsDefaultInstance() {
  userinfo_ = const_cast< ::hadoop::common::UserInformationProto*>(&::hadoop::common::UserInformationProto::default_instance());
}

IpcConnectionContextProto::IpcConnectionContextProto(const IpcConnectionContextProto& from)
  : ::google::protobuf::Message() {
  SharedCtor();
  MergeFrom(from);
}

void IpcConnectionContextProto::SharedCtor() {
  _cached_size_ = 0;
  userinfo_ = NULL;
  protocol_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
}

IpcConnectionContextProto::~IpcConnectionContextProto() {
  SharedDtor();
}

void IpcConnectionContextProto::SharedDtor() {
  if (protocol_ != &::google::protobuf::internal::kEmptyString) {
    delete protocol_;
  }
  if (this != default_instance_) {
    delete userinfo_;
  }
}

void IpcConnectionContextProto::SetCachedSize(int size) const {
  GOOGLE_SAFE_CONCURRENT_WRITES_BEGIN();
  _cached_size_ = size;
  GOOGLE_SAFE_CONCURRENT_WRITES_END();
}
const ::google::protobuf::Descriptor* IpcConnectionContextProto::descriptor() {
  protobuf_AssignDescriptorsOnce();
  return IpcConnectionContextProto_descriptor_;
}

const IpcConnectionContextProto& IpcConnectionContextProto::default_instance() {
  if (default_instance_ == NULL) protobuf_AddDesc_IpcConnectionContext_2eproto();
  return *default_instance_;
}

IpcConnectionContextProto* IpcConnectionContextProto::default_instance_ = NULL;

IpcConnectionContextProto* IpcConnectionContextProto::New() const {
  return new IpcConnectionContextProto;
}

void IpcConnectionContextProto::Clear() {
  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (has_userinfo()) {
      if (userinfo_ != NULL) userinfo_->::hadoop::common::UserInformationProto::Clear();
    }
    if (has_protocol()) {
      if (protocol_ != &::google::protobuf::internal::kEmptyString) {
        protocol_->clear();
      }
    }
  }
  ::memset(_has_bits_, 0, sizeof(_has_bits_));
  mutable_unknown_fields()->Clear();
}

bool IpcConnectionContextProto::MergePartialFromCodedStream(
    ::google::protobuf::io::CodedInputStream* input) {
#define DO_(EXPRESSION) if (!(EXPRESSION)) return false
  ::google::protobuf::uint32 tag;
  while ((tag = input->ReadTag()) != 0) {
    switch (::google::protobuf::internal::WireFormatLite::GetTagFieldNumber(tag)) {
      // optional .hadoop.common.UserInformationProto userInfo = 2;
      case 2: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
          DO_(::google::protobuf::internal::WireFormatLite::ReadMessageNoVirtual(
               input, mutable_userinfo()));
        } else {
          goto handle_uninterpreted;
        }
        if (input->ExpectTag(26)) goto parse_protocol;
        break;
      }

      // optional string protocol = 3;
      case 3: {
        if (::google::protobuf::internal::WireFormatLite::GetTagWireType(tag) ==
            ::google::protobuf::internal::WireFormatLite::WIRETYPE_LENGTH_DELIMITED) {
         parse_protocol:
          DO_(::google::protobuf::internal::WireFormatLite::ReadString(
                input, this->mutable_protocol()));
          ::google::protobuf::internal::WireFormat::VerifyUTF8String(
            this->protocol().data(), this->protocol().length(),
            ::google::protobuf::internal::WireFormat::PARSE);
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

void IpcConnectionContextProto::SerializeWithCachedSizes(
    ::google::protobuf::io::CodedOutputStream* output) const {
  // optional .hadoop.common.UserInformationProto userInfo = 2;
  if (has_userinfo()) {
    ::google::protobuf::internal::WireFormatLite::WriteMessageMaybeToArray(
      2, this->userinfo(), output);
  }

  // optional string protocol = 3;
  if (has_protocol()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->protocol().data(), this->protocol().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    ::google::protobuf::internal::WireFormatLite::WriteString(
      3, this->protocol(), output);
  }

  if (!unknown_fields().empty()) {
    ::google::protobuf::internal::WireFormat::SerializeUnknownFields(
        unknown_fields(), output);
  }
}

::google::protobuf::uint8* IpcConnectionContextProto::SerializeWithCachedSizesToArray(
    ::google::protobuf::uint8* target) const {
  // optional .hadoop.common.UserInformationProto userInfo = 2;
  if (has_userinfo()) {
    target = ::google::protobuf::internal::WireFormatLite::
      WriteMessageNoVirtualToArray(
        2, this->userinfo(), target);
  }

  // optional string protocol = 3;
  if (has_protocol()) {
    ::google::protobuf::internal::WireFormat::VerifyUTF8String(
      this->protocol().data(), this->protocol().length(),
      ::google::protobuf::internal::WireFormat::SERIALIZE);
    target =
      ::google::protobuf::internal::WireFormatLite::WriteStringToArray(
        3, this->protocol(), target);
  }

  if (!unknown_fields().empty()) {
    target = ::google::protobuf::internal::WireFormat::SerializeUnknownFieldsToArray(
        unknown_fields(), target);
  }
  return target;
}

int IpcConnectionContextProto::ByteSize() const {
  int total_size = 0;

  if (_has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    // optional .hadoop.common.UserInformationProto userInfo = 2;
    if (has_userinfo()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::MessageSizeNoVirtual(
          this->userinfo());
    }

    // optional string protocol = 3;
    if (has_protocol()) {
      total_size += 1 +
        ::google::protobuf::internal::WireFormatLite::StringSize(
          this->protocol());
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

void IpcConnectionContextProto::MergeFrom(const ::google::protobuf::Message& from) {
  GOOGLE_CHECK_NE(&from, this);
  const IpcConnectionContextProto* source =
    ::google::protobuf::internal::dynamic_cast_if_available<const IpcConnectionContextProto*>(
      &from);
  if (source == NULL) {
    ::google::protobuf::internal::ReflectionOps::Merge(from, this);
  } else {
    MergeFrom(*source);
  }
}

void IpcConnectionContextProto::MergeFrom(const IpcConnectionContextProto& from) {
  GOOGLE_CHECK_NE(&from, this);
  if (from._has_bits_[0 / 32] & (0xffu << (0 % 32))) {
    if (from.has_userinfo()) {
      mutable_userinfo()->::hadoop::common::UserInformationProto::MergeFrom(from.userinfo());
    }
    if (from.has_protocol()) {
      set_protocol(from.protocol());
    }
  }
  mutable_unknown_fields()->MergeFrom(from.unknown_fields());
}

void IpcConnectionContextProto::CopyFrom(const ::google::protobuf::Message& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

void IpcConnectionContextProto::CopyFrom(const IpcConnectionContextProto& from) {
  if (&from == this) return;
  Clear();
  MergeFrom(from);
}

bool IpcConnectionContextProto::IsInitialized() const {

  return true;
}

void IpcConnectionContextProto::Swap(IpcConnectionContextProto* other) {
  if (other != this) {
    std::swap(userinfo_, other->userinfo_);
    std::swap(protocol_, other->protocol_);
    std::swap(_has_bits_[0], other->_has_bits_[0]);
    _unknown_fields_.Swap(&other->_unknown_fields_);
    std::swap(_cached_size_, other->_cached_size_);
  }
}

::google::protobuf::Metadata IpcConnectionContextProto::GetMetadata() const {
  protobuf_AssignDescriptorsOnce();
  ::google::protobuf::Metadata metadata;
  metadata.descriptor = IpcConnectionContextProto_descriptor_;
  metadata.reflection = IpcConnectionContextProto_reflection_;
  return metadata;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace common
}  // namespace hadoop

// @@protoc_insertion_point(global_scope)
