// Generated by the gRPC C++ plugin.
// If you make any local change, they will be lost.
// source: sundial.proto

#include "sundial.pb.h"
#include "sundial.grpc.pb.h"

#include <functional>
#include <grpcpp/impl/codegen/async_stream.h>
#include <grpcpp/impl/codegen/async_unary_call.h>
#include <grpcpp/impl/codegen/channel_interface.h>
#include <grpcpp/impl/codegen/client_unary_call.h>
#include <grpcpp/impl/codegen/client_callback.h>
#include <grpcpp/impl/codegen/message_allocator.h>
#include <grpcpp/impl/codegen/method_handler.h>
#include <grpcpp/impl/codegen/rpc_service_method.h>
#include <grpcpp/impl/codegen/server_callback.h>
#include <grpcpp/impl/codegen/server_callback_handlers.h>
#include <grpcpp/impl/codegen/server_context.h>
#include <grpcpp/impl/codegen/service_type.h>
#include <grpcpp/impl/codegen/sync_stream.h>
namespace sundial_rpc {

static const char* SundialRPC_method_names[] = {
  "/sundial_rpc.SundialRPC/contactRemote",
};

std::unique_ptr< SundialRPC::Stub> SundialRPC::NewStub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options) {
  (void)options;
  std::unique_ptr< SundialRPC::Stub> stub(new SundialRPC::Stub(channel, options));
  return stub;
}

SundialRPC::Stub::Stub(const std::shared_ptr< ::grpc::ChannelInterface>& channel, const ::grpc::StubOptions& options)
  : channel_(channel), rpcmethod_contactRemote_(SundialRPC_method_names[0], options.suffix_for_stats(),::grpc::internal::RpcMethod::NORMAL_RPC, channel)
  {}

::grpc::Status SundialRPC::Stub::contactRemote(::grpc::ClientContext* context, const ::sundial_rpc::SundialRequest& request, ::sundial_rpc::SundialResponse* response) {
  return ::grpc::internal::BlockingUnaryCall< ::sundial_rpc::SundialRequest, ::sundial_rpc::SundialResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), rpcmethod_contactRemote_, context, request, response);
}

void SundialRPC::Stub::async::contactRemote(::grpc::ClientContext* context, const ::sundial_rpc::SundialRequest* request, ::sundial_rpc::SundialResponse* response, std::function<void(::grpc::Status)> f) {
  ::grpc::internal::CallbackUnaryCall< ::sundial_rpc::SundialRequest, ::sundial_rpc::SundialResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_contactRemote_, context, request, response, std::move(f));
}

void SundialRPC::Stub::async::contactRemote(::grpc::ClientContext* context, const ::sundial_rpc::SundialRequest* request, ::sundial_rpc::SundialResponse* response, ::grpc::ClientUnaryReactor* reactor) {
  ::grpc::internal::ClientCallbackUnaryFactory::Create< ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(stub_->channel_.get(), stub_->rpcmethod_contactRemote_, context, request, response, reactor);
}

::grpc::ClientAsyncResponseReader< ::sundial_rpc::SundialResponse>* SundialRPC::Stub::PrepareAsynccontactRemoteRaw(::grpc::ClientContext* context, const ::sundial_rpc::SundialRequest& request, ::grpc::CompletionQueue* cq) {
  return ::grpc::internal::ClientAsyncResponseReaderHelper::Create< ::sundial_rpc::SundialResponse, ::sundial_rpc::SundialRequest, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(channel_.get(), cq, rpcmethod_contactRemote_, context, request);
}

::grpc::ClientAsyncResponseReader< ::sundial_rpc::SundialResponse>* SundialRPC::Stub::AsynccontactRemoteRaw(::grpc::ClientContext* context, const ::sundial_rpc::SundialRequest& request, ::grpc::CompletionQueue* cq) {
  auto* result =
    this->PrepareAsynccontactRemoteRaw(context, request, cq);
  result->StartCall();
  return result;
}

SundialRPC::Service::Service() {
  AddMethod(new ::grpc::internal::RpcServiceMethod(
      SundialRPC_method_names[0],
      ::grpc::internal::RpcMethod::NORMAL_RPC,
      new ::grpc::internal::RpcMethodHandler< SundialRPC::Service, ::sundial_rpc::SundialRequest, ::sundial_rpc::SundialResponse, ::grpc::protobuf::MessageLite, ::grpc::protobuf::MessageLite>(
          [](SundialRPC::Service* service,
             ::grpc::ServerContext* ctx,
             const ::sundial_rpc::SundialRequest* req,
             ::sundial_rpc::SundialResponse* resp) {
               return service->contactRemote(ctx, req, resp);
             }, this)));
}

SundialRPC::Service::~Service() {
}

::grpc::Status SundialRPC::Service::contactRemote(::grpc::ServerContext* context, const ::sundial_rpc::SundialRequest* request, ::sundial_rpc::SundialResponse* response) {
  (void) context;
  (void) request;
  (void) response;
  return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED, "");
}


}  // namespace sundial_rpc

