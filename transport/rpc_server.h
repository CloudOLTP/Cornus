#pragma once

#include "sundial.grpc.pb.h"
#include "sundial.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <grpcpp/grpcpp.h>

using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

class SundialRPCServerImpl final : public SundialRPC::Service {
public:
    void run();
    static void HandleRpcs(SundialRPCServerImpl * s);
    //void Export(net_http::HTTPServerInterface* http_server);
    Status contactRemote(ServerContext * context, const SundialRequest* request,
                     SundialResponse* response) override;
    static void processContactRemote(ServerContext* context, const SundialRequest* request,
                     SundialResponse* response);
private:
    class CallData {
    public:
        CallData(SundialRPC::AsyncService* service, ServerCompletionQueue* cq);
        void Proceed();
    private:
        enum CallStatus { CREATE, PROCESS, FINISH };
        SundialRPC::AsyncService* service_;
        ServerCompletionQueue* cq_;
        ServerContext ctx_;
        SundialRequest request_;
        SundialResponse reply_;
        ServerAsyncResponseWriter<SundialResponse> responder_;
        CallStatus status_;
    };
    //pthread_t **    _thread_pool;
    std::thread **  _thread_pool;
    SundialRPC::AsyncService service_;
    std::unique_ptr<Server> server_;
    std::unique_ptr<ServerCompletionQueue> cq_;
};

