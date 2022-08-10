#pragma once

#include "config.h"
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
    static void HandleRpcs(SundialRPCServerImpl * s, uint32_t thd_id);
    Status contactRemote(ServerContext * context, const SundialRequest* request,
                     SundialResponse* response) override;
    static void processContactRemote(ServerContext* context, const SundialRequest* request,
                     SundialResponse* response);
private:
    /*
    class RPCServerThread {
      public:
        RPCServerThread(SundialRPCServerImpl * server) {
            thread_ = new std::thread(HandleRpcs, server);
        };
        ~RPCServerThread() {
            delete thread_;
        };
        std::thread * thread_;
#if NODE_TYPE == STORAGE_NODE
        SundialRequest thd_requests_[NUM_NODES];
        SundialResponse thd_responses_[NUM_NODES];
#else
        // request in phase 1, as leader of paxos
        SundialRequest thd_requests_[NUM_STORAGE_NODES];
        SundialResponse thd_responses_[NUM_STORAGE_NODES];
#endif
        // request in phase 2, as leader of paxos
        SundialRequest thd_requests2_[NUM_STORAGE_NODES];
        SundialResponse thd_responses2_[NUM_STORAGE_NODES];
    };
     */
    class CallData {
    public:
        CallData(SundialRPC::AsyncService* service, ServerCompletionQueue* cq,
                 uint32_t thd_id);
        void Proceed(uint32_t thd_id);
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
    // RPCServerThread ** _thread_pool;
    SundialRPC::AsyncService service_;
    std::unique_ptr<Server> server_;
    std::unique_ptr<ServerCompletionQueue> cq_;
};

