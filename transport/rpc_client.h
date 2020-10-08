#pragma once

#include "sundial.grpc.pb.h"
#include "sundial.pb.h"
#include <iostream>
#include <memory>
#include <string>
#include <grpcpp/grpcpp.h>
#include <thread>

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;
using sundial_rpc::SundialRequest;
using sundial_rpc::SundialResponse;
using sundial_rpc::SundialRPC;

class TxnManager;
struct AsyncClientCall {
    // Container for the data we expect from the server.
    SundialResponse reply;
    // Context for the client. It could be used to convey extra information to
    // the server and/or tweak certain RPC behaviors.
    ClientContext context;
    // Storage for the status of the RPC upon completion.
    Status status;
    std::unique_ptr<ClientAsyncResponseReader<SundialResponse>> response_reader;
};

class SundialRPCClientStub {
public:
    SundialRPCClientStub (std::shared_ptr<Channel> channel) : stub_(SundialRPC::NewStub(channel)) {};
    Status contactRemote(ClientContext* context, SundialRequest &request, SundialResponse* response) {
	    Status s = stub_->contactRemote(context, request, response);
	    return s;
    };
    std::unique_ptr<SundialRPC::Stub> stub_;
};

class SundialRPCClient {
public:
    SundialRPCClient();
    static void AsyncCompleteRpc(SundialRPCClient * s);
    void sendRequest(uint64_t node_id, SundialRequest &request, SundialResponse &response);
    void sendRequestAsync(TxnManager * txn, uint64_t node_id,
                          SundialRequest &request, SundialResponse &response);
    void sendRequestDone(SundialResponse &response);
private:
    //std::unique_ptr<SundialRPC::Stub> ** _servers;
    SundialRPCClientStub ** _servers;
    CompletionQueue cq;
    //pthread_t * _thread;
    std::thread * _thread;
};
