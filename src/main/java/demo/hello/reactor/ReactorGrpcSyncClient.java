package demo.hello.reactor;

import demo.proto.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import reactor.core.publisher.Flux;

public class ReactorGrpcSyncClient {

    public static void main(String[] args) throws Exception {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", 8888).usePlaintext().build();
        ReactorSkipGreeterGrpc.ReactorSkipGreeterStub stub = ReactorSkipGreeterGrpc.newReactorStub(channel);

        stub.skipGreet(
                Flux.just(
                        Frame.newBuilder().setConfig(Config.newBuilder().setSkip(3).build()).build(),
                        Frame.newBuilder().setPayload(Payload.newBuilder().setName("Andy").build()).build(),
                        Frame.newBuilder().setPayload(Payload.newBuilder().setName("Brad").build()).build(),
                        Frame.newBuilder().setPayload(Payload.newBuilder().setName("Charles").build()).build(),
                        Frame.newBuilder().setPayload(Payload.newBuilder().setName("Diane").build()).build(),
                        Frame.newBuilder().setPayload(Payload.newBuilder().setName("Edith").build()).build()
                ))
                .map(Greeting::getMessage)
                .log()
                .blockLast();

    }
}
