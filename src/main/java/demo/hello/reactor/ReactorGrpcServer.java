package demo.hello.reactor;

import demo.proto.*;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorGrpcServer extends ReactorSkipGreeterGrpc.SkipGreeterImplBase {
    public static void main(String[] args) throws Exception {
        demonstrateLocalSwitchOnFirst();

        // Start the server
        Server server = ServerBuilder.forPort(8888).addService(new ReactorGrpcServer()).build().start();
        server.awaitTermination();

    }

    private static void demonstrateLocalSwitchOnFirst() {
        applySkipGreet(Flux.just(
                Frame.newBuilder().setConfig(Config.newBuilder().setSkip(3).build()).build(),
                Frame.newBuilder().setPayload(Payload.newBuilder().setName("Andy").build()).build(),
                Frame.newBuilder().setPayload(Payload.newBuilder().setName("Brad").build()).build(),
                Frame.newBuilder().setPayload(Payload.newBuilder().setName("Charles").build()).build(),
                Frame.newBuilder().setPayload(Payload.newBuilder().setName("Diane").build()).build(),
                Frame.newBuilder().setPayload(Payload.newBuilder().setName("Edith").build()).build())
                .hide()
        )
                .map(Greeting::getMessage)
                .doOnNext(message -> System.err.println("RECEIVED: " + message))
                .blockLast();
        System.out.println("local demonstration done");
    }

    @Override
    public Flux<Greeting> skipGreet(Flux<Frame> request) {
        return applySkipGreet(request);
    }

    private static Flux<Greeting> applySkipGreet(Flux<Frame> request) {
        return request
                .doOnNext(frame -> System.out.println("input from client: " + frame.toString().replaceAll("\n", "\t")))
                .switchOnFirst((firstSignal, all) -> {
                    if (firstSignal.isOnNext()) {
                        Frame frame = firstSignal.get();
                        assert frame != null;
                        if (frame.hasConfig()) {
                            int skip = frame.getConfig().getSkip();
                            return all.filter(Frame::hasPayload).skip(skip).map(Frame::getPayload);
                        }
                        else {
                            //no configuration frame
                            return Mono.error(new IllegalArgumentException("Missing Config frame at start"));
                        }
                    } else {
                        //FIXME does this suppress an immediate error?
                        return Flux.empty(); //the input completes or errors immediately, so no real payload
                    }
                })
                .map(payload -> Greeting.newBuilder().setMessage("Greetings, " + payload.getName()).build());
    }
}
