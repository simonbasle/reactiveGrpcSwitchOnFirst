package demo.hello.reactor;

import demo.proto.Frame;
import demo.proto.Greeting;
import demo.proto.ReactorSkipGreeterGrpc;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class ReactorGrpcServer extends ReactorSkipGreeterGrpc.SkipGreeterImplBase {
    public static void main(String[] args) throws Exception {
        // Start the server
        Server server = ServerBuilder.forPort(8888).addService(new ReactorGrpcServer()).build().start();
        server.awaitTermination();
    }

    @Override
    public Flux<Greeting> skipGreet(Flux<Frame> request) {
        return request
                .log("before switchOnFirst")
                .switchOnFirst((firstSignal, all) -> {
                    if (firstSignal.isOnNext()) {
                        Frame frame = firstSignal.get();
                        assert frame != null;
                        if (frame.hasConfig()) {
                            int skip = frame.getConfig().getSkip();
                            return all.skip(skip).filter(Frame::hasPayload).map(Frame::getPayload);
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
