package io.github.oxia.okk.worker;

import com.google.common.base.Throwables;
import com.google.protobuf.util.JsonFormat;
import io.github.oxia.okk.worker.engine.Engine;
import io.grpc.stub.StreamObserver;
import io.oxia.okk.proto.v1.ExecuteCommand;
import io.oxia.okk.proto.v1.ExecuteResponse;
import io.oxia.okk.proto.v1.OkkGrpc;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WorkerService extends OkkGrpc.OkkImplBase {
    private final Engine engine;

    public WorkerService(Engine engine) {
        this.engine = engine;
    }

    @Override
    public StreamObserver<ExecuteCommand> execute(StreamObserver<ExecuteResponse> responseObserver) {
        log.info("Open stream.");
        return new StreamObserver<>() {

            @Override
            public void onNext(ExecuteCommand command) {
                try {
                    log.info("Received command: {}", JsonFormat.printer()
                            .omittingInsignificantWhitespace()
                            .print(command));
                    final ExecuteResponse executeResponse = engine.onCommand(command);
                    responseObserver.onNext(executeResponse);
                } catch (Throwable ex) {
                    log.error("Stream has been closed due to an unexpected error when processing the command.");
                    responseObserver.onError(ex);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                log.error("Stream has been closed by peer error.", Throwables.getRootCause(throwable));
                responseObserver.onCompleted();
            }

            @Override
            public void onCompleted() {
                log.info("Stream has been completed.");
                responseObserver.onCompleted();
            }
        };
    }
}
