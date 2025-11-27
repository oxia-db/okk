package io.github.oxia.okk.worker;


import io.github.oxia.okk.worker.engine.Engine;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class Worker implements Runnable {
    private final Options options;

    public Worker(Options options) {
        this.options = options;
    }


    @SneakyThrows
    @Override
    public void run() {
        log.info("Starting worker. options: {}", options);
        final Engine engine = EngineFactory.loadEngine(options.engineName());
        engine.init();

        final Server server = ServerBuilder.forPort(6666)
                .addService(new WorkerService(engine))
                .build()
                .start();
        log.info("GRPC server has been started. port: {}", 6666);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down gRPC server since JVM is shutting down");
            try {
                server.shutdown().awaitTermination();
            } catch (InterruptedException ex) {
                log.error("JVM shutdown hook interrupted", ex);
            }
            log.info("Grpc Server shut down");
        }));
        server.awaitTermination();
    }
}
