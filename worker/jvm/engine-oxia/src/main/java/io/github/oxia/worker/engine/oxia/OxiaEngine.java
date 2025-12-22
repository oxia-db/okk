package io.github.oxia.worker.engine.oxia;

import io.github.oxia.okk.worker.engine.Engine;
import io.oxia.client.api.AsyncOxiaClient;
import io.oxia.client.api.OxiaClientBuilder;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionEphemeral;
import io.oxia.okk.proto.v1.Assertion;
import io.oxia.okk.proto.v1.ExecuteCommand;
import io.oxia.okk.proto.v1.ExecuteResponse;
import io.oxia.okk.proto.v1.Operation;
import io.oxia.okk.proto.v1.OperationList;
import io.oxia.okk.proto.v1.OperationPut;
import io.oxia.okk.proto.v1.Status;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;

@Slf4j
public class OxiaEngine implements Engine {

    private Options options;
    private AsyncOxiaClient oxiaClient;

    @Override
    public void init() {
        options = Options.fromEnv();
        log.info("Loading oxia engine. options: {}", options);
        initClient();
    }


    private void initClient() {
        oxiaClient = OxiaClientBuilder.create(options.serviceURL())
                .namespace(options.namespace())
                .asyncClient().join();
    }


    private ExecuteResponse processPut(Operation operation) {
        final OperationPut put = operation.getPut();
        final var optionSet = new HashSet<PutOption>();
        optionSet.add(OptionEphemeral.AsEphemeralRecord);
        try {
            oxiaClient.put(put.getKey(), put.getValue().toByteArray(), optionSet).join();
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.Ok)
                    .build();
        } catch (Throwable ex) {
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.RetryableFailure)
                    .setStatusInfo(ex.getMessage())
                    .build();
        }
    }

    private ExecuteResponse processScan(Operation operation) {
        return  ExecuteResponse.newBuilder()
                .setStatus(Status.NonRetryableFailure)
                .setStatusInfo("Unsupported Operation.")
                .build();
    }

    private ExecuteResponse processGet(Operation operation) {
        return  ExecuteResponse.newBuilder()
                .setStatus(Status.NonRetryableFailure)
                .setStatusInfo("Unsupported Operation.")
                .build();
    }

    private ExecuteResponse processList(Operation operation) {
        final Assertion assertion = operation.getAssertion();
        final OperationList listOp = operation.getList();
        final List<String> keys = oxiaClient.list(listOp.getKeyStart(), listOp.getKeyEnd()).join();
        if (assertion.hasEmpty()) {
            if (!keys.isEmpty()) {
                return ExecuteResponse.newBuilder()
                        .setStatus(Status.AssertionFailure)
                        .setStatusInfo("expect empty, but the actual is %s ".formatted(String.join(",", keys)))
                        .build();
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processSessionRestart(Operation operation) {
        try {
            oxiaClient.close();
        } catch (Exception ex) {
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.RetryableFailure)
                    .setStatusInfo(ex.getMessage())
                    .build();
        }
        initClient();
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processDelete(Operation operation) {
        return  ExecuteResponse.newBuilder()
                .setStatus(Status.NonRetryableFailure)
                .setStatusInfo("Unsupported Operation.")
                .build();
    }


    @Override
    public ExecuteResponse onCommand(ExecuteCommand command) {
        final Operation operation = command.getOperation();
        return switch (operation.getOperationCase()) {
            case GET -> processGet(operation);
            case PUT -> processPut(operation);
            case LIST -> processList(operation);
            case SCAN -> processScan(operation);
            case DELETE -> processDelete(operation);
            case SESSION_RESTART -> processSessionRestart(operation);
            case RANGE_DELETE -> null;
            case OPERATION_NOT_SET -> {
                log.error("Unsupported operation. operation={}", operation);
                yield ExecuteResponse.newBuilder()
                        .setStatus(Status.NonRetryableFailure)
                        .setStatusInfo("Unsupported Operation.")
                        .build();
            }
        };
    }


}
