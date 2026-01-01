package io.github.oxia.worker.engine.oxia;

import io.github.oxia.okk.worker.engine.Engine;
import io.oxia.client.api.*;
import io.oxia.client.api.options.GetOption;
import io.oxia.client.api.options.PutOption;
import io.oxia.client.api.options.defs.OptionEphemeral;
import io.oxia.okk.proto.v1.*;
import io.oxia.okk.proto.v1.Record;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
        if (operation.hasPrecondition()) {
            final Precondition precondition = operation.getPrecondition();
            if (precondition.getBypassIfAssertKeyExist()) {
                // Idempotent operations
                if (operation.hasAssertion()) {
                    final Assertion assertion = operation.getAssertion();
                    if (!assertion.getRecordsList().isEmpty()) {
                        final Record expectRecord = assertion.getRecords(0);
                        final GetResult result = oxiaClient.get(expectRecord.getKey()).join();
                        if (result != null) {
                            // the key might be exists
                            final String getKey = result.key();
                            final byte[] getValue = result.value();
                            if (getKey.equals(expectRecord.getKey())
                                    && Arrays.equals(getValue, expectRecord.getValue().toByteArray())) {
                                log.info("[Put][{}] The precondition BypassIfAssertKeyExist is met.", operation.getSequence());
                                return ExecuteResponse.newBuilder()
                                        .setStatus(Status.Ok)
                                        .build();
                            } else {
                                log.warn("[Put][{}] Assertion failure, mismatched key or value. expect: key={} vlaue={} actual: key={} value={}",
                                        operation.getSequence(), expectRecord.getKey(), expectRecord.getValue(), getKey, getValue);
                                return ExecuteResponse.newBuilder()
                                        .setStatus(Status.AssertionFailure)
                                        .setStatusInfo("mismatched key or value.")
                                        .build();
                            }
                        }
                    }
                }
            }
        }

        final OperationPut put = operation.getPut();
        final var optionSet = new HashSet<PutOption>();
        if (put.hasPartitionKey()) {
            optionSet.add(PutOption.PartitionKey(put.getPartitionKey()));
        }
        if (put.getSequenceKeyDeltaCount() > 0) {
            optionSet.add(PutOption.SequenceKeysDeltas(put.getSequenceKeyDeltaList()));
        }
        if (put.getEphemeral()) {
            optionSet.add(OptionEphemeral.AsEphemeralRecord);
        }
        final PutResult result = oxiaClient.put(put.getKey(), put.getValue().toByteArray(), optionSet).join();

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (!assertion.getRecordsList().isEmpty()) {
                final Record expectRecord = assertion.getRecords(0);

                final String putKey = result.key();
                final String expectKey = expectRecord.getKey();
                if (!putKey.equals(expectKey)) {
                    log.warn("[Put][{}] Assertion failure, mismatched key. expect: key={} actual: key={}",
                            operation.getSequence(), expectRecord.getKey(), putKey);
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatched key.")
                            .build();
                }
            }
        }

        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processScan(Operation operation) {
        final OperationScan scanOp = operation.getScan();

//        final CompletableFuture<Void> future = new CompletableFuture<>();
//        final List<GetResult> results = new ArrayList<>();
//        oxiaClient.rangeScan(scanOp.getKeyStart(), scanOp.getKeyEnd(), new RangeScanConsumer() {
//            @Override
//            public void onNext(GetResult result) {
//                results.add(result);
//            }
//
//            @Override
//            public void onError(Throwable throwable) {
//                future.completeExceptionally(throwable);
//            }
//
//            @Override
//            public void onCompleted() {
//                future.complete(null);
//            }
//        });
//        future.join();
//
//        if (operation.hasAssertion()) {
//            final Assertion assertion = operation.getAssertion();
//            if (!assertion.getRecordsList().isEmpty()) {
//                log.info("[Scan][{}] Check the assertion records", operation.getSequence());
//
//                log.info("[Scan][{}] Assertion successful", operation.getSequence());
//            }
//        }

        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processGet(String testcase, Operation operation) {
        final OperationGet get = operation.getGet();
        final String key = get.getKey();
        final KeyComparisonType comparisonType = get.getComparisonType();

        final Set<GetOption> getOptions = new HashSet<>();
        switch (comparisonType) {
            case EQUAL -> getOptions.add(GetOption.ComparisonEqual);
            case FLOOR -> getOptions.add(GetOption.ComparisonFloor);
            case LOWER -> getOptions.add(GetOption.ComparisonLower);
            case HIGHER -> getOptions.add(GetOption.ComparisonHigher);
            case CEILING -> getOptions.add(GetOption.ComparisonCeiling);
            default -> {
            }
        }
        GetResult getResult = oxiaClient.get(key, getOptions).join();

        // avoid expose internal keys
        if (getResult != null){
            final String getKey = getResult.key();
            if (getKey.startsWith("__oxia/")  //
                    || !getKey.startsWith(testcase)) {
                getResult = null;
            }
        }

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasEmptyRecords() && assertion.getEmptyRecords()) {
                log.info("[Get][{}] Check the assertion empty records", operation.getSequence());
                if (getResult != null) {
                    log.warn("[Get][{}] Assertion failure, expect empty record. actual: key={} value={}",
                            operation.getSequence(), getResult.key(), getResult.value());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatch key or value")
                            .build();
                }
                log.info("[Get][{}] Assertion successful", operation.getSequence());
            }
            final List<Record> recordsList = assertion.getRecordsList();
            if (!recordsList.isEmpty()) {
                final String actualKey = getResult.key();
                final byte[] actualValue = getResult.value();
                log.info("[Get][{}] Check the assertion records", operation.getSequence());
                final Record expectRecord = recordsList.get(0);
                if (!expectRecord.getKey().equals(actualKey) || !Arrays.equals(expectRecord.getValue().toByteArray(), actualValue)) {
                    log.warn("[Get][{}] Assertion failure, mismatched key or value. expect: key={} value={} actual: key={} value={}",
                            operation.getSequence(), expectRecord.getKey(), expectRecord.getValue(), actualKey, actualValue);
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("mismatch key or value")
                            .build();
                }
                log.info("[Get][{}] Assertion successful", operation.getSequence());
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processList(Operation operation) {
        final OperationList listOp = operation.getList();
        final List<String> actualKeys = oxiaClient.list(listOp.getKeyStart(), listOp.getKeyEnd()).join();

        if (operation.hasAssertion()) {
            final Assertion assertion = operation.getAssertion();
            if (assertion.hasEventuallyEmpty() || assertion.hasEmptyRecords()) {
                log.info("[List][{}] Check the empty assertion", operation.getSequence());
                if (!actualKeys.isEmpty()) {
                    log.warn("[List][{}] Assertion failure", operation.getSequence());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("expect empty, but the actual is %s ".formatted(String.join(",", actualKeys)))
                            .build();
                }
                log.info("[List][{}] Assertion successful", operation.getSequence());
            }
            if (!assertion.getRecordsList().isEmpty()) {
                log.info("[List][{}] Check the assertion records", operation.getSequence());
                final List<String> expectKeys = assertion.getRecordsList().stream().map(Record::getKey).toList();
                if (!actualKeys.equals(expectKeys)) {
                    log.warn("[List][{}] Assertion failure", operation.getSequence());
                    return ExecuteResponse.newBuilder()
                            .setStatus(Status.AssertionFailure)
                            .setStatusInfo("different keys expect %s, but the actual is %s ".formatted(
                                    String.join(",", expectKeys),
                                    String.join(",", actualKeys)
                            ))
                            .build();
                }
                log.info("[List][{}] Assertion successful", operation.getSequence());
            }
        }
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    @SneakyThrows
    private ExecuteResponse processSessionRestart(Operation __) {
        oxiaClient.close();
        initClient();
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processDelete(Operation operation) {
        final OperationDelete delete = operation.getDelete();
        final String key = delete.getKey();
        oxiaClient.delete(key).join();
        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }

    private ExecuteResponse processDeleteRange(Operation operation) {
        final OperationDeleteRange deleteRange = operation.getDeleteRange();
        final String keyStart = deleteRange.getKeyStart();
        final String keyEnd = deleteRange.getKeyEnd();

        oxiaClient.deleteRange(keyStart, keyEnd).join();

        return ExecuteResponse.newBuilder()
                .setStatus(Status.Ok)
                .build();
    }


    @Override
    public ExecuteResponse onCommand(ExecuteCommand command) {
        final Operation operation = command.getOperation();
        try {
            return switch (operation.getOperationCase()) {
                case GET -> processGet(command.getTestcase(), operation);
                case PUT -> processPut(operation);
                case LIST -> processList(operation);
                case SCAN -> processScan(operation);
                case DELETE -> processDelete(operation);
                case SESSION_RESTART -> processSessionRestart(operation);
                case DELETE_RANGE -> processDeleteRange(operation);
                case OPERATION_NOT_SET -> {
                    log.error("Unsupported operation. operation={}", operation);
                    yield ExecuteResponse.newBuilder()
                            .setStatus(Status.NonRetryableFailure)
                            .setStatusInfo("Unsupported Operation.")
                            .build();
                }
            };
        } catch (Throwable ex) {
            log.error("unexpected error", ex);
            return ExecuteResponse.newBuilder()
                    .setStatus(Status.RetryableFailure)
                    .setStatusInfo(ex.getMessage())
                    .build();
        }
    }


}
