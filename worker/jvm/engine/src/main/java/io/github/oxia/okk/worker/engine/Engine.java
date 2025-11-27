package io.github.oxia.okk.worker.engine;

import io.oxia.okk.proto.v1.ExecuteCommand;
import io.oxia.okk.proto.v1.ExecuteResponse;

public interface Engine {

    void init();

    ExecuteResponse onCommand(ExecuteCommand command);
}
