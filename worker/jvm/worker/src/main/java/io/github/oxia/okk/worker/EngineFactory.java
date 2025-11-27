package io.github.oxia.okk.worker;

import io.github.oxia.okk.worker.engine.Engine;
import io.github.oxia.worker.engine.oxia.OxiaEngine;
import lombok.experimental.UtilityClass;

import java.util.Objects;

@UtilityClass
public final class EngineFactory {

    public static Engine loadEngine(String name) {
        if (Objects.equals(name, "oxia")) {
            return new OxiaEngine();
        }
        throw new UnsupportedOperationException(String.format("%s is not supported", name));
    }
}
