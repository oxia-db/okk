package io.github.oxia.okk.worker;


import lombok.Builder;

@Builder
public record Options(String engineName) {

    private static final String ENV_KEY_ENGINE_NAME = "OKK_WORKER_ENGINE_NAME";

    public static Options fromEnv() {
        String engineName = System.getenv(ENV_KEY_ENGINE_NAME);
        if (engineName == null || engineName.isEmpty()) {
            engineName = "oxia";
        }

        return Options.builder()
                .engineName(engineName)
                .build();
    }
}
