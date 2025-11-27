package io.github.oxia.worker.engine.oxia;

public record Options(String serviceURL, String namespace) {
    private static final String ENV_KEY_SERVICE_URL = "OKK_WORKER_OXIA_SERVICE_URL";
    private static final String ENV_KEY_NAMESPACE = "OKK_WORKER_OXIA_NAMESPACE";


    public static Options fromEnv() {
        String serviceURL = System.getenv(ENV_KEY_SERVICE_URL);
        if (serviceURL == null || serviceURL.isEmpty()) {
            serviceURL = "localhost:6648";
        }

        String namespace = System.getenv(ENV_KEY_NAMESPACE);
        if (namespace == null || namespace.isEmpty()) {
            namespace = "default";
        }

        return new Options(serviceURL, namespace);
    }

}
