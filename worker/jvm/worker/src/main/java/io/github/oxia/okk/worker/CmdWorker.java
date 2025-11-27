package io.github.oxia.okk.worker;


import picocli.CommandLine;

@CommandLine.Command(name = "worker", mixinStandardHelpOptions = true, version = "1.0",
        description = "Okk JVM worker")
public final class CmdWorker implements Runnable {

    public static void main(String[] args) {
        final int code = new CommandLine(new CmdWorker()).execute(args);
        System.exit(code);
    }

    @Override
    public void run() {
        final Options options = Options.fromEnv();
        final Worker worker = new Worker(options);
        worker.run();
    }
}
