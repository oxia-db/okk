FROM eclipse-temurin:21


WORKDIR /okk


COPY ./worker/build/distributions/worker.tar /okk/worker.tar

RUN tar -xf worker.tar &&\
    rm -rf worker.tar


CMD ["/okk/worker/bin/worker"]