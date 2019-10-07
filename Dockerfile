FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy build-essential unixodbc-dev cmake

COPY . workdir/
RUN cd workdir && cmake -DCMAKE_BUILD_TYPE=Release . && make

FROM ubuntu:bionic

RUN apt-get update && apt-get install -qy unixodbc

COPY --from=0 /workdir/chbenchmark /usr/local/bin/chBenchmark

ENTRYPOINT ["chBenchmark"]
