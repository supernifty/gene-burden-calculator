#!/bin/bash

./run_webserver.sh 2>webserver.err 1>webserver.out &
./run_runner.sh 2>runner.err 1>runner.out

