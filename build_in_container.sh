#!/bin/bash

sudo docker run --rm -ti -v $PWD:/app -w /app rust:bookworm cargo build --release
