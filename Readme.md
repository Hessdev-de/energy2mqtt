# Energy2MQTT

Energy2MQTT provides a way to handle multiple energy meters and protocols in a unified way. The idea is to remove the need of understanding the meter data from smart home tools and energy managements systems. We want to implement more and more meters of time.

The system is build around MQTT, therefore the name. Home Assistant is the main target for now because the latest upgraded broke Modbus too many times. Energy2MQTT works around that.

## Building from source

You can build Energy2MQTT by running `cargo build --release`.

## Building the container

You can use `docker build .` as well as `podman build .` to build the image. The image is as small als possible to reduce memory usage and attack surface.

## Running the container
You can run the container from docker hub:

`docker run  hessdev/energy2mqtt:lastest`

If you want to have your configuration persistant, bind `/config` to a volume or use a bind mount.

`docker run -v /path/to/config:/config:rw hessdev/energy2mqtt:latest`

