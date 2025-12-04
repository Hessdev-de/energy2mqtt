#!/bin/sh

# Functions are based on https://github.com/Milesight-IoT/SensorDecoders/blob/main/wt-series/wt303/wt303-encoder.js

set -e

payload=""
imme=true
port=85

########################################################
# SYSTEM CONTROL
#
# Set the main control system (setTemperatureControlMode in wt303-encoder.js)
if [ "x$1" = "xset_mode" ]; then

    MODE=0 # fan_only using setTemperatureControlMode

    if [ "x$PAYLOAD" = "xheat" ]; then
        MODE=1
    elif [ "x$PAYLOAD" = "xcool" ]; then
        MODE=2
    fi

    payload=`printf "68%02X" $MODE`

#######
# set_temp needs to check for the mode (heating or cooling)
elif [ "x$1" = "xset_temp" ]; then
    # Encoded as 16 bit LE
    TEMPERATURE=`jq -n $PAYLOAD*100`

    if [ $TEMPERATURE < 500 -o $TEMPERATURE > 3800 ]; then
        echo "Temperature out of scope 5 < $PAYLOAD < 38" >&2
        exit 1
    fi

    MODE="6B" # Heatig, see setHeatingTargetTemperature
    if [ "x$DEV_TEMPERATURE_CONTROL_MODE" = "xcooling" ]; then
        MODE="6C"
    fi

    # Honor the endianess
    endian=`printf %04X $TEMPERATURE`
    high=`echo $endian | cut -b 1-2`
    low=`echo $endian | cut -b 3-4`

    payload=`printf "%s%s%s" $MODE $low $high`

########################################################
# FAN CONTROL
#
# preset_mode_command sets the fan speed
elif [ "x$1" = "xpreset_mode_command" ]; then
    # var mode_map = { 0: "auto", 1: "low", 2: "medium", 3: "high" };

    MODE=0 # auto mode
    if [ "x$PAYLOAD" = "xlow" ]; then
        MODE=1
    elif [ "x$PAYLOAD" = "xmedium" ]; then
        MODE=2
    elif [ "x$PAYLOAD" = "xhigh" ]; then
        MODE=3
    fi

    payload=`printf "72%02X" $MODE`

elif [ "x$1" = "xcommand" ]; then
    # we set low if the user says to enable the fan
    payload="7201"


########################################################
# UNDEFINED COMMAND
#
else
    echo -n "command $1 not allowed with payload $PAYLOAD" >&2
    exit 1
fi

echo -n "{ \"proto\": \"lora\", \"id\": \"$DEVICE\", \"imme\": $imme, \"port\": $port, \"payload\": \"$payload\" }"
exit 0