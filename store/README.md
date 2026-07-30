# Store for energy2mqtt

## Idea

energy2mqtt itself is stateless. No data is stored beside some temporary data. store4mqtt handles this bit which is missing in energy2mqtt while integrating directly into energy2mqtt.

store4mqtt writes received metering data, we call those meter records. We store that information in a database which can be accessed by different processes on the device.

## Data structure

Each device has it's own table to speed up the search because we expect to search for a single device but not for all metering data from all devices in a longer period of time.

The databases store data with an UNIX Timestamp (PRIMARY UNIQUE INDEX), the relevant JSON document and the trigger which caused the storage.

## Configuration

```yaml
mqtt:
  host: your_mqtt_host
  port: 1883
  user: mqtt_user_name
  pass: look_at_this_secure_password

default:
  # How often will the retention thread run and delete data from the tables
  retention: !Seconds 60

# You can specify as many topics as you like
topics:
  # subtopics subscriptions need th have a name_template
  - topic: energy2mqtt/devs/#

    # topic will be energy2mqtt/devs/KNX/dg_1_a
    # we split that into path_$idx values
    # path_1 = energy2mqtt
    # path_2 = devs
    # path_3 = KNX
    # path_4 = dg_1_a
    # the resulting name for sqlite will be KNX_dg_1_a
    name_template: 'path_2 + "_" + path_3'
    # Tell the system when to record data
    store:
      # Every 90 seconds
      - !Seconds 90
      # At an exact point of time (local time)
      - !Exact
        - 15
        - 51
        - 12
      # When ever someone publishes to one of those topics
      - !Trigger triggers/store
      - !Trigger your/application/changed/something

    # optional: German laws you may only use data which has a specific
    # timings so we allow to set the maximum number of seconds which
    # data is allowed to arraive BEFORE or AFTER the correct point in time
    #
    # 3% of 900 seconds (15 Minutes)
    max_variant: !Seconds 27

    # optional: Delete all values older than two hours for this topic
    # and all tables belonging to it.
    #
    # defaults to 90 days if left out
    retention: !Hours 2

    # optional: Specify the name of the database where the data should be stored.
    # You can have seperate database for metering, logs and event.
    # Each db has it's own retention setting. This way you can keep metering
    # data for years but clean up logs every 96 hours.
    # 
    # defaults to "store"
    database: metering
```