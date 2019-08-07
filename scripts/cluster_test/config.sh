#!/bin/bash -eux

base_url="http://consul:8500/v1/kv/viyadb"

write_key() {
  curl -sSfL --request PUT --data "$2" $base_url/$1
}

cluster_config=$(cat<<EOF
{
  "replication_factor": 1,
  "tables": ["events"],
  "indexers": ["main"]
}
EOF
)
write_key "clusters/cluster001/config" "$cluster_config"

table_config=$(cat<<EOF
{
  "name": "events",
  "dimensions": [
    {"name": "app_id"},
    {"name": "user_id", "type": "uint"},
    {
      "name": "event_time", "type": "time",
      "format": "millis", "granularity": "day"
    },
    {"name": "country"},
    {"name": "city"},
    {"name": "device_type"},
    {"name": "device_vendor"},
    {"name": "ad_network"},
    {"name": "campaign"},
    {"name": "site_id"},
    {"name": "event_type"},
    {"name": "event_name"},
    {"name": "organic", "cardinality": 2},
    {"name": "days_from_install", "type": "ushort"}
  ],
  "metrics": [
    {"name": "revenue" , "type": "double_sum"},
    {"name": "users", "type": "bitset", "field": "user_id", "max": 4294967295},
    {"name": "count" , "type": "count"}
  ]
}
EOF
)
write_key "tables/events/config" "$table_config"

indexer_config=$(cat<<EOF
{
   "tables":[
      "events"
   ],
   "deepStorePath":"/tmp/viyadb/deepstore",
   "realTime":{
      "windowDuration":"PT15S",
      "kafkaSource":{
         "topics":[
            "events"
         ],
         "brokers":[
            "kafka:9092"
         ]
      },
      "parseSpec":{
         "format":"json",
         "timeColumn":{
            "name":"event_time"
         }
      },
      "notifier":{
         "type":"kafka",
         "channel":"kafka:9092",
         "queue":"rt-notifications"
      }
   },
   "batch":{
      "partitioning":{
         "columns":[
            "app_id"
         ],
         "partitions":2
      },
      "notifier":{
         "type":"kafka",
         "channel":"kafka:9092",
         "queue":"batch-notifications"
      }
   }
}
EOF
)
write_key "indexers/main/config" "$indexer_config"

sleep 10000
