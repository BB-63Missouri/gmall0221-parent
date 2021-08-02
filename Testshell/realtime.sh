#!/usr/bin/env bash
frink=/opt/module/flink-yarn/bin/flink
jar=/opt/

apps=(
  com.missouri.realtime.app.DWD.DWDDbApp
  com.missouri.realtime.app.DWD.DWDLogApp
)
  #便历数组
for app in ${apps[*]} ; do
    $flink run -d -c $app $jar
done