#!/usr/bin/env bash
frink=/opt/module/flink-yarn/bin/flink
jar=/opt/lib

apps=(
  com.missouri.realtime.app.DWD.DWDDbApp
  #com.missouri.realtime.app.DWD.DWDLogApp
  #com.missouri.realtime.app.DWM.DwmUvApp
  #com.missouri.realtime.app.DWM.DwmJumpDetailApp
  com.missouri.realtime.app.DWM.DwmOrderWideApp_Cache_Async
)
  #遍历数组
for app in ${apps[*]} ; do
    $flink run -d -c $app $jar
done