bin/flink list | egrep RUNNING | awk '{ print $4 }' | xargs -n1 bin/flink cancel
