#!/bin/bash

su hdfs -c "hadoop fs -copyFromLocal /mnt/share/lib/* /user/oozie/share/lib/"

exit 0
