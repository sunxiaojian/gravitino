#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

# Copy main Gravitino configuration files
cp /tmp/conf/* ${GRAVITINO_HOME}/conf
cp /tmp/conf/log4j2.properties ${GRAVITINO_HOME}/conf

# Create Iceberg REST server configuration directory if it doesn't exist
mkdir -p ${GRAVITINO_HOME}/iceberg-rest-server/conf

# Copy Iceberg REST configuration files if they exist
if [ -d "/tmp/iceberg-rest-conf" ]; then
    echo "Copying Iceberg REST configuration files..."

    # Copy log4j2.properties for Iceberg REST if it exists
    if [ -f "/tmp/iceberg-rest-conf/log4j2.properties" ]; then
        cp /tmp/iceberg-rest-conf/log4j2.properties ${GRAVITINO_HOME}/iceberg-rest-server/conf/
        echo "Copied Iceberg REST log4j2.properties"
    fi

    # Copy Hadoop configuration files if they exist
    if [ -d "/tmp/iceberg-rest-conf/hadoop" ]; then
        mkdir -p ${GRAVITINO_HOME}/iceberg-rest-server/conf/hadoop
        cp -r /tmp/iceberg-rest-conf/hadoop/* ${GRAVITINO_HOME}/iceberg-rest-server/conf/hadoop/
        echo "Copied Iceberg REST Hadoop configuration files"
    fi
fi

echo "Start the Gravitino Server"
/bin/bash ${GRAVITINO_HOME}/bin/gravitino.sh run