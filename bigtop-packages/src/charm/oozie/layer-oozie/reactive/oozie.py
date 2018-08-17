# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from charmhelpers.core import hookenv
from charms.layer.bigtop_oozie import Oozie
from charms.reactive import is_state, set_state, when, when_not
from charms.reactive.helpers import data_changed
from charmhelpers.core.hookenv import unit_private_ip


@when('bigtop.available', 'hadoop.hdfs.ready')
@when_not('oozie.installed')
def install_oozie(hdfs):
    hookenv.status_set('maintenance', 'installing oozie')
    hosts = {}
    hosts['namenode'] = hdfs.namenodes()[0]
    oozie = Oozie()
    oozie.install_oozie(hosts)
    oozie.initial_oozie_config()
    set_state('oozie.installed')


@when('oozie.installed')
def check_config():
    mode = 'mapreduce' if is_state('hadoop.ready') else 'local'
    if data_changed('oozie.mode', mode):
        Oozie().update_config(mode)
        hookenv.status_set('active', 'ready (%s)' % mode)

@when('oozie.config.requested')
def ship_config(endpoint):
    endpoint.provide_config(unit_private_ip(), '11000')