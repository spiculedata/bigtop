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

from subprocess import CalledProcessError, check_output

from charms.layer.apache_bigtop_base import Bigtop
from charms import layer
from charmhelpers.core import hookenv
from jujubigdata import utils


class Oozie(object):
    """
    This class manages Pig.
    """
    def __init__(self):
        self.dist_config = utils.DistConfig(
            data=layer.options('apache-bigtop-base'))

    def install_oozie(self, hosts):
        '''
        Trigger the Bigtop puppet recipe that handles the Oozie service.
        '''
        # Dirs are handled by the bigtop deb. No need to call out to
        # dist_config to do that work.
        roles = ['oozie-server', 'mapred-app']

        override = {
            'bigtop::jdk_preinstalled': False
        }
        bigtop = Bigtop()
        bigtop.render_site_yaml(hosts,roles,override)
        bigtop.trigger_puppet()
        check_output(['dpkg', '-i', '--force-overwrite', '/var/cache/apt/archives/oozie_4.3.0-1_all.deb'])
        bigtop.trigger_puppet()
        check_output(['tar', 'xvfz', '/usr/lib/oozie/oozie-sharelib.tar.gz', '-C', '/mnt'])
        check_output(['su', 'hdfs', '-c', '"hadoop fs -copyFromLocal /mnt/share/lib/* /user/oozie/share/lib/"'])
        check_output(['service', 'oozie', 'restart'])

    def initial_oozie_config(self):
        '''
        Configure system-wide pig bits.
        '''
        pig_bin = self.dist_config.path('oozie') / 'bin'
        with utils.environment_edit_in_place('/etc/environment') as env:
            if pig_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], pig_bin])
            env['PIG_CONF_DIR'] = self.dist_config.path('oozie_conf')
            env['PIG_HOME'] = self.dist_config.path('oozie')
            env['HADOOP_CONF_DIR'] = self.dist_config.path('hadoop_conf')

    def update_config(self, mode):
        """
        Configure Pig with the correct classpath.  If Hadoop is available, use
        HADOOP_CONF_DIR, otherwise use PIG_HOME.
        """
        with utils.environment_edit_in_place('/etc/environment') as env:
            key = 'HADOOP_CONF_DIR' if mode == 'mapreduce' else 'PIG_HOME'
            env['PIG_CLASSPATH'] = env[key]
