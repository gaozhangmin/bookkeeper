#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from kconf.get_config import get_string_config
import os


default_kconf_prefix = 'infra.bookkeeperRocksdb.'
default_kconf_cluster = 'default_rocksdb_config'
rocksdb_config_file = '/home/web_server/bookkeeper/apps/conf/entry_location_rocksdb.conf'


def read_kconf(kconf_key):
    config = None
    try:
        config = get_string_config(kconf_key)
    except Exception as e:
        print(f'get config from kconf failed, kconf key:{kconf_key}, exception:{e}')
    return config


def write_file(file_path, config):
    try:
        with open(file_path, 'w') as f:
            f.write(config)
    except Exception as e:
        print(f'write config to file {file_path} failed, exception:{e}')
        sys.exit(1)


def get_cluster_name():
    ksn = os.environ['KWS_SERVICE_NAME']
    if not ksn:
        print('error: KWS_SERVICE_NAME environment variable is not set')
        sys.exit(1)
    return ksn.replace("infra-", "").replace('-', '_')

cluster_name = get_cluster_name()
if cluster_name.startswith('bop'):
    default_kconf_prefix = 'infra.BopBookkeeperRocksdb.'
print(f'cluster name:{cluster_name} default_kconf_prefix:{default_kconf_prefix}')

default_config = read_kconf(default_kconf_prefix + default_kconf_cluster)
if None is default_config or not default_config:
    print('error, default cluster config is empty')
    sys.exit(1)


real_cluster_config = default_config
cluster_config = read_kconf(default_kconf_prefix + cluster_name)
if None is cluster_config or not cluster_config:
    print(f'not get cluster {cluster_name} config, use default config')
else:
    real_cluster_config = cluster_config


print(f'cluster:{cluster_name} config: {real_cluster_config}')
write_file(rocksdb_config_file, real_cluster_config)
