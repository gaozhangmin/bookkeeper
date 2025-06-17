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


def read_config(kconf_key):
    lines = get_string_config(kconf_key).splitlines()
    config_dict = {}
    for line in lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            key, value = line.split('=', 1)
            config_dict[key.strip()] = value.strip()
    return config_dict


ksn = os.environ['KWS_SERVICE_NAME']
if not ksn:
    print('error: KWS_SERVICE_NAME environment variable is not set')
    sys.exit(1)
cluster_name = ksn.replace("infra-", "").replace('-', '_')

default_cluster_config = read_config('infra.bookkeeper.default_cluster_config')
if None is default_cluster_config or not default_cluster_config:
    print('error, default cluster config is empty')
    sys.exit(1)
print(f'default config: {default_cluster_config}')

cluster_config = read_config(f'infra.bookkeeper.{cluster_name}')
if None is cluster_config or not cluster_config:
    print('error, current cluster config is empty')
    sys.exit(1)
print(f'current cluster config: {cluster_config}')


default_cluster_config.update(cluster_config)

print(f'new config: {default_cluster_config}')

if None is default_cluster_config or not default_cluster_config:
    print('error, new cluster config is empty')
    sys.exit(1)

bk_conf_path = '/home/web_server/bookkeeper/apps/conf/generated_bk_server.conf'
try:
    with open(bk_conf_path, 'w') as f:
        f.write('# =========== kop generated config ===========\n')
        for k, v in default_cluster_config.items():
            f.write(f'{k}={v}\n')
except FileNotFoundError:
    print(f'path not exist, path:{bk_conf_path}')
    sys.exit(1)
