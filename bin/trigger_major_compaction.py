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

import argparse
import datetime
import json
import time
import os
import sys

import requests
from infra.scheduler import Task, TaskContext
from kazoo.client import KazooClient
from kconf.get_config import get_string_config


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

def get_available_bookies(zk_url, zk_ledgers_root_path):
  zk_client = KazooClient(hosts=zk_url)
  zk_client.start()
  try:
      available_path = f"{zk_ledgers_root_path}/available"
      bookies = zk_client.get_children(available_path)
      return bookies
  except Exception as e:
      return []
  finally:
      zk_client.stop()

def trigger_bookie_gc(bookie_address, gc_config):
  if ':' in bookie_address:
      ip = bookie_address.split(':')[0]
  else:
      ip = bookie_address

  gc_endpoint = f"http://{ip}:8989/api/v1/bookie/gc"

  try:
      print(f"开始对 {bookie_address} 执行GC操作...")

      response = requests.put(
          gc_endpoint,
          headers={'Content-Type': 'application/json'},
          json=gc_config,
          timeout=30
      )

      if response.ok:
          response_text = response.text
          expected_message = f"Triggered GC on BookieServer: {bookie_address}"

          if expected_message in response_text:
              print(f"GC执行成功: {bookie_address} - {response_text}")
              return True, response_text
          else:
              print(f"GC可能执行成功但返回消息不符合预期: {bookie_address} - {response_text}")
              return True, response_text
      else:
          error_msg = f"GC执行失败: {bookie_address}, HTTP状态码: {response.status_code}, 响应: {response.text}"
          print(error_msg)
          return False, error_msg

  except requests.exceptions.Timeout:
      error_msg = f"GC执行超时: {bookie_address}"
      print(error_msg)
      return False, error_msg
  except requests.exceptions.ConnectionError:
      error_msg = f"连接失败: {bookie_address} (请检查节点是否可访问)"
      print(error_msg)
      return False, error_msg
  except Exception as e:
      error_msg = f"GC执行异常: {bookie_address}, 错误: {str(e)}"
      print(error_msg)
      return False, error_msg

def send_oncall_group_message(msg: str, token: str, users='@all') -> bool:
  if isinstance(users, str):
      users = [users]
  data = {
      "msgtype": "markdown",
      "markdown": {
          "content": msg,
          "mentioned_list": users
      }
  }
  try:
      res = requests.post(
          f"http://kim-robot.internal/api/robot/send?key={token}",
          data=json.dumps(data),
          headers={'content-type': 'application/json'},
          timeout=10)
      if res.ok:
          print(f"kim 发送消息成功，status_code={res.status_code}, msg={res.text}")
          return True
      print(f"kim 发送消息失败，status_code={res.status_code}, msg={res.text}")
  except Exception as e:
      print(e)
  return False

def execute_bookkeeper_gc(kconf_key, oncall_token="3c89e9b1-5c64-4741-bae9-c92920013161"):
  clusterName = kconf_key.split(".")[-1]
  try:
      # 1. 读取配置
      config = read_config(kconf_key)

      zk_servers = config.get('zkServers')
      zk_ledgers_root_path = config.get('zkLedgersRootPath')

      if not zk_servers or not zk_ledgers_root_path:
          error_msg = f"配置缺失: zkServers={zk_servers}, zkLedgersRootPath={zk_ledgers_root_path}"
          send_oncall_group_message(
              f"【BK Compaction告警】{clusterName}: 执行失败，配置缺失: {error_msg}",
              token=oncall_token
          )
          return False


      bookies = get_available_bookies(zk_servers, zk_ledgers_root_path)
      bookies.remove("readonly")

      if not bookies:
          send_oncall_group_message(
              f"【BK Compaction告警】{clusterName}: 执行失败，{error_msg}",
              token=oncall_token
          )
          return False

      # 3. GC配置
      gc_config = {
          "forceMajor": True,
          "forceMinor": False,
          "majorCompactionThreshold": 0.9,
          "minorCompactionThreshold": 0.3,
          "majorCompactionMaxTimeMillis": 3600000,  # 1小时
          "minorCompactionMaxTimeMillis": 10000       # 10秒
      }

      # 4. 对每个BookKeeper节点执行GC
      success_count = 0
      failed_bookies = []

      for i, bookie in enumerate(bookies):
          if clusterName.strip() == 'kop_bk_hb1_reco1' and bookie not in ['10.88.42.78:3181', '10.57.224.80:3181', '10.50.119.205:3181', '10.108.51.45:3181']:
              continue
          if clusterName.strip() == 'kop_bk_hb1_reco2' and bookie not in ['10.53.160.80:3181']:
              continue
          success, result = trigger_bookie_gc(bookie, gc_config)

          if success:
              success_count += 1
          else:
              failed_bookies.append((bookie, result))

          # 如果不是最后一个节点，等待10秒
          if i < len(bookies) - 1:
              time.sleep(10)

      # 5. 汇总结果
      total_bookies = len(bookies)
      failed_count = len(failed_bookies)

      summary_msg = (
          f"BK Compaction执行完成\n"
          f"集群: {clusterName}\n"
          f"总节点数: {total_bookies}\n"
          f"成功: {success_count}\n"
          f"失败: {failed_count}"
      )

      if failed_bookies:
          summary_msg += "\n失败节点详情:\n"
          for bookie, error in failed_bookies:
              summary_msg += f"- {bookie}: {error}\n"


      # 发送汇总消息
      if failed_count == 0:
          send_oncall_group_message(
              f"【BK Compaction通知】{summary_msg}",
              token=oncall_token
          )
      else:
          send_oncall_group_message(
              f"【BK Compaction告警】{summary_msg}",
              token=oncall_token
          )

      return failed_count == 0

  except Exception as e:
      send_oncall_group_message(
          f"【BK Compaction告警】{clusterName}: 执行异常: {str(e)}\n",
          token=oncall_token
      )
      return False

# 使用示例
if __name__ == "__main__":

  # 执行GC操作
  #kconf_key = "infra.bookkeeper.kop_bk_e6_test"  # 替换为实际的配置键 dataarch-bjmt-e6-906.idchb1az4.hb1.kwaidc.com
  success = execute_bookkeeper_gc(sys.argv[1])

  if success:
      print("BK Compaction执行成功")
  else:
      print("BK Compaction执行失败，请查看日志")