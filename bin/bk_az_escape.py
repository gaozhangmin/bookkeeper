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

def parse_bookie_service_info_protobuf(data):
    try:
        properties = {}
        data_str = data.decode('utf-8', errors='ignore')

        # 查找 KWS_SERVICE_PAZ
        paz_key = "KWS_SERVICE_PAZ"
        if paz_key in data_str:
            paz_start = data_str.find(paz_key) + len(paz_key)
            # 跳过分隔符
            while paz_start < len(data_str) and not data_str[paz_start].isalnum():
                paz_start += 1

            paz_end = paz_start
            while paz_end < len(data_str) and data_str[paz_end].isalnum():
                paz_end += 1

            if paz_end > paz_start:
                properties[paz_key] = data_str[paz_start:paz_end]

        # 查找 KWS_SERVICE_REGION
        region_key = "KWS_SERVICE_REGION"
        if region_key in data_str:
            region_start = data_str.find(region_key) + len(region_key)
            # 跳过分隔符
            while region_start < len(data_str) and not data_str[region_start].isalnum():
                region_start += 1

            region_end = region_start
            while region_end < len(data_str) and data_str[region_end].isalnum():
                region_end += 1

            if region_end > region_start:
                properties[region_key] = data_str[region_start:region_end]

        return properties

    except Exception as e:
        print(f"解析 BookieServiceInfo 失败: {e}")
        return {}


def get_per_region_available_bookies(zk_url, zk_ledgers_root_path):
    """
    根据 KWS_SERVICE_PAZ 分类获取可用的bookie节点

    Args:
        zk_url: ZooKeeper连接地址
        zk_ledgers_root_path: ZK中ledgers的根路径

    Returns:
        dict: 按PAZ分类的bookie字典，格式如下:
        {
            'HB1AZ1': ['10.106.122.141:3181', '10.106.122.142:3181'],
            'HB1AZ2': ['10.106.122.145:3181'],
            'HB1AZ3': ['10.106.122.143:3181'],
            'unknown': ['10.106.122.144:3181']  # 无法解析PAZ的节点
        }
    """
    zk_client = KazooClient(hosts=zk_url)
    zk_client.start()

    bookies_by_paz = {}

    try:
        available_path = f"{zk_ledgers_root_path}/available"
        bookies = zk_client.get_children(available_path)

        # 过滤掉readonly节点
        if "readonly" in bookies:
            bookies.remove("readonly")

        print(f"发现 {len(bookies)} 个可用的bookie节点")

        for bookie in bookies:
            try:
                # 获取bookie节点的元数据
                bookie_path = f"{available_path}/{bookie}"
                data, stat = zk_client.get(bookie_path)

                if not data:
                    print(f"Bookie {bookie}: 无数据，归类为unknown")
                    if 'unknown' not in bookies_by_paz:
                        bookies_by_paz['unknown'] = []
                    bookies_by_paz['unknown'].append(bookie)
                    continue

                # 解析protobuf数据
                properties = parse_bookie_service_info_protobuf(data)

                paz = properties.get('KWS_SERVICE_PAZ')
                region = properties.get('KWS_SERVICE_REGION')

                if paz:
                    print(f"Bookie {bookie}: PAZ={paz}, Region={region}")
                    if paz not in bookies_by_paz:
                        bookies_by_paz[paz] = []
                    bookies_by_paz[paz].append(bookie)
                else:
                    print(f"Bookie {bookie}: 无法解析PAZ信息，归类为unknown")
                    if 'unknown' not in bookies_by_paz:
                        bookies_by_paz['unknown'] = []
                    bookies_by_paz['unknown'].append(bookie)

            except Exception as e:
                print(f"获取bookie {bookie} 元数据失败: {e}")
                if 'unknown' not in bookies_by_paz:
                    bookies_by_paz['unknown'] = []
                bookies_by_paz['unknown'].append(bookie)

        return bookies_by_paz

    except Exception as e:
        print(f"获取可用bookies失败: {e}")
        return {}
    finally:
        zk_client.stop()

def get_per_region_readonly_bookies(zk_url, zk_ledgers_root_path):
    """
    根据 KWS_SERVICE_PAZ 分类获取只读状态的bookie节点

    Args:
        zk_url: ZooKeeper连接地址
        zk_ledgers_root_path: ZK中ledgers的根路径

    Returns:
        dict: 按PAZ分类的只读bookie字典，格式如下:
        {
            'HB1AZ1': ['10.106.122.141:3181', '10.106.122.142:3181'],
            'HB1AZ2': ['10.106.122.145:3181'],
            'HB1AZ3': ['10.106.122.143:3181'],
            'unknown': ['10.106.122.144:3181']  # 无法解析PAZ的节点
        }
    """
    zk_client = KazooClient(hosts=zk_url)
    zk_client.start()

    bookies_by_paz = {}

    try:
        readonly_path = f"{zk_ledgers_root_path}/available/readonly"

        # 检查readonly节点是否存在
        if not zk_client.exists(readonly_path):
            print(f"只读节点路径不存在: {readonly_path}")
            return bookies_by_paz

        bookies = zk_client.get_children(readonly_path)
        print(f"发现 {len(bookies)} 个只读状态的bookie节点")

        for bookie in bookies:
            try:
                # 获取bookie节点的元数据
                bookie_path = f"{readonly_path}/{bookie}"
                data, stat = zk_client.get(bookie_path)

                if not data:
                    print(f"Readonly Bookie {bookie}: 无数据，归类为unknown")
                    if 'unknown' not in bookies_by_paz:
                        bookies_by_paz['unknown'] = []
                    bookies_by_paz['unknown'].append(bookie)
                    continue

                # 解析protobuf数据
                properties = parse_bookie_service_info_protobuf(data)

                paz = properties.get('KWS_SERVICE_PAZ')
                region = properties.get('KWS_SERVICE_REGION')

                if paz:
                    print(f"Readonly Bookie {bookie}: PAZ={paz}, Region={region}")
                    if paz not in bookies_by_paz:
                        bookies_by_paz[paz] = []
                    bookies_by_paz[paz].append(bookie)
                else:
                    print(f"Readonly Bookie {bookie}: 无法解析PAZ信息，归类为unknown")
                    if 'unknown' not in bookies_by_paz:
                        bookies_by_paz['unknown'] = []
                    bookies_by_paz['unknown'].append(bookie)

            except Exception as e:
                print(f"获取只读bookie {bookie} 元数据失败: {e}")
                if 'unknown' not in bookies_by_paz:
                    bookies_by_paz['unknown'] = []
                bookies_by_paz['unknown'].append(bookie)

        return bookies_by_paz

    except Exception as e:
        print(f"获取只读bookies失败: {e}")
        return {}
    finally:
        zk_client.stop()

def trigger_bookie_readonly(bookie_address):
    """
    将指定的bookie节点设置为只读模式

    Args:
        bookie_address: bookie地址，格式如 "10.106.122.141:3181"

    Returns:
        tuple: (success: bool, message: str)
    """
    if ':' in bookie_address:
        ip = bookie_address.split(':')[0]
    else:
        ip = bookie_address

    readonly_endpoint = f"http://{ip}:8989/api/v1/bookie/state/readonly"

    try:
        print(f"开始将 {bookie_address} 设置为只读模式...")

        # 发送PUT请求设置只读模式
        response = requests.put(
            readonly_endpoint,
            headers={'Content-Type': 'application/json'},
            json={"readOnly": True},
            timeout=30
        )

        if response.ok:
            print(f"只读模式设置成功: {bookie_address} - {response.text}")
            return True, response.text
        else:
            error_msg = f"只读模式设置失败: {bookie_address}, HTTP状态码: {response.status_code}, 响应: {response.text}"
            print(error_msg)
            return False, error_msg

    except requests.exceptions.Timeout:
        error_msg = f"只读模式设置超时: {bookie_address}"
        print(error_msg)
        return False, error_msg
    except requests.exceptions.ConnectionError:
        error_msg = f"连接失败: {bookie_address} (请检查节点是否可访问)"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"只读模式设置异常: {bookie_address}, 错误: {str(e)}"
        print(error_msg)
        return False, error_msg

def recover_bookie_from_readonly(bookie_address):
    """
    将指定的bookie节点从只读模式恢复为读写模式

    Args:
        bookie_address: bookie地址，格式如 "10.106.122.141:3181"

    Returns:
        tuple: (success: bool, message: str)
    """
    if ':' in bookie_address:
        ip = bookie_address.split(':')[0]
    else:
        ip = bookie_address

    readonly_endpoint = f"http://{ip}:8989/api/v1/bookie/state/readonly"

    try:
        print(f"开始将 {bookie_address} 从只读模式恢复为读写模式...")

        # 发送PUT请求取消只读模式
        response = requests.put(
            readonly_endpoint,
            headers={'Content-Type': 'application/json'},
            json={"readOnly": False},
            timeout=30
        )

        if response.ok:
            print(f"只读模式恢复成功: {bookie_address} - {response.text}")
            return True, response.text
        else:
            error_msg = f"只读模式恢复失败: {bookie_address}, HTTP状态码: {response.status_code}, 响应: {response.text}"
            print(error_msg)
            return False, error_msg

    except requests.exceptions.Timeout:
        error_msg = f"只读模式恢复超时: {bookie_address}"
        print(error_msg)
        return False, error_msg
    except requests.exceptions.ConnectionError:
        error_msg = f"连接失败: {bookie_address} (请检查节点是否可访问)"
        print(error_msg)
        return False, error_msg
    except Exception as e:
        error_msg = f"只读模式恢复异常: {bookie_address}, 错误: {str(e)}"
        print(error_msg)
        return False, error_msg

def check_bookie_readonly_status(bookie_address):
    """
    检查bookie节点的只读状态

    Args:
        bookie_address: bookie地址，格式如 "10.106.122.141:3181"

    Returns:
        tuple: (success: bool, is_readonly: bool, message: str)
    """
    if ':' in bookie_address:
        ip = bookie_address.split(':')[0]
    else:
        ip = bookie_address

    status_endpoint = f"http://{ip}:8989/api/v1/bookie/state/readonly"

    try:
        print(f"检查 {bookie_address} 只读状态...")

        response = requests.get(
            status_endpoint,
            timeout=10
        )

        if response.ok:
            try:
                data = response.json()
                is_readonly = data.get('readOnly', False)
                return True, is_readonly
            except json.JSONDecodeError:
                return False, False
        else:
            return False, False

    except requests.exceptions.Timeout:
        error_msg = f"状态检查超时: {bookie_address}"
        print(error_msg)
        return False, False, error_msg
    except requests.exceptions.ConnectionError:
        error_msg = f"连接失败: {bookie_address} (请检查节点是否可访问)"
        print(error_msg)
        return False, False, error_msg
    except Exception as e:
        error_msg = f"状态检查异常: {bookie_address}, 错误: {str(e)}"
        print(error_msg)
        return False, False, error_msg

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

def execute_bookkeeper_readonly(kconf_key, az, oncall_token="3c89e9b1-5c64-4741-bae9-c92920013161"):
    """
    将指定AZ下的所有BookKeeper节点设置为只读模式

    Args:
        kconf_key: 配置键名
        az: 可用区名称，如 "HB1AZ1"
        oncall_token: 告警通知token

    Returns:
        bool: 执行是否成功
    """
    cluster_name = kconf_key.split(".")[-1]

    try:
        # 1. 读取配置
        config = read_config(kconf_key)

        zk_servers = config.get('zkServers')
        zk_ledgers_root_path = config.get('zkLedgersRootPath')

        if not zk_servers or not zk_ledgers_root_path:
            error_msg = f"配置缺失: zkServers={zk_servers}, zkLedgersRootPath={zk_ledgers_root_path}"
            print(f"【BK ReadOnly告警】{cluster_name}: 执行失败，配置缺失: {error_msg}")
            return False

        # 2. 获取按AZ分类的bookie节点
        bookies_by_paz = get_per_region_available_bookies(zk_servers, zk_ledgers_root_path)

        if not bookies_by_paz:
            error_msg = "无法获取bookie节点信息"
            print(f"【BK ReadOnly告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        # 3. 检查指定AZ是否存在
        if az not in bookies_by_paz:
            available_azs = list(bookies_by_paz.keys())
            error_msg = f"指定的AZ '{az}' 不存在，可用的AZ: {available_azs}"
            print(f"【BK ReadOnly告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        target_bookies = bookies_by_paz[az]
        if not target_bookies:
            error_msg = f"AZ '{az}' 下没有可用的bookie节点"
            print(f"【BK ReadOnly告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        print(f"将对AZ '{az}' 下的 {len(target_bookies)} 个bookie节点执行只读模式设置")

        # 4. 检查并创建disable节点
        zk_client = KazooClient(hosts=zk_servers)
        zk_client.start()

        try:
            disable_path = f"{zk_ledgers_root_path}/disable"

            # 检查disable节点是否存在
            if not zk_client.exists(disable_path):
                print(f"disable节点不存在，正在创建: {disable_path}")
                # 创建disable节点
                zk_client.create(disable_path, value=b'', makepath=True)
                print(f"disable节点创建成功: {disable_path}")
            else:
                print(f"disable节点已存在: {disable_path}")

        except Exception as e:
            print(f"检查/创建disable节点失败: {e}")
            print(f"【BK ReadOnly告警】{cluster_name}: disable节点检查/创建失败: {str(e)}")
            return False
        finally:
            zk_client.stop()

        # 5. 对指定AZ下的每个BookKeeper节点设置只读模式
        success_count = 0
        failed_bookies = []
        readonly_verified_count = 0
        readonly_verification_failed = []

        for i, bookie in enumerate(target_bookies):
            # 设置只读模式
            success, result = trigger_bookie_readonly(bookie)

            if success:
                success_count += 1

                # 等待一下让设置生效
                time.sleep(2)

                # 检查只读状态
                status_success, is_readonly = check_bookie_readonly_status(bookie)
                if status_success and is_readonly:
                    readonly_verified_count += 1
                else:
                    readonly_verification_failed.append(bookie)
            else:
                failed_bookies.append((bookie, result))

            # 如果不是最后一个节点，等待5秒
            if i < len(target_bookies) - 1:
                time.sleep(5)

        # 6. 汇总结果
        total_bookies = len(target_bookies)
        failed_count = len(failed_bookies)
        verification_failed_count = len(readonly_verification_failed)

        summary_msg = (
            f"BK ReadOnly执行完成\\n"
            f"集群: {cluster_name}\\n"
            f"目标AZ: {az}\\n"
            f"总节点数: {total_bookies}\\n"
            f"设置成功: {success_count}\\n"
            f"状态验证成功: {readonly_verified_count}\\n"
            f"状态验证失败: {verification_failed_count}\\n"
            f"设置失败: {failed_count}"
        )

        if readonly_verification_failed:
            summary_msg += "\\n状态验证失败节点:\\n"
            for bookie in readonly_verification_failed:
                summary_msg += f"- {bookie}\\n"

        if failed_bookies:
            summary_msg += "\\n设置失败节点详情:\\n"
            for bookie, error in failed_bookies:
                summary_msg += f"- {bookie}: {error}\\n"

        # 发送汇总消息
        if failed_count == 0:
            print(f"【BK ReadOnly通知】{summary_msg}")
        else:
            print(f"【BK ReadOnly告警】{summary_msg}")

        return failed_count == 0

    except Exception as e:
        print(f"【BK ReadOnly告警】{cluster_name}: 执行异常: {str(e)}")
        return False

def execute_bookkeeper_recovery(kconf_key, az, oncall_token="3c89e9b1-5c64-4741-bae9-c92920013161"):
    """
    将指定AZ下的所有BookKeeper节点从只读模式恢复为读写模式

    Args:
        kconf_key: 配置键名
        az: 可用区名称，如 "HB1AZ1"
        oncall_token: 告警通知token

    Returns:
        bool: 执行是否成功
    """
    cluster_name = kconf_key.split(".")[-1]

    try:
        # 1. 读取配置
        config = read_config(kconf_key)

        zk_servers = config.get('zkServers')
        zk_ledgers_root_path = config.get('zkLedgersRootPath')

        if not zk_servers or not zk_ledgers_root_path:
            error_msg = f"配置缺失: zkServers={zk_servers}, zkLedgersRootPath={zk_ledgers_root_path}"
            print(f"【BK Recovery告警】{cluster_name}: 执行失败，配置缺失: {error_msg}")
            return False

        # 2. 获取按AZ分类的只读bookie节点
        bookies_by_paz = get_per_region_readonly_bookies(zk_servers, zk_ledgers_root_path)

        if not bookies_by_paz:
            error_msg = "无法获取只读bookie节点信息"
            print(f"【BK Recovery告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        # 3. 检查指定AZ是否存在
        if az not in bookies_by_paz:
            available_azs = list(bookies_by_paz.keys())
            error_msg = f"指定的AZ '{az}' 中没有只读bookie节点，存在只读节点的AZ: {available_azs}"
            print(f"【BK Recovery告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        target_bookies = bookies_by_paz[az]
        if not target_bookies:
            error_msg = f"AZ '{az}' 下没有只读状态的bookie节点"
            print(f"【BK Recovery告警】{cluster_name}: 执行失败，{error_msg}")
            return False

        print(f"将对AZ '{az}' 下的 {len(target_bookies)} 个只读bookie节点执行恢复操作")

        # 4. 对指定AZ下的每个BookKeeper节点恢复读写模式
        success_count = 0
        failed_bookies = []
        recovery_verified_count = 0
        recovery_verification_failed = []

        for i, bookie in enumerate(target_bookies):
            # 恢复读写模式
            success, result = recover_bookie_from_readonly(bookie)

            if success:
                success_count += 1

                # 等待一下让设置生效
                time.sleep(2)

                # 检查状态（应该不是只读模式）
                status_success, is_readonly = check_bookie_readonly_status(bookie)
                if status_success and not is_readonly:
                    recovery_verified_count += 1
                else:
                    recovery_verification_failed.append(bookie)
            else:
                failed_bookies.append((bookie, result))

            # 如果不是最后一个节点，等待5秒
            if i < len(target_bookies) - 1:
                time.sleep(5)

        # 6. 汇总结果
        total_bookies = len(target_bookies)
        failed_count = len(failed_bookies)
        verification_failed_count = len(recovery_verification_failed)

        summary_msg = (
            f"BK Recovery执行完成\\n"
            f"集群: {cluster_name}\\n"
            f"目标AZ: {az}\\n"
            f"总节点数: {total_bookies}\\n"
            f"恢复成功: {success_count}\\n"
            f"状态验证成功: {recovery_verified_count}\\n"
            f"状态验证失败: {verification_failed_count}\\n"
            f"恢复失败: {failed_count}"
        )

        if recovery_verification_failed:
            summary_msg += "\\n状态验证失败节点:\\n"
            for bookie in recovery_verification_failed:
                summary_msg += f"- {bookie}\\n"

        if failed_bookies:
            summary_msg += "\\n恢复失败节点详情:\\n"
            for bookie, error in failed_bookies:
                summary_msg += f"- {bookie}: {error}\\n"

        # 发送汇总消息
        if failed_count == 0:
            print(f"【BK Recovery通知】{summary_msg}")
        else:
            print(f"【BK Recovery告警】{summary_msg}")

        return failed_count == 0

    except Exception as e:
        print(f"【BK Recovery告警】{cluster_name}: 执行异常: {str(e)}")
        return False

# 使用示例
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("使用方法:")
        print("  设置只读: python bk_az_escape.py <集群名称> readonly <az>")
        print("  恢复读写: python bk_az_escape.py <集群名称> recovery <az>")
        print("  例如: python bk_az_escape.py kop_bk_e6_test readonly HB1AZ1")
        sys.exit(1)

    cluster_name = sys.argv[1]
    # 自动添加infra.bookkeeper.前缀
    kconf_key = f"infra.bookkeeper.{cluster_name}"
    action = sys.argv[2]

    print(f"集群名称: {cluster_name}")
    print(f"完整kconf_key: {kconf_key}")

    if len(sys.argv) == 4:
        az = sys.argv[3]

        if action == "readonly":
            # 执行只读模式设置
            print(f"对AZ '{az}' 执行BK ReadOnly操作...")
            success = execute_bookkeeper_readonly(kconf_key, az)

            if success:
                print(f"BK ReadOnly执行成功，AZ: {az}")
            else:
                print(f"BK ReadOnly执行失败，AZ: {az}，请查看日志")

        elif action == "recovery":
            # 执行恢复操作
            print(f"对AZ '{az}' 执行BK Recovery操作...")
            success = execute_bookkeeper_recovery(kconf_key, az)

            if success:
                print(f"BK Recovery执行成功，AZ: {az}")
            else:
                print(f"BK Recovery执行失败，AZ: {az}，请查看日志")
        else:
            print(f"未知的操作: {action}")
            print("使用方法:")
            print("  设置只读: python bk_az_escape.py <集群名称> readonly <az>")
            print("  恢复读写: python bk_az_escape.py <集群名称> recovery <az>")
            sys.exit(1)

    else:
        print("参数错误！")
        print("使用方法:")
        print("  设置只读: python bk_az_escape.py <集群名称> readonly <az>")
        print("  恢复读写: python bk_az_escape.py <集群名称> recovery <az>")
        sys.exit(1)