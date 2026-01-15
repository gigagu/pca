#!/usr/bin/env python3
"""
工具：检查 HDFS 资源类型的 Ranger 策略冲突

该工具检查以下类型的冲突：
1. 一个策略拒绝所有用户/组（isDenyAllElse=True 或包含 isAllowed=False 的访问），
   另一个策略允许相同的用户/组访问相同的资源路径
2. 不同的策略配置了相同的资源路径和用户/组，但是权限不同
   （例如：策略1允许 read，策略2允许 write）
3. 一个策略允许某个用户/组的某个权限，另一个策略拒绝相同用户/组的相同权限

输出冲突的策略名称、资源路径、用户/组、权限信息。
"""
import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple, Any, Optional
from urllib.parse import urljoin

import requests
import urllib3
import yaml

# Disable SSL warnings for self-signed certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PathMatcher:
    """路径匹配器，用于检查路径是否重叠"""
    
    @staticmethod
    def paths_overlap(path1: str, path2: str, recursive1: bool = False, recursive2: bool = False) -> bool:
        """
        检查两个路径是否重叠
        
        Args:
            path1: 第一个路径
            path2: 第二个路径
            recursive1: 第一个路径是否递归
            recursive2: 第二个路径是否递归
            
        Returns:
            True 如果路径重叠
        """
        # 规范化路径（确保以 / 开头，移除尾部 /）
        def normalize(p: str) -> str:
            p = p.strip()
            if not p.startswith('/'):
                p = '/' + p
            if p != '/' and p.endswith('/'):
                p = p[:-1]
            return p
        
        p1 = normalize(path1)
        p2 = normalize(path2)
        
        # 完全相同的路径
        if p1 == p2:
            return True
        
        # 检查一个路径是否是另一个路径的前缀
        if recursive1 and p2.startswith(p1 + '/'):
            return True
        if recursive2 and p1.startswith(p2 + '/'):
            return True
        
        return False
    
    @staticmethod
    def path_matches(path: str, target_path: str, recursive: bool = False) -> bool:
        """
        检查路径是否匹配目标路径
        
        Args:
            path: 要检查的路径
            target_path: 目标路径
            recursive: 是否递归匹配
            
        Returns:
            True 如果路径匹配
        """
        return PathMatcher.paths_overlap(path, target_path, recursive, False)


class PolicyAnalyzer:
    """策略分析器，用于分析 Ranger 策略"""
    
    def __init__(self, ranger_base_url: str, username: str, password: str):
        self.ranger_base_url = ranger_base_url.rstrip('/')
        self.username = username
        self.password = password
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.verify = False
    
    def get_all_policies(self, service_type: str = "hdfs", service_name: str = None) -> List[Dict[str, Any]]:
        """
        从 Ranger API 获取所有指定类型的策略
        
        Args:
            service_type: 服务类型（默认：hdfs）
            service_name: 服务名称（可选，如果提供则使用服务特定的端点）
            
        Returns:
            策略列表
        """
        policies = []
        
        # 尝试多种 API 端点格式
        endpoints_to_try = []
        
        if service_name:
            # 使用服务名称的端点
            endpoints_to_try.append(f"/service/public/v2/api/service/{service_name}/policy")
        
        # 通用端点
        endpoints_to_try.extend([
            "/service/public/v2/api/policy",
            "/public/api/policy",
            "/service/plugins/policies"
        ])
        
        for endpoint in endpoints_to_try:
            url = urljoin(self.ranger_base_url, endpoint)
            try:
                params = {}
                if service_type and not service_name:
                    params["serviceType"] = service_type
                
                logger.debug(f"尝试从 {url} 获取策略...")
                response = self.session.get(url, params=params, timeout=30)
                response.raise_for_status()
                
                data = response.json()
                
                # 处理不同的响应格式
                if isinstance(data, list):
                    policies = data
                elif isinstance(data, dict):
                    if "vXPolicies" in data:
                        policies = data["vXPolicies"]
                    elif "policies" in data:
                        policies = data["policies"]
                    elif "policyList" in data:
                        policies = data["policyList"]
                    else:
                        # 可能是单个策略对象
                        policies = [data]
                
                # 过滤服务类型
                if service_type:
                    policies = [p for p in policies if p.get("serviceType", "").lower() == service_type.lower()]
                
                logger.info(f"从 {endpoint} 获取到 {len(policies)} 个 {service_type} 类型的策略")
                return policies
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 404:
                    logger.debug(f"端点 {endpoint} 不存在，尝试下一个...")
                    continue
                else:
                    logger.warning(f"从 {endpoint} 获取策略时出错: {e}")
                    continue
            except Exception as e:
                logger.debug(f"从 {endpoint} 获取策略时出错: {e}，尝试下一个...")
                continue
        
        # 如果所有端点都失败，抛出异常
        raise Exception(f"无法从任何端点获取策略。尝试的端点: {endpoints_to_try}")
    
    def extract_policy_info(self, policy: Dict[str, Any]) -> Dict[str, Any]:
        """
        从策略中提取关键信息
        
        Args:
            policy: 策略字典
            
        Returns:
            提取的策略信息
        """
        policy_name = policy.get("name", "unknown")
        service_type = policy.get("serviceType", "")
        is_enabled = policy.get("isEnabled", True)
        is_deny_all_else = policy.get("isDenyAllElse", False)
        
        # 提取资源路径
        resources = policy.get("resources", {})
        path_resource = resources.get("path", {})
        paths = path_resource.get("values", [])
        path_is_recursive = path_resource.get("isRecursive", False)
        path_is_excludes = path_resource.get("isExcludes", False)
        
        # 提取策略项（用户、组、访问权限）
        policy_items = policy.get("policyItems", [])
        deny_policy_items = policy.get("denyPolicyItems", [])
        
        # 分析允许的访问（保留详细信息）
        allowed_groups = set()
        allowed_users = set()
        allowed_accesses = []
        # 按用户/组组织的权限映射: {(user/group, access_type): True}
        allowed_permissions = {}  # {(entity_type, entity_name, access_type): True}
        
        for item in policy_items:
            groups = item.get("groups", [])
            users = item.get("users", [])
            accesses = item.get("accesses", [])
            
            for access in accesses:
                if access.get("isAllowed", True):
                    access_type = access.get("type", "")
                    allowed_groups.update(groups)
                    allowed_users.update(users)
                    allowed_accesses.append({
                        "type": access_type,
                        "groups": groups,
                        "users": users
                    })
                    # 记录每个组和用户的权限
                    for group in groups:
                        allowed_permissions[("group", group, access_type)] = True
                    for user in users:
                        allowed_permissions[("user", user, access_type)] = True
        
        # 分析拒绝的访问（保留详细信息）
        denied_groups = set()
        denied_users = set()
        denied_accesses = []
        denied_permissions = {}  # {(entity_type, entity_name, access_type): True}
        
        for item in deny_policy_items:
            groups = item.get("groups", [])
            users = item.get("users", [])
            accesses = item.get("accesses", [])
            
            for access in accesses:
                if not access.get("isAllowed", True):
                    access_type = access.get("type", "")
                    denied_groups.update(groups)
                    denied_users.update(users)
                    denied_accesses.append({
                        "type": access_type,
                        "groups": groups,
                        "users": users
                    })
                    # 记录每个组和用户的拒绝权限
                    for group in groups:
                        denied_permissions[("group", group, access_type)] = True
                    for user in users:
                        denied_permissions[("user", user, access_type)] = True
        
        # 检查是否有 isAllowed=False 的访问（在 policyItems 中）
        has_denied_access = False
        for item in policy_items:
            groups = item.get("groups", [])
            users = item.get("users", [])
            accesses = item.get("accesses", [])
            for access in accesses:
                if not access.get("isAllowed", True):
                    has_denied_access = True
                    access_type = access.get("type", "")
                    # 也记录这些拒绝的权限
                    for group in groups:
                        denied_permissions[("group", group, access_type)] = True
                    for user in users:
                        denied_permissions[("user", user, access_type)] = True
        
        return {
            "name": policy_name,
            "serviceType": service_type,
            "isEnabled": is_enabled,
            "isDenyAllElse": is_deny_all_else,
            "paths": paths,
            "pathIsRecursive": path_is_recursive,
            "pathIsExcludes": path_is_excludes,
            "allowedGroups": allowed_groups,
            "allowedUsers": allowed_users,
            "allowedAccesses": allowed_accesses,
            "allowedPermissions": allowed_permissions,  # 详细的权限映射
            "deniedGroups": denied_groups,
            "deniedUsers": denied_users,
            "deniedAccesses": denied_accesses,
            "deniedPermissions": denied_permissions,  # 详细的拒绝权限映射
            "hasDeniedAccess": has_denied_access
        }
    
    def check_conflicts(self, policies: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        检查策略之间的冲突
        
        Args:
            policies: 策略列表
            
        Returns:
            冲突列表
        """
        conflicts = []
        
        # 提取所有策略信息
        policy_infos = []
        for policy in policies:
            if not policy.get("isEnabled", True):
                continue
            
            try:
                info = self.extract_policy_info(policy)
                policy_infos.append(info)
            except Exception as e:
                logger.warning(f"提取策略 {policy.get('name', 'unknown')} 信息失败: {e}")
                continue
        
        # 检查每对策略之间的冲突
        for i, policy1 in enumerate(policy_infos):
            for j, policy2 in enumerate(policy_infos[i+1:], start=i+1):
                # 检查路径是否重叠
                path_conflicts = []
                for path1 in policy1["paths"]:
                    for path2 in policy2["paths"]:
                        if PathMatcher.paths_overlap(
                            path1, path2,
                            policy1["pathIsRecursive"],
                            policy2["pathIsRecursive"]
                        ):
                            path_conflicts.append((path1, path2))
                
                if not path_conflicts:
                    continue
                
                # 检查冲突类型 1: policy1 拒绝所有，policy2 允许某些组/用户
                if policy1["isDenyAllElse"] or policy1["hasDeniedAccess"]:
                    # 检查是否有共同的组或用户
                    common_groups = policy1["deniedGroups"] & policy2["allowedGroups"]
                    common_users = policy1["deniedUsers"] & policy2["allowedUsers"]
                    
                    # 或者如果 policy1 拒绝所有，policy2 允许任何组/用户
                    if policy1["isDenyAllElse"]:
                        if policy2["allowedGroups"] or policy2["allowedUsers"]:
                            conflicts.append({
                                "type": "deny_all_vs_allow",
                                "denyPolicy": policy1["name"],
                                "allowPolicy": policy2["name"],
                                "paths": path_conflicts,
                                "conflictingGroups": list(common_groups) if common_groups else list(policy2["allowedGroups"]),
                                "conflictingUsers": list(common_users) if common_users else list(policy2["allowedUsers"]),
                                "denyPolicyDetails": {
                                    "isDenyAllElse": policy1["isDenyAllElse"],
                                    "deniedGroups": list(policy1["deniedGroups"]),
                                    "deniedUsers": list(policy1["deniedUsers"])
                                },
                                "allowPolicyDetails": {
                                    "allowedGroups": list(policy2["allowedGroups"]),
                                    "allowedUsers": list(policy2["allowedUsers"])
                                }
                            })
                    elif common_groups or common_users:
                        conflicts.append({
                            "type": "deny_specific_vs_allow",
                            "denyPolicy": policy1["name"],
                            "allowPolicy": policy2["name"],
                            "paths": path_conflicts,
                            "conflictingGroups": list(common_groups),
                            "conflictingUsers": list(common_users),
                            "denyPolicyDetails": {
                                "deniedGroups": list(policy1["deniedGroups"]),
                                "deniedUsers": list(policy1["deniedUsers"])
                            },
                            "allowPolicyDetails": {
                                "allowedGroups": list(policy2["allowedGroups"]),
                                "allowedUsers": list(policy2["allowedUsers"])
                            }
                        })
                
                # 检查冲突类型 2: policy2 拒绝所有，policy1 允许某些组/用户
                if policy2["isDenyAllElse"] or policy2["hasDeniedAccess"]:
                    common_groups = policy2["deniedGroups"] & policy1["allowedGroups"]
                    common_users = policy2["deniedUsers"] & policy1["allowedUsers"]
                    
                    if policy2["isDenyAllElse"]:
                        if policy1["allowedGroups"] or policy1["allowedUsers"]:
                            conflicts.append({
                                "type": "deny_all_vs_allow",
                                "denyPolicy": policy2["name"],
                                "allowPolicy": policy1["name"],
                                "paths": path_conflicts,
                                "conflictingGroups": list(common_groups) if common_groups else list(policy1["allowedGroups"]),
                                "conflictingUsers": list(common_users) if common_users else list(policy1["allowedUsers"]),
                                "denyPolicyDetails": {
                                    "isDenyAllElse": policy2["isDenyAllElse"],
                                    "deniedGroups": list(policy2["deniedGroups"]),
                                    "deniedUsers": list(policy2["deniedUsers"])
                                },
                                "allowPolicyDetails": {
                                    "allowedGroups": list(policy1["allowedGroups"]),
                                    "allowedUsers": list(policy1["allowedUsers"])
                                }
                            })
                    elif common_groups or common_users:
                        conflicts.append({
                            "type": "deny_specific_vs_allow",
                            "denyPolicy": policy2["name"],
                            "allowPolicy": policy1["name"],
                            "paths": path_conflicts,
                            "conflictingGroups": list(common_groups),
                            "conflictingUsers": list(common_users),
                            "denyPolicyDetails": {
                                "deniedGroups": list(policy2["deniedGroups"]),
                                "deniedUsers": list(policy2["deniedUsers"])
                            },
                            "allowPolicyDetails": {
                                "allowedGroups": list(policy1["allowedGroups"]),
                                "allowedUsers": list(policy1["allowedUsers"])
                            }
                        })
                
                # 检查冲突类型 3: 相同路径 + 相同用户/组，但权限不同
                # 找出共同的用户和组
                common_groups = policy1["allowedGroups"] & policy2["allowedGroups"]
                common_users = policy1["allowedUsers"] & policy2["allowedUsers"]
                
                if common_groups or common_users:
                    # 检查权限差异
                    permission_conflicts = []
                    
                    # 检查组的权限差异
                    for group in common_groups:
                        # 获取两个策略对该组的权限
                        p1_perms = set()
                        p2_perms = set()
                        
                        for (entity_type, entity_name, access_type) in policy1["allowedPermissions"]:
                            if entity_type == "group" and entity_name == group:
                                p1_perms.add(access_type)
                        for (entity_type, entity_name, access_type) in policy2["allowedPermissions"]:
                            if entity_type == "group" and entity_name == group:
                                p2_perms.add(access_type)
                        
                        # 找出不同的权限
                        only_in_p1 = p1_perms - p2_perms
                        only_in_p2 = p2_perms - p1_perms
                        common_perms = p1_perms & p2_perms
                        
                        if only_in_p1 or only_in_p2:
                            permission_conflicts.append({
                                "entityType": "group",
                                "entityName": group,
                                "policy1Permissions": list(p1_perms),
                                "policy2Permissions": list(p2_perms),
                                "onlyInPolicy1": list(only_in_p1),
                                "onlyInPolicy2": list(only_in_p2),
                                "commonPermissions": list(common_perms)
                            })
                    
                    # 检查用户的权限差异
                    for user in common_users:
                        p1_perms = set()
                        p2_perms = set()
                        
                        for (entity_type, entity_name, access_type) in policy1["allowedPermissions"]:
                            if entity_type == "user" and entity_name == user:
                                p1_perms.add(access_type)
                        for (entity_type, entity_name, access_type) in policy2["allowedPermissions"]:
                            if entity_type == "user" and entity_name == user:
                                p2_perms.add(access_type)
                        
                        only_in_p1 = p1_perms - p2_perms
                        only_in_p2 = p2_perms - p1_perms
                        common_perms = p1_perms & p2_perms
                        
                        if only_in_p1 or only_in_p2:
                            permission_conflicts.append({
                                "entityType": "user",
                                "entityName": user,
                                "policy1Permissions": list(p1_perms),
                                "policy2Permissions": list(p2_perms),
                                "onlyInPolicy1": list(only_in_p1),
                                "onlyInPolicy2": list(only_in_p2),
                                "commonPermissions": list(common_perms)
                            })
                    
                    if permission_conflicts:
                        conflicts.append({
                            "type": "different_permissions",
                            "policy1": policy1["name"],
                            "policy2": policy2["name"],
                            "paths": path_conflicts,
                            "commonGroups": list(common_groups),
                            "commonUsers": list(common_users),
                            "permissionConflicts": permission_conflicts,
                            "policy1Details": {
                                "allowedGroups": list(policy1["allowedGroups"]),
                                "allowedUsers": list(policy1["allowedUsers"])
                            },
                            "policy2Details": {
                                "allowedGroups": list(policy2["allowedGroups"]),
                                "allowedUsers": list(policy2["allowedUsers"])
                            }
                        })
                
                # 检查冲突类型 4: 相同路径 + 相同用户/组，但一个允许一个拒绝相同权限
                # 检查 policy1 允许 vs policy2 拒绝
                for (entity_type1, entity_name1, access_type1) in policy1["allowedPermissions"]:
                    if (entity_type1, entity_name1, access_type1) in policy2["deniedPermissions"]:
                        conflicts.append({
                            "type": "allow_vs_deny_same_permission",
                            "allowPolicy": policy1["name"],
                            "denyPolicy": policy2["name"],
                            "paths": path_conflicts,
                            "entityType": entity_type1,
                            "entityName": entity_name1,
                            "permission": access_type1,
                            "allowPolicyDetails": {
                                "allowedGroups": list(policy1["allowedGroups"]),
                                "allowedUsers": list(policy1["allowedUsers"])
                            },
                            "denyPolicyDetails": {
                                "deniedGroups": list(policy2["deniedGroups"]),
                                "deniedUsers": list(policy2["deniedUsers"])
                            }
                        })
                
                # 检查 policy2 允许 vs policy1 拒绝
                for (entity_type2, entity_name2, access_type2) in policy2["allowedPermissions"]:
                    if (entity_type2, entity_name2, access_type2) in policy1["deniedPermissions"]:
                        conflicts.append({
                            "type": "allow_vs_deny_same_permission",
                            "allowPolicy": policy2["name"],
                            "denyPolicy": policy1["name"],
                            "paths": path_conflicts,
                            "entityType": entity_type2,
                            "entityName": entity_name2,
                            "permission": access_type2,
                            "allowPolicyDetails": {
                                "allowedGroups": list(policy2["allowedGroups"]),
                                "allowedUsers": list(policy2["allowedUsers"])
                            },
                            "denyPolicyDetails": {
                                "deniedGroups": list(policy1["deniedGroups"]),
                                "deniedUsers": list(policy1["deniedUsers"])
                            }
                        })
        
        return conflicts
    
    def print_conflicts(self, conflicts: List[Dict[str, Any]], output_format: str = "text"):
        """
        打印冲突信息
        
        Args:
            conflicts: 冲突列表
            output_format: 输出格式（text 或 json）
        """
        if output_format == "json":
            print(json.dumps(conflicts, indent=2, ensure_ascii=False))
            return
        
        if not conflicts:
            print("✓ 未发现策略冲突")
            return
        
        print(f"\n发现 {len(conflicts)} 个策略冲突:\n")
        print("=" * 80)
        
        for idx, conflict in enumerate(conflicts, 1):
            conflict_type = conflict.get('type', 'unknown')
            print(f"\n冲突 #{idx}: {conflict_type}")
            
            # 处理不同类型的冲突
            if conflict_type in ['deny_all_vs_allow', 'deny_specific_vs_allow']:
                print(f"  拒绝策略: {conflict.get('denyPolicy', 'unknown')}")
                print(f"  允许策略: {conflict.get('allowPolicy', 'unknown')}")
                print(f"  冲突路径:")
                for path1, path2 in conflict.get('paths', []):
                    print(f"    - {path1} (策略1)")
                    print(f"    - {path2} (策略2)")
                
                if conflict.get('conflictingGroups'):
                    print(f"  冲突的组: {', '.join(conflict['conflictingGroups'])}")
                if conflict.get('conflictingUsers'):
                    print(f"  冲突的用户: {', '.join(conflict['conflictingUsers'])}")
                
                print(f"  拒绝策略详情:")
                deny_details = conflict.get('denyPolicyDetails', {})
                if deny_details.get('isDenyAllElse'):
                    print(f"    - isDenyAllElse: True (拒绝所有)")
                if deny_details.get('deniedGroups'):
                    print(f"    - 拒绝的组: {', '.join(deny_details['deniedGroups'])}")
                if deny_details.get('deniedUsers'):
                    print(f"    - 拒绝的用户: {', '.join(deny_details['deniedUsers'])}")
                
                print(f"  允许策略详情:")
                allow_details = conflict.get('allowPolicyDetails', {})
                if allow_details.get('allowedGroups'):
                    print(f"    - 允许的组: {', '.join(allow_details['allowedGroups'])}")
                if allow_details.get('allowedUsers'):
                    print(f"    - 允许的用户: {', '.join(allow_details['allowedUsers'])}")
            
            elif conflict_type == 'different_permissions':
                print(f"  策略1: {conflict.get('policy1', 'unknown')}")
                print(f"  策略2: {conflict.get('policy2', 'unknown')}")
                print(f"  冲突路径:")
                for path1, path2 in conflict.get('paths', []):
                    print(f"    - {path1} (策略1)")
                    print(f"    - {path2} (策略2)")
                
                if conflict.get('commonGroups'):
                    print(f"  共同的组: {', '.join(conflict['commonGroups'])}")
                if conflict.get('commonUsers'):
                    print(f"  共同的用户: {', '.join(conflict['commonUsers'])}")
                
                print(f"  权限冲突详情:")
                for perm_conflict in conflict.get('permissionConflicts', []):
                    entity_type = perm_conflict.get('entityType', 'unknown')
                    entity_name = perm_conflict.get('entityName', 'unknown')
                    print(f"    {entity_type}: {entity_name}")
                    print(f"      策略1权限: {', '.join(perm_conflict.get('policy1Permissions', []))}")
                    print(f"      策略2权限: {', '.join(perm_conflict.get('policy2Permissions', []))}")
                    if perm_conflict.get('onlyInPolicy1'):
                        print(f"      仅在策略1中: {', '.join(perm_conflict['onlyInPolicy1'])}")
                    if perm_conflict.get('onlyInPolicy2'):
                        print(f"      仅在策略2中: {', '.join(perm_conflict['onlyInPolicy2'])}")
                    if perm_conflict.get('commonPermissions'):
                        print(f"      共同权限: {', '.join(perm_conflict['commonPermissions'])}")
                
                print(f"  策略1详情:")
                policy1_details = conflict.get('policy1Details', {})
                if policy1_details.get('allowedGroups'):
                    print(f"    - 允许的组: {', '.join(policy1_details['allowedGroups'])}")
                if policy1_details.get('allowedUsers'):
                    print(f"    - 允许的用户: {', '.join(policy1_details['allowedUsers'])}")
                
                print(f"  策略2详情:")
                policy2_details = conflict.get('policy2Details', {})
                if policy2_details.get('allowedGroups'):
                    print(f"    - 允许的组: {', '.join(policy2_details['allowedGroups'])}")
                if policy2_details.get('allowedUsers'):
                    print(f"    - 允许的用户: {', '.join(policy2_details['allowedUsers'])}")
            
            elif conflict_type == 'allow_vs_deny_same_permission':
                print(f"  允许策略: {conflict.get('allowPolicy', 'unknown')}")
                print(f"  拒绝策略: {conflict.get('denyPolicy', 'unknown')}")
                print(f"  冲突路径:")
                for path1, path2 in conflict.get('paths', []):
                    print(f"    - {path1} (策略1)")
                    print(f"    - {path2} (策略2)")
                
                entity_type = conflict.get('entityType', 'unknown')
                entity_name = conflict.get('entityName', 'unknown')
                permission = conflict.get('permission', 'unknown')
                print(f"  冲突的{entity_type}: {entity_name}")
                print(f"  冲突的权限: {permission}")
                print(f"  (策略 '{conflict.get('allowPolicy')}' 允许，但策略 '{conflict.get('denyPolicy')}' 拒绝)")
                
                print(f"  允许策略详情:")
                allow_details = conflict.get('allowPolicyDetails', {})
                if allow_details.get('allowedGroups'):
                    print(f"    - 允许的组: {', '.join(allow_details['allowedGroups'])}")
                if allow_details.get('allowedUsers'):
                    print(f"    - 允许的用户: {', '.join(allow_details['allowedUsers'])}")
                
                print(f"  拒绝策略详情:")
                deny_details = conflict.get('denyPolicyDetails', {})
                if deny_details.get('deniedGroups'):
                    print(f"    - 拒绝的组: {', '.join(deny_details['deniedGroups'])}")
                if deny_details.get('deniedUsers'):
                    print(f"    - 拒绝的用户: {', '.join(deny_details['deniedUsers'])}")
            
            print("-" * 80)


def main():
    parser = argparse.ArgumentParser(
        description="检查 HDFS 资源类型的 Ranger 策略冲突",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  # 使用环境变量配置
  export RANGER_USER=admin
  export RANGER_PASS=password
  python check_ranger_conflicts.py --env sit

  # 使用配置文件
  python check_ranger_conflicts.py --config config/sit.yml --ranger-url https://ranger.example.com

  # 输出 JSON 格式
  python check_ranger_conflicts.py --env sit --format json
        """
    )
    
    parser.add_argument(
        "--env",
        type=str,
        choices=["sit", "prod"],
        help="环境名称（sit 或 prod），用于加载配置文件"
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="配置文件路径（YAML 格式）"
    )
    parser.add_argument(
        "--ranger-url",
        type=str,
        help="Ranger 基础 URL（如果未提供，将从配置文件或环境变量读取）"
    )
    parser.add_argument(
        "--ranger-user",
        type=str,
        help="Ranger 用户名（如果未提供，将从环境变量 RANGER_USER 读取）"
    )
    parser.add_argument(
        "--ranger-pass",
        type=str,
        help="Ranger 密码（如果未提供，将从环境变量 RANGER_PASS 读取）"
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=["text", "json"],
        default="text",
        help="输出格式（默认：text）"
    )
    parser.add_argument(
        "--service-type",
        type=str,
        default="hdfs",
        help="服务类型（默认：hdfs）"
    )
    parser.add_argument(
        "--service-name",
        type=str,
        help="Ranger 服务名称（可选，如果提供则使用服务特定的 API 端点）"
    )
    
    args = parser.parse_args()
    
    # 获取 Ranger 配置
    ranger_base_url = args.ranger_url
    username = args.ranger_user
    password = args.ranger_pass
    
    # 从配置文件加载
    if args.config or args.env:
        if args.config:
            config_file = args.config
        else:
            config_dir = Path(__file__).parent.parent.parent / "config"
            config_file = config_dir / f"{args.env}.yml"
        
        if not config_file.exists():
            logger.error(f"配置文件不存在: {config_file}")
            sys.exit(1)
        
        with config_file.open("r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        
        if not ranger_base_url:
            ranger_base_url = config.get("env-configs", {}).get("RANGER_BASE_URL")
    
    # 从环境变量获取
    if not username:
        username = os.getenv("RANGER_USER")
    if not password:
        password = os.getenv("RANGER_PASS")
    
    # 验证必需的参数
    if not ranger_base_url:
        logger.error("Ranger 基础 URL 未提供。请使用 --ranger-url 或 --config/--env")
        sys.exit(1)
    
    if not username or not password:
        logger.error("Ranger 用户名或密码未提供。请使用 --ranger-user/--ranger-pass 或设置环境变量 RANGER_USER/RANGER_PASS")
        sys.exit(1)
    
    try:
        # 创建分析器
        analyzer = PolicyAnalyzer(ranger_base_url, username, password)
        
        # 获取所有策略
        logger.info(f"正在从 {ranger_base_url} 获取 {args.service_type} 类型的策略...")
        policies = analyzer.get_all_policies(service_type=args.service_type, service_name=args.service_name)
        
        if not policies:
            logger.warning("未找到任何策略")
            if args.format == "json":
                print("[]")
            else:
                print("未找到任何策略")
            return
        
        # 检查冲突
        logger.info("正在分析策略冲突...")
        conflicts = analyzer.check_conflicts(policies)
        
        # 输出结果
        analyzer.print_conflicts(conflicts, output_format=args.format)
        
        # 如果有冲突，返回非零退出码
        if conflicts:
            sys.exit(1)
        
    except Exception as e:
        logger.error(f"检查策略冲突时出错: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
