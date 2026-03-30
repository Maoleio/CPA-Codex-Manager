"""
Cliproxy (CPA) 账号清理工具
"""

import asyncio
import logging
import time
import json
import random
import uuid
import urllib.parse
from typing import List, Optional, Dict, Any
from datetime import datetime
from pathlib import Path
import aiohttp
from fastapi import APIRouter, HTTPException, BackgroundTasks, Body
from pydantic import BaseModel

from ...database import crud
from ...database.session import get_db
from ..task_manager import task_manager

logger = logging.getLogger(__name__)
router = APIRouter()

# ---------------- Pydantic Models ----------------

class ScanRequest(BaseModel):
    service_id: int
    mode: str = "all"  # "401", "quota", "all"
    names: Optional[List[str]] = None  # 如果提供，则只检测勾选的账号
    target_type: str = "codex"
    workers: int = 30
    timeout: int = 15
    retries: int = 1
    weekly_threshold: float = 95.0
    primary_threshold: float = 95.0
    allow_disabled: bool = False

class AutoPatrolConfig(BaseModel):
    service_id: int
    enabled: bool = False
    interval_minutes: int = 60
    mode: str = "all"
    action_401: str = "none"  # "none", "delete"
    action_quota: str = "none"  # "none", "close", "delete"
    target_type: str = "codex"
    weekly_threshold: float = 95.0
    primary_threshold: float = 95.0
    
    # 紧急防御 (就绪率 < 阈值)
    emergency_defense: bool = True
    emergency_threshold: float = 0.5
    emergency_cooldown_minutes: int = 5
    
    # 自动扩容参数
    auto_replenish: bool = False
    replenish_threshold: int = 10
    replenish_count: int = 5
    replenish_email_service_id: Optional[int] = None
    replenish_reg_mode: str = "pipeline"
    replenish_concurrency: int = 3
    replenish_interval_min: int = 1
    replenish_interval_max: int = 3
    patrol_workers: int = 20
    patrol_timeout: int = 45
    startup_jitter_seconds: int = 8

class ActionRequest(BaseModel):
    service_id: int
    action: str  # "close" or "delete" or "enable"
    names: List[str]
    workers: int = 20
    timeout: int = 30

# ---------------- Helper Functions ----------------

def _normalize_mgmt_url(api_url: str) -> str:
    """规范化管理端地址"""
    normalized = (api_url or "").strip().rstrip("/")
    if not normalized: return ""
    if "/v0/management" in normalized: return normalized
    if normalized.endswith("/v0"): return f"{normalized}/management"
    if "/v0" not in normalized: return f"{normalized}/v0/management"
    return normalized

def _get_mgmt_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}

def _extract_chatgpt_account_id(item: dict) -> Optional[str]:
    for key in ("chatgpt_account_id", "chatgptAccountId", "account_id", "accountId"):
        val = item.get(key)
        if val: return val
    return None

def _contains_limit_error(text: str) -> bool:
    keywords = ["usage_limit_reached", "insufficient_quota", "quota_exceeded", "limit_reached", "rate limit"]
    lower_text = text.lower()
    return any(k in lower_text for k in keywords)


def _new_batch_id(prefix: str) -> str:
    """生成批量任务 ID，避免秒级时间戳导致不同任务发生复用/串线。"""
    return f"{prefix}_{uuid.uuid4().hex}"


def _mark_batch_failed(batch_id: str, reason: str, log_message: Optional[str] = None):
    """统一记录批次失败原因，便于自动巡检日志排查。"""
    if log_message:
        task_manager.add_batch_log(batch_id, log_message)
    task_manager.update_batch_status(batch_id, finished=True, status="failed", error=reason)

# ---------------- Core Logic ----------------

async def _run_bounded(items, limit, make_coro):
    limit = max(1, int(limit or 1))
    it = iter(items or [])
    running = set()
    out = []
    
    # 初始启动
    for _ in range(limit):
        try:
            item = next(it)
            # 平滑启动：增加 10-50ms 抖动，防止瞬间并发高峰拉满 CPU
            await asyncio.sleep(random.uniform(0.01, 0.05))
            running.add(asyncio.create_task(make_coro(item)))
        except StopIteration:
            break
            
    while running:
        done, running = await asyncio.wait(running, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            out.append(await task)
            try:
                item = next(it)
                # 喂入新任务时也带上微小抖动
                await asyncio.sleep(0.01)
                running.add(asyncio.create_task(make_coro(item)))
            except StopIteration:
                continue
    return out

async def perform_scan(batch_id: str, req: ScanRequest):
    """执行账号扫描流程，支持模式选择"""
    started_at = time.perf_counter()
    existing_status = task_manager.get_batch_status(batch_id) or {}
    current_mode = existing_status.get("mode") or "cliproxy_scan"
    current_monitor_visible = existing_status.get("monitor_visible", False)
    task_manager.init_batch(batch_id, total=0)
    task_manager.update_batch_status(
        batch_id,
        mode=current_mode,
        status="running",
        finished=False,
        monitor_visible=current_monitor_visible,
    )

    mode_cn = {"401": "401 检测", "quota": "额度检测", "all": "全量检测"}.get(req.mode, "全量检测")
    task_manager.add_batch_log(batch_id, f"[阶段] 开始执行 CPA {mode_cn} (并发: {req.workers})")

    service_lookup_started = time.perf_counter()
    with get_db() as db:
        service = crud.get_cpa_service_by_id(db, req.service_id)
        if not service:
            _mark_batch_failed(
                batch_id,
                reason=f"找不到指定的 CPA 服务 ID: {req.service_id}",
                log_message=f"[错误] 找不到指定的 CPA 服务 ID: {req.service_id}"
            )
            return
    service_lookup_cost = time.perf_counter() - service_lookup_started
    task_manager.add_batch_log(batch_id, f"[阶段] 已加载 CPA 服务配置，耗时 {service_lookup_cost:.2f}s")

    base_mgmt_url = _normalize_mgmt_url(service.api_url)
    api_token = service.api_token

    task_manager.add_batch_log(batch_id, f"[阶段] 正在从 {service.name} 拉取账号列表...")

    all_files = []
    list_fetch_started = time.perf_counter()

    async def _fetch_all_files(session: aiohttp.ClientSession):
        url = f"{base_mgmt_url}/auth-files"
        async with session.get(url, headers=_get_mgmt_headers(api_token)) as resp:
            if resp.status != 200:
                error_text = await resp.text()
                raise RuntimeError(f"获取列表失败 (HTTP {resp.status}): {error_text[:200]}")
            data = await resp.json()
            return data.get("files", [])

    async def _fetch_selected_files(session: aiohttp.ClientSession, names: List[str]):
        task_manager.add_batch_log(batch_id, f"[阶段] 检测到仅选中 {len(names)} 个账号，尝试走快速路径...")
        sem = asyncio.Semaphore(min(req.workers, max(1, len(names))))
        fetched = []
        fallback = False

        async def fetch_one(name: str):
            nonlocal fallback
            encoded_name = urllib.parse.quote(name, safe="")
            url = f"{base_mgmt_url}/auth-files?name={encoded_name}"
            async with sem:
                try:
                    async with session.get(url, headers=_get_mgmt_headers(api_token)) as resp:
                        if resp.status != 200:
                            fallback = True
                            error_text = await resp.text()
                            logger.warning(f"CPA 快速路径获取账号失败 {name}: HTTP {resp.status} {error_text[:200]}")
                            return []
                        data = await resp.json()
                        files = data.get("files")
                        if isinstance(files, list):
                            return files
                        if isinstance(data, dict):
                            if any(k in data for k in ("name", "auth_index", "type")):
                                return [data]
                            item = data.get("file")
                            if isinstance(item, dict):
                                return [item]
                        return []
                except Exception as e:
                    fallback = True
                    logger.warning(f"CPA 快速路径获取账号异常 {name}: {e}")
                    return []

        for chunk in await asyncio.gather(*(fetch_one(name) for name in names), return_exceptions=False):
            fetched.extend(chunk)

        if fallback:
            task_manager.add_batch_log(batch_id, "[阶段] 快速路径不可用，回退为全量账号列表同步...")
            return None

        deduped = []
        seen = set()
        for item in fetched:
            key = item.get("name") or item.get("auth_index") or item.get("email")
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)

        task_manager.add_batch_log(batch_id, f"[阶段] 快速路径命中完成，返回 {len(deduped)} 条账号记录")
        return deduped
    
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=req.timeout)) as session:
            if req.names:
                all_files = await _fetch_selected_files(session, req.names)
            if all_files is None or not req.names:
                all_files = await _fetch_all_files(session)
    except asyncio.TimeoutError:
        _mark_batch_failed(
            batch_id,
            reason=f"拉取账号列表超时（timeout={req.timeout}s）",
            log_message=f"[错误] 拉取账号列表超时，请检查 CPA 管理端响应速度或适当提高超时时间（当前 {req.timeout}s）"
        )
        return
    except Exception as e:
        _mark_batch_failed(
            batch_id,
            reason=f"拉取账号列表网络异常: {str(e)}",
            log_message=f"[错误] 网络连接异常: {str(e)}"
        )
        return

    list_fetch_cost = time.perf_counter() - list_fetch_started
    task_manager.add_batch_log(batch_id, f"[阶段] 账号列表拉取完成，共 {len(all_files)} 条，耗时 {list_fetch_cost:.2f}s")

    if not all_files:
        task_manager.init_batch(batch_id, total=0)
        _mark_batch_failed(
            batch_id,
            reason="拉取账号列表成功但返回为空",
            log_message="[错误] 无法获取文件列表：CPA 管理端返回空列表"
        )
        return

    # 过滤候选
    filter_started = time.perf_counter()
    task_manager.add_batch_log(batch_id, "[阶段] 正在过滤可检测账号...")
    candidates = []
    for f in all_files:
        if f.get("type") != req.target_type: continue
        if not req.allow_disabled and f.get("disabled"): continue
        if not f.get("auth_index"): continue
        
        # 核心修复：如果指定了名字，只保留勾选的名字
        if req.names and f.get("name") not in req.names:
            continue
            
        candidates.append(f)

    total = len(candidates)
    filter_cost = time.perf_counter() - filter_started
    prepare_cost = time.perf_counter() - started_at
    
    # 核心：在这里才正式初始化，确保 total 正确
    task_manager.init_batch(batch_id, total=total)

    task_manager.add_batch_log(batch_id, f"[阶段] 候选账号过滤完成，耗时 {filter_cost:.2f}s")
    task_manager.add_batch_log(batch_id, f"[阶段] 已准备完成，开始并发检测... (准备阶段总耗时 {prepare_cost:.2f}s)")
    task_manager.add_batch_log(batch_id, f"[信息] 识别到 {total} 个待检测候选账号" + (f" (已过滤，目标选中的 {len(req.names)} 个)" if req.names else ""))

    if total == 0:
        task_manager.add_batch_log(batch_id, "[完成] 没有符合条件的账号需要检测")
        task_manager.update_batch_status(batch_id, finished=True, status="completed")
        return

    # 2. 并发检测
    results = []
    completed = 0
    invalid_401 = 0
    invalid_quota = 0
    errors = 0
    progress_lock = asyncio.Lock()

    async def check_one(client_session, sem, item):
        nonlocal completed, invalid_401, invalid_quota, errors
        auth_index = item.get("auth_index")
        name = item.get("name")
        email = item.get("email") or item.get("account") or "unknown"
        
        res = {"name": name, "email": email, "status": "ok", "quota": None, "error": None}
        
        payload = {
            "authIndex": auth_index,
            "method": "GET",
            "url": "https://chatgpt.com/backend-api/wham/usage",
            "header": {
                "Authorization": "Bearer $TOKEN$",
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            }
        }
        acc_id = _extract_chatgpt_account_id(item)
        if acc_id: payload["header"]["Chatgpt-Account-Id"] = acc_id

        try:
            async with sem:
                # 只有在 mode 是 all 或 401 时才检测 401
                check_401 = req.mode in ("all", "401")
                # 只有在 mode 是 all 或 quota 时才记录状态为 ok
                check_quota = req.mode in ("all", "quota")

                # 如果只测 401，我们通过 wham/usage 的状态码逻辑其实是一样的。
                # 但如果只想节省资源，可以只看 status_code。
                async with client_session.post(
                    f"{base_mgmt_url}/api-call",
                    headers=_get_mgmt_headers(api_token),
                    json=payload,
                    timeout=req.timeout
                ) as resp:
                    if resp.status != 200:
                        res["status"] = "error"
                        res["error"] = f"HTTP {resp.status}"
                        errors += 1
                    else:
                        data = await resp.json()
                        sc = data.get("status_code")
                        if sc == 401:
                            res["status"] = "401"
                            invalid_401 += 1
                        elif sc == 200:
                            if req.mode == "401":
                                res["status"] = "ok" # 401 mode under 200 is ok
                            else:
                                # 解析额度
                                body = data.get("body")
                                if isinstance(body, str): body = json.loads(body)
                                
                                rate_limit = (body or {}).get("rate_limit", {})
                                usage = 0.0
                                limit_reached = False
                                
                                for win in rate_limit.values():
                                    if not isinstance(win, dict): continue
                                    p = win.get("used_percent")
                                    if p is not None: usage = max(usage, float(p))
                                    if win.get("limit_reached"): limit_reached = True
                                
                                res["quota"] = usage
                                if limit_reached or usage >= req.weekly_threshold or usage >= req.primary_threshold:
                                    res["status"] = "exhausted"
                                    invalid_quota += 1
                        else:
                            res["status"] = "error"
                            res["error"] = f"Code {sc}"
                            errors += 1
        except Exception as e:
            res["status"] = "error"
            res["error"] = str(e)
            errors += 1
            
        async with progress_lock:
            completed += 1
            ready = completed - invalid_401 - invalid_quota - errors
            task_manager.update_batch_status(
                batch_id,
                completed=completed,
                success=max(0, ready),
                failed=invalid_401 + invalid_quota + errors,
            )
            if completed % 10 == 0 or completed == total:
                task_manager.add_batch_log(
                    batch_id,
                    f"[进度] 已完成 {completed}/{total} | 有效: {max(0, ready)} | 401: {invalid_401} | 额度耗尽: {invalid_quota} | 异常: {errors}"
                )
        return res

    sem = asyncio.Semaphore(req.workers)
    connector = aiohttp.TCPConnector(limit=req.workers)
    async with aiohttp.ClientSession(connector=connector) as session:
        results = await _run_bounded(candidates, req.workers, lambda item: check_one(session, sem, item))

    # 3. 保存最后扫描结果到 batch 状态中，供前端展示
    ready_total = max(0, total - invalid_401 - invalid_quota - errors)
    task_manager.update_batch_status(
        batch_id,
        finished=True,
        status="completed",
        completed=total,
        success=ready_total,
        failed=invalid_401 + invalid_quota + errors,
        results=results,
        summary={
            "total": total,
            "ready": ready_total,
            "invalid_401": invalid_401,
            "invalid_quota": invalid_quota,
            "errors": errors
        }
    )
    total_cost = time.perf_counter() - started_at
    task_manager.add_batch_log(batch_id, f"[耗时] 全部扫描总耗时 {total_cost:.2f}s")
    task_manager.add_batch_log(batch_id, f"[完成] 扫描结束。有效: {total - invalid_401 - invalid_quota - errors}, 401: {invalid_401}, 额度耗尽: {invalid_quota}, 异常: {errors}")

async def perform_action(batch_id: str, req: ActionRequest):
    """执行批量关闭或删除动作"""
    action_cn = {"close": "关闭", "delete": "删除", "enable": "开启"}.get(req.action, req.action)
    task_manager.add_batch_log(batch_id, f"[系统] 开始执行批量 {action_cn} 任务 (并发: {req.workers}, 目标数: {len(req.names)})")
    
    with get_db() as db:
        service = crud.get_cpa_service_by_id(db, req.service_id)
        if not service:
            task_manager.add_batch_log(batch_id, f"[错误] 找不到指定的 CPA 服务 ID: {req.service_id}")
            task_manager.update_batch_status(batch_id, finished=True, status="failed")
            return

    base_mgmt_url = _normalize_mgmt_url(service.api_url)
    api_token = service.api_token
    total = len(req.names)
    task_manager.update_batch_status(batch_id, total=total)
    
    completed = 0
    success = 0
    failed = 0

    async def act_one(client_session, sem, name):
        nonlocal completed, success, failed
        try:
            async with sem:
                if req.action == "delete":
                    encoded_name = urllib.parse.quote(name, safe="")
                    url = f"{base_mgmt_url}/auth-files?name={encoded_name}"
                    async with client_session.delete(url, headers=_get_mgmt_headers(api_token)) as resp:
                        is_ok = resp.status == 200 or resp.status == 204
                else:
                    # close or enable
                    url = f"{base_mgmt_url}/auth-files/status"
                    disabled = (req.action == "close")
                    payload = {"name": name, "disabled": disabled}
                    async with client_session.patch(url, headers=_get_mgmt_headers(api_token), json=payload) as resp:
                        is_ok = resp.status == 200
                        
                if is_ok:
                    success += 1
                else:
                    failed += 1
        except Exception as e:
            failed += 1
            logger.error(f"操作 {name} 失败: {e}")
            
        completed += 1
        task_manager.update_batch_status(batch_id, completed=completed, success=success, failed=failed)
        if completed % 10 == 0 or completed == total:
             task_manager.add_batch_log(batch_id, f"[进度] 动作 {action_cn} 已完成 {completed}/{total} (成功: {success}, 失败: {failed})")

    sem = asyncio.Semaphore(req.workers)
    async with aiohttp.ClientSession() as session:
        await _run_bounded(req.names, req.workers, lambda name: act_one(session, sem, name))

    task_manager.update_batch_status(batch_id, finished=True, status="completed")
    task_manager.add_batch_log(batch_id, f"[完成] 批量 {action_cn} 任务结束。总计: {total}, 成功: {success}, 失败: {failed}")

# ---------------- Auto Patrol Manager ----------------

class AutoPatrolManager:
    """自动巡检管理器（按 CPA 服务独立配置/独立任务）"""
    def __init__(self):
        self._configs: Dict[int, AutoPatrolConfig] = {}
        self._tasks: Dict[int, asyncio.Task] = {}
        self._startup_tasks: Dict[int, asyncio.Task] = {}
        self._last_run_map: Dict[int, datetime] = {}
        self._status_map: Dict[int, str] = {}
        self._history_by_service: Dict[int, List[Dict[str, Any]]] = {}
        self._data_path = Path("data/cliproxy_patrol.json")
        self._load()

    def _get_service_name(self, service_id: int) -> str:
        try:
            with get_db() as db:
                service = crud.get_cpa_service_by_id(db, service_id)
                if service and getattr(service, "name", None):
                    return service.name
        except Exception as e:
            logger.warning(f"获取 CPA 服务名称失败 (ID: {service_id}): {e}")
        return f"CPA-{service_id}"

    def _load(self):
        """加载持久化配置"""
        if not self._data_path.exists():
            return
        try:
            with open(self._data_path, "r", encoding="utf-8") as f:
                data = json.load(f)

                # 新格式：按 service_id 分组保存
                if isinstance(data.get("configs"), dict):
                    for raw_service_id, raw_cfg in data.get("configs", {}).items():
                        try:
                            service_id = int(raw_service_id)
                            self._configs[service_id] = AutoPatrolConfig(**raw_cfg)
                        except Exception as inner_e:
                            logger.warning(f"加载巡检配置项失败 {raw_service_id}: {inner_e}")

                    raw_history_map = data.get("history_by_service", {}) or {}
                    for raw_service_id, items in raw_history_map.items():
                        try:
                            service_id = int(raw_service_id)
                            self._history_by_service[service_id] = items if isinstance(items, list) else []
                        except Exception as inner_e:
                            logger.warning(f"加载巡检历史项失败 {raw_service_id}: {inner_e}")
                    return

                # 兼容旧格式：单配置 + 单历史，迁移到对应 service_id 下
                if "config" in data and data["config"]:
                    cfg = AutoPatrolConfig(**data["config"])
                    self._configs[cfg.service_id] = cfg
                    self._history_by_service[cfg.service_id] = data.get("history", [])
        except Exception as e:
            logger.error(f"加载巡检配置失败: {e}")

    async def _delayed_start(self):
        await asyncio.sleep(5)
        for service_id, cfg in list(self._configs.items()):
            if cfg.enabled:
                self.start(service_id)

    async def _delayed_start_if_needed(self):
        """在事件循环就绪后按需延迟启动自动巡检。"""
        enabled_services = [service_id for service_id, cfg in self._configs.items() if cfg.enabled]
        if not enabled_services:
            logger.info("自动巡检未启用，跳过启动")
            return

        delay_seconds = 5 * 60
        logger.info(f"应用已就绪，{delay_seconds // 60} 分钟后尝试启动自动巡检: {enabled_services}")
        await asyncio.sleep(delay_seconds)
        for service_id in enabled_services:
            cfg = self._configs.get(service_id)
            if cfg and cfg.enabled:
                self.start(service_id)

    def _save(self):
        """保存持久化配置"""
        try:
            self._data_path.parent.mkdir(parents=True, exist_ok=True)
            data = {
                "configs": {str(service_id): cfg.dict() for service_id, cfg in self._configs.items()},
                "history_by_service": {str(service_id): history for service_id, history in self._history_by_service.items()}
            }
            with open(self._data_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存巡检配置失败: {e}")

    def update_config(self, config: AutoPatrolConfig):
        service_id = int(config.service_id)
        self._configs[service_id] = config
        self._history_by_service.setdefault(service_id, [])
        self._save()
        if config.enabled:
            self.start(service_id)
        else:
            self.stop(service_id)

    def get_status(self, service_id: Optional[int] = None) -> dict:
        if service_id is None:
            return {
                "status": "idle" if any(cfg.enabled for cfg in self._configs.values()) else "stopped",
                "config": None,
                "last_run": None,
                "next_run": None,
                "history": [],
            }

        cfg = self._configs.get(service_id)
        last_run = self._last_run_map.get(service_id)
        history = self._history_by_service.get(service_id, [])
        return {
            "status": self._status_map.get(service_id, "stopped"),
            "config": cfg.dict() if cfg else None,
            "last_run": last_run.isoformat() if last_run else None,
            "next_run": (last_run.timestamp() + cfg.interval_minutes * 60) if last_run and cfg else None,
            "history": history[:20]
        }

    def get_history(self, service_id: Optional[int] = None) -> List[Dict[str, Any]]:
        if service_id is None:
            merged: List[Dict[str, Any]] = []
            for items in self._history_by_service.values():
                merged.extend(items)
            return merged
        return self._history_by_service.get(service_id, [])

    def get_overview(self) -> List[Dict[str, Any]]:
        overview = []
        for service_id, cfg in self._configs.items():
            if not cfg.enabled:
                continue
            overview.append({
                "service_id": service_id,
                "service_name": self._get_service_name(service_id),
                "enabled": cfg.enabled,
                "status": self._status_map.get(service_id, "stopped"),
                "last_run": self._last_run_map.get(service_id).isoformat() if self._last_run_map.get(service_id) else None,
            })
        overview.sort(key=lambda item: item["service_name"])
        return overview

    def start(self, service_id: int):
        task = self._tasks.get(service_id)
        if task and not task.done():
            return
        self._status_map[service_id] = "running"
        self._tasks[service_id] = asyncio.create_task(self._loop(service_id))
        logger.info(f"自动巡检已启动: service_id={service_id}")

    def stop(self, service_id: Optional[int] = None):
        if service_id is None:
            for sid in list(self._tasks.keys()):
                self.stop(sid)
            return

        startup_task = self._startup_tasks.get(service_id)
        if startup_task and not startup_task.done():
            startup_task.cancel()
        self._startup_tasks.pop(service_id, None)

        task = self._tasks.get(service_id)
        if task:
            task.cancel()
        self._tasks.pop(service_id, None)
        self._status_map[service_id] = "stopped"
        logger.info(f"自动巡检已停止: service_id={service_id}")

    async def _loop(self, service_id: int):
        first_round = True
        while True:
            try:
                config = self._configs.get(service_id)
                if not config or not config.enabled:
                    self._status_map[service_id] = "stopped"
                    await asyncio.sleep(10)
                    continue

                if first_round:
                    jitter_max = max(0, int(getattr(config, 'startup_jitter_seconds', 8) or 0))
                    if jitter_max > 0:
                        jitter = min(jitter_max, service_id % (jitter_max + 1))
                        if jitter > 0:
                            logger.info(f"自动巡检首轮错峰等待 {jitter}s: service_id={service_id}")
                            await asyncio.sleep(jitter)
                    first_round = False

                self._status_map[service_id] = "running"
                self._last_run_map[service_id] = datetime.now()
                service_name = self._get_service_name(service_id)
                
                # 执行一次扫描
                batch_id = _new_batch_id("auto_patrol")
                task_manager.init_batch(batch_id, total=0, description=f"自动巡检 [{service_name}]")
                task_manager.update_batch_status(
                    batch_id,
                    mode="cliproxy_scan",
                    status="running",
                    finished=False,
                    monitor_visible=False,
                )
                task_manager.add_batch_log(batch_id, f"[自动巡检][{service_name}] 开始新一轮巡检，batch_id={batch_id}")
                logger.info(f"自动巡检开始首轮扫描: batch_id={batch_id}, service_id={service_id}, service_name={service_name}")

                scan_req = ScanRequest(
                    service_id=service_id,
                    mode=config.mode,
                    target_type=config.target_type,
                    weekly_threshold=config.weekly_threshold,
                    primary_threshold=config.primary_threshold,
                    allow_disabled=True,
                    workers=getattr(config, 'patrol_workers', 20),
                    timeout=max(15, int(getattr(config, 'patrol_timeout', 45) or 45)),
                )
                
                # 直接通过 perform_scan 执行，内部有 init_batch 和完成逻辑
                await perform_scan(batch_id, scan_req)
                
                # 获取结果并执行动作
                status = task_manager.get_batch_status(batch_id)
                if not status:
                    logger.error(f"自动巡检扫描结束后未找到批次状态: {batch_id}")
                    self._status_map[service_id] = "error"
                    await asyncio.sleep(60)
                    continue

                if status.get("status") != "completed":
                    logger.warning(
                        f"自动巡检扫描未成功完成: batch_id={batch_id}, status={status.get('status')}, "
                        f"error={status.get('error') or '-'}"
                    )

                if status and status.get("status") == "completed":
                    results = status.get("results", [])
                    names_401 = [r["name"] for r in results if r["status"] == "401"]
                    names_quota = [r["name"] for r in results if r["status"] == "exhausted"]
                    names_errors = [r["name"] for r in results if r["status"] == "error"]

                    sum_data = status.get("summary", {})
                    total_scanned = sum_data.get("total", 0)
                    ready_count = sum_data.get("ready", 0)

                    # 逻辑：如果就绪比例少于阈值，则跳过本轮清理并冷却后重试
                    threshold = getattr(config, 'emergency_threshold', 0.5)
                    cooldown = getattr(config, 'emergency_cooldown_minutes', 5)
                    if config.emergency_defense and total_scanned > 0 and (ready_count / total_scanned) < threshold:
                        msg = f"检测到有效账号占比过低 ({ready_count}/{total_scanned} < {int(threshold * 100)}%)，触发紧急防御: 本轮跳过清理，{cooldown} 分钟后重试"
                        logger.warning(msg)
                        
                        # 记录紧急防御到历史
                        history = self._history_by_service.setdefault(service_id, [])
                        history.insert(0, {
                            "service_id": service_id,
                            "service_name": service_name,
                            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "total": total_scanned,
                            "ready": ready_count,
                            "invalid_401": sum_data.get("invalid_401", 0),
                            "invalid_quota": sum_data.get("invalid_quota", 0),
                            "errors": sum_data.get("errors", 0),
                            "cleared": 0,
                            "threshold": threshold,
                            "cooldown_minutes": cooldown,
                            "emergency": True
                        })
                        if len(history) > 50: history.pop()
                        self._save()

                        logger.info(f"[{service_name}] 自动检测扫描异常结束 | 有效: {ready_count}, 异常触发: 紧急防御未执行清理，将在 {cooldown} 分钟后重新检测")
                        self._status_map[service_id] = "idle"
                        await asyncio.sleep(cooldown * 60)
                        continue
                    
                    # 执行 401 动作
                    if config.action_401 == "delete" and names_401:
                        action_batch_id = _new_batch_id("auto_action_401")
                        task_manager.init_batch(action_batch_id, total=len(names_401))
                        task_manager.update_batch_status(action_batch_id, mode="cliproxy_action", monitor_visible=False)
                        await perform_action(action_batch_id, ActionRequest(
                            service_id=service_id,
                            action="delete",
                            names=names_401
                        ))
                    
                    # 执行 Quota 动作
                    if config.action_quota in ("close", "delete") and names_quota:
                        action_batch_id = _new_batch_id("auto_action_quota")
                        task_manager.init_batch(action_batch_id, total=len(names_quota))
                        task_manager.update_batch_status(action_batch_id, mode="cliproxy_action", monitor_visible=False)
                        await perform_action(action_batch_id, ActionRequest(
                            service_id=service_id,
                            action=config.action_quota,
                            names=names_quota
                        ))

                    # 执行 Error 动作 (新要求：异常账号也清理)
                    if names_errors:
                        action_batch_id = _new_batch_id("auto_action_error")
                        task_manager.init_batch(action_batch_id, total=len(names_errors))
                        task_manager.update_batch_status(action_batch_id, mode="cliproxy_action", monitor_visible=False)
                        await perform_action(action_batch_id, ActionRequest(
                            service_id=service_id,
                            action="delete",
                            names=names_errors
                        ))
                    
                    # 检查是否需要自动补货
                    replenish_log = ""
                    replenish_info = None
                    if config.auto_replenish and ready_count < config.replenish_threshold:
                        # 提前获取详情用于日志
                        service_name = "tempmail"
                        try:
                            with get_db() as db:
                                if config.replenish_email_service_id:
                                    service = crud.get_email_service_by_id(db, config.replenish_email_service_id)
                                    if service: service_name = service.name
                        except: pass
                        
                        count = config.replenish_count
                        threads = config.replenish_concurrency if config.replenish_reg_mode == "parallel" else 1
                        replenish_log = f" | 触发自动补货: {threads}个线程执行[{service_name}] 补货 {count} 个账号"
                        replenish_info = {"method": service_name, "count": count, "threads": threads}
                        asyncio.create_task(self._trigger_replenish(service_id))

                    # 记录到持久化历史
                    cleared_count = (len(names_401) if config.action_401 == 'delete' else 0) + \
                                    (len(names_quota) if config.action_quota == 'delete' else 0) + \
                                    len(names_errors)
                    
                    history = self._history_by_service.setdefault(service_id, [])
                    history.insert(0, {
                        "service_id": service_id,
                        "service_name": self._get_service_name(service_id),
                        "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "total": sum_data.get("total", 0),
                        "invalid_401": sum_data.get("invalid_401", 0),
                        "invalid_quota": sum_data.get("invalid_quota", 0),
                        "errors": sum_data.get("errors", 0),
                        "cleared": cleared_count,
                        "replenish": replenish_info
                    })
                    if len(history) > 50: history.pop()
                    self._save() # 每次成功巡检后保存历史

                    final_msg = f"[{self._get_service_name(service_id)}] 自动检测扫描结束 | 有效: {ready_count}, 401: {len(names_401)}, 额度耗尽: {len(names_quota)}, 异常: {len(names_errors)} 已清理 {cleared_count} 个账号{replenish_log}"
                    logger.info(final_msg)

                self._status_map[service_id] = "idle"
                logger.info(f"自动巡检一轮结束，等待 {config.interval_minutes} 分钟: service_id={service_id}")
                await asyncio.sleep(config.interval_minutes * 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"自动巡检异常 (service_id={service_id}): {e}")
                self._status_map[service_id] = "error"
                await asyncio.sleep(60)

    async def _trigger_replenish(self, service_id: int):
        """执行自动补货逻辑"""
        try:
            from .registration import run_batch_registration, BatchRegistrationRequest
            import uuid
            config = self._configs.get(service_id)
            if not config:
                logger.warning(f"自动补货触发失败: 未找到巡检配置 (service_id={service_id})")
                return
            
            # 由于导入循环，我们需要动态获取
            # 获取邮箱服务详情以确认模式
            with get_db() as db:
                if config.replenish_email_service_id:
                    email_service = crud.get_email_service_by_id(db, config.replenish_email_service_id)
                    if not email_service:
                        logger.warning(f"自动巡检补货失败: 找不到配置的邮箱服务 (ID: {config.replenish_email_service_id})")
                        return
                    email_type = email_service.service_type
                else:
                    # 如果 ID 为空，默认为内置的 tempmail
                    email_type = "tempmail"

            mode_name = "并行" if config.replenish_reg_mode == "parallel" else "串行"
            batch_id = _new_batch_id("batch_auto")
            display_name = f"自动补货 [{self._get_service_name(service_id)}] ({mode_name})"
            count = config.replenish_count
            
            # 创建子任务 UUIDs
            task_uuids = []
            with get_db() as db:
                for _ in range(count):
                    t_uuid = str(uuid.uuid4())
                    crud.create_registration_task(db, task_uuid=t_uuid)
                    task_uuids.append(t_uuid)
            
            # 初始化批次，确保它出现在首页下拉框中
            # mode 设为 "registration" 这样首页的 stats 计算逻辑才能生效
            task_manager.init_batch(batch_id, total=count, description=display_name)
            task_manager.update_batch_status(batch_id, mode="registration", status="running")
            task_manager.add_batch_log(batch_id, f"[阶段] 触发 {display_name}: 数量={count}, Concurrency={config.replenish_concurrency}")
            
            logger.info(f"开启自动补货任务 {batch_id}: {display_name}")
            
            # 直接复用 registration.py 中的执行逻辑
            if config.replenish_reg_mode == "parallel":
                from .registration import run_batch_parallel
                asyncio.create_task(run_batch_parallel(
                    batch_id=batch_id,
                    task_uuids=task_uuids,
                    email_service_type=email_type,
                    proxy=None,
                    email_service_config=None,
                    email_service_id=config.replenish_email_service_id,
                    concurrency=config.replenish_concurrency,
                    auto_upload_cpa=True,
                    cpa_service_ids=[service_id],
                ))
            else:
                from .registration import run_batch_pipeline
                asyncio.create_task(run_batch_pipeline(
                    batch_id=batch_id,
                    task_uuids=task_uuids,
                    email_service_type=email_type,
                    proxy=None,
                    email_service_config=None,
                    email_service_id=config.replenish_email_service_id,
                    interval_min=config.replenish_interval_min,
                    interval_max=config.replenish_interval_max,
                    concurrency=config.replenish_concurrency,
                    auto_upload_cpa=True,
                    cpa_service_ids=[service_id],
                ))
        except Exception as e:
            logger.error(f"自动补货触发失败: {e}")

auto_patrol_manager = AutoPatrolManager()

# ---------------- API Endpoints ----------------

@router.post("/scan")
async def start_scan(request: ScanRequest, background_tasks: BackgroundTasks):
    batch_id = _new_batch_id("cliproxy_scan")
    task_manager.init_batch(batch_id, total=0)
    # CPA 检测任务只应在 CPA 页面内消费，不应复用首页注册任务监听/文案逻辑
    task_manager.update_batch_status(batch_id, mode="cliproxy_scan", monitor_visible=False)
    
    background_tasks.add_task(perform_scan, batch_id, request)
    return {"batch_id": batch_id, "message": "扫描任务已启动"}

@router.post("/action")
async def start_action(request: ActionRequest, background_tasks: BackgroundTasks):
    if not request.names:
        raise HTTPException(status_code=400, detail="未指定待处理的账号列表")
    
    batch_id = _new_batch_id("cliproxy_action")
    task_manager.init_batch(batch_id, total=len(request.names))
    # CPA 批量动作同样仅在 CPA 页面展示，避免被首页注册任务监控复用
    task_manager.update_batch_status(batch_id, mode="cliproxy_action", monitor_visible=False)
    
    background_tasks.add_task(perform_action, batch_id, request)
    return {"batch_id": batch_id, "message": "动作任务已启动"}

@router.get("/list")
async def list_accounts(service_id: int, target_type: str = "codex"):
    with get_db() as db:
        service = crud.get_cpa_service_by_id(db, service_id)
        if not service:
            raise HTTPException(status_code=404, detail="找不到指定的 CPA 服务")

    base_mgmt_url = _normalize_mgmt_url(service.api_url)
    api_token = service.api_token
    
    try:
        import requests as sync_requests
        url = f"{base_mgmt_url}/auth-files"
        resp = sync_requests.get(url, headers=_get_mgmt_headers(api_token), timeout=15)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="无法获取账号列表")
        
        data = resp.json()
        all_files = data.get("files", [])
        # 简单过滤，不执行检测
        candidates = [f for f in all_files if f.get("type") == target_type]
        return {"accounts": candidates}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/patrol/status")
async def get_patrol_status(service_id: Optional[int] = None):
    return auto_patrol_manager.get_status(service_id)

@router.get("/patrol/overview")
async def get_patrol_overview():
    return {"items": auto_patrol_manager.get_overview()}

@router.post("/patrol/config")
async def update_patrol_config(config: AutoPatrolConfig):
    auto_patrol_manager.update_config(config)
    return {"message": "巡检配置已更新"}

@router.get("/patrol/history")
async def get_patrol_history(service_id: Optional[int] = None):
    history = auto_patrol_manager.get_history(service_id)
    if service_id is not None:
        history = history[:20]
    return {"history": history}

@router.post("/patrol/test-replenish")
async def test_replenish(payload: Dict[str, Any] = Body(default={})): 
    """手动触发一次补货测试"""
    service_id = payload.get("service_id")
    if not service_id:
        raise HTTPException(status_code=400, detail="请先选择要测试补货的 CPA 服务")

    service_id = int(service_id)
    config = auto_patrol_manager._configs.get(service_id)
    if not config:
        raise HTTPException(status_code=400, detail="请先保存巡检配置后再测试")
    
    asyncio.create_task(auto_patrol_manager._trigger_replenish(service_id))
    return {"message": "补货测试任务已提交，请前往首页查看进度"}

@router.get("/batch/{batch_id}")
async def get_batch_status(batch_id: str):
    status = task_manager.get_batch_status(batch_id)
    if not status:
        raise HTTPException(status_code=404, detail="任务不存在")
    return status

@router.get("/batch/{batch_id}/logs")
async def get_batch_logs(batch_id: str):
    return {"logs": task_manager.get_batch_logs(batch_id)}
