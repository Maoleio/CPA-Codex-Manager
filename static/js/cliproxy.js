/**
 * CPA 检测工具前端逻辑 (V2.1)
 * 适配搜索功能、异常模式过滤及初始待检测状态
 */

document.addEventListener('DOMContentLoaded', () => {
    // ---------------- 全局变量 ----------------
    let currentBatchId = null;
    let scanResults = []; // 存储所有同步到的原始账号 [{name, email, status, quota, error}]
    let pollingInterval = null;
    let socket = null;
    let patrolStatus = { status: 'stopped' };
    let patrolOverview = [];
    let cpaServiceMap = {}; // 存储 ID -> 名称映射
    let currentFilter = 'all';
    let searchQuery = '';
    let currentActionNames = []; // 存储正在执行批量动作的账号名
    let latestPreparationMessage = '正在从 CPA 拉取账号列表...';
    let currentTaskType = null; // scan | action

    // ---------------- DOM 元素 ----------------
    const scanForm = document.getElementById('scan-form');
    const patrolForm = document.getElementById('patrol-form');
    const cpaServiceSelect = document.getElementById('cpa-service');
    const cpaServiceMain = document.getElementById('cpa-service-main');
    const patrolCpaServiceSelect = document.getElementById('patrol-cpa-service');
    const patrolEnabledToggle = document.getElementById('patrol-enabled-toggle');
    const replenishEmailSelect = document.getElementById('replenish-email-service');
    const consoleLog = document.getElementById('console-log');
    const resultsTableBody = document.getElementById('results-table-body');
    const masterCheckbox = document.getElementById('master-checkbox');
    const selectionActions = document.getElementById('selection-actions');
    const selectionInfo = document.getElementById('selection-info');
    const selectedCountBold = document.getElementById('selected-count-bold');
    const accountSearch = document.getElementById('account-search');

    const statTotal = document.getElementById('stat-total');
    const stat401 = document.getElementById('stat-401');
    const statExhausted = document.getElementById('stat-exhausted');
    const statOk = document.getElementById('stat-ok');

    const progressModal = document.getElementById('progress-modal');
    const progressBar = document.getElementById('modal-progress-bar');
    const progressStatus = document.getElementById('progress-status');
    const progressTitle = document.getElementById('progress-title');
    const patrolStatusLabel = document.getElementById('patrol-status-label');
    const patrolHistoryTitle = document.getElementById('patrol-history-title');

    loadCpaServices();
    loadReplenishEmailServices();
    loadPatrolOverview();
    loadPatrolStatus(true); // 初始加载需要回填表单
    loadPatrolHistory();

    // 仅自动刷新顶部巡检概览与运行状态，不自动刷新“检测通知”列表。
    // 否则移动端/桌面端在查看历史通知时会不断被重绘，造成“自动刷新”的观感。
    setInterval(() => {
        loadPatrolOverview();
        loadPatrolStatus(false); // 定时加载只更新运行状态文字，不回填表单、不刷新通知列表
    }, 5000);

    // ---------------- 导航/过滤/搜索 ----------------
    // 抽屉内标签页切换
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            const tab = btn.dataset.tab;
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.tab-content').forEach(c => c.style.display = 'none');
            document.getElementById(`${tab}-tab`).style.display = 'block';
        });
    });

    // 筛选按钮切换
    document.querySelectorAll('.filter-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            currentFilter = btn.dataset.filter;
            document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            renderResults();
        });
    });

    // 实时搜索功能
    accountSearch.addEventListener('input', debounce((e) => {
        searchQuery = e.target.value.toLowerCase().trim();
        renderResults();
    }, 400));

    // ---------------- 功能触发 ----------------
    window.startScan = async (mode) => {
        const formData = new FormData(scanForm);
        const data = Object.fromEntries(formData.entries());
        data.mode = mode;
        data.service_id = parseInt(data.service_id);
        data.weekly_threshold = parseFloat(data.weekly_threshold);
        data.primary_threshold = parseFloat(data.primary_threshold);
        data.allow_disabled = scanForm.allow_disabled.checked;

        // 获取当前勾选的名字
        const checked = resultsTableBody.querySelectorAll('.selection-checkbox:checked');
        if (checked.length > 0) {
            data.names = Array.from(checked).map(cb => cb.dataset.name);
        }

        if (!data.service_id) {
            toast.warning('请先设置 CPA 服务');
            toggleDrawer(true);
            return;
        }

        try {
            const targetText = data.names ? `选中的 ${data.names.length} 个账号` : '全部账号';
            currentTaskType = 'scan';
            showProgressModal(`执行 ${mode === 'all' ? '全量检测' : (mode === '401' ? '401 检测' : '额度检测')} (${targetText})...`);
            clearLogs();
            const res = await api.post('/cliproxy/scan', data);
            currentBatchId = res.batch_id;
            addLog('info', `[系统] 任务已启动: ${currentBatchId} | 模式: ${mode} | 目标: ${targetText}`);
            startPolling(currentBatchId, 'scan');
            connectWebSocket(currentBatchId);
        } catch (err) {
            toast.error(`启动失败: ${err.message}`);
        }
    };

    // ---------------- 手动检测存储与加载 ----------------
    const SCAN_CONFIG_KEY = 'cliproxy_scan_config';

    function saveScanConfig() {
        const formData = new FormData(scanForm);
        const config = Object.fromEntries(formData.entries());
        config.allow_disabled = scanForm.allow_disabled.checked;
        // 不存储 service_id，因为复载时会重新加载
        delete config.service_id;
        localStorage.setItem(SCAN_CONFIG_KEY, JSON.stringify(config));
        toast.show('手动检测配置已保存');
    }

    function loadScanConfig() {
        try {
            const saved = localStorage.getItem(SCAN_CONFIG_KEY);
            if (!saved) return;
            const config = JSON.parse(saved);
            if (config.target_type) scanForm.target_type.value = config.target_type;
            if (config.weekly_threshold) scanForm.weekly_threshold.value = config.weekly_threshold;
            if (config.primary_threshold) scanForm.primary_threshold.value = config.primary_threshold;
            if (config.allow_disabled !== undefined) scanForm.allow_disabled.checked = config.allow_disabled;
        } catch (e) { }
    }

    loadScanConfig();

    document.getElementById('save-scan-config-btn').addEventListener('click', saveScanConfig);

    // 巡检配置保存/开关
    document.getElementById('save-patrol-btn').addEventListener('click', async () => savePatrolConfig());
    document.getElementById('test-replenish-btn').addEventListener('click', async () => {
        try {
            const serviceId = parseInt(patrolCpaServiceSelect.value || cpaServiceMain.value || cpaServiceSelect.value);
            if (!serviceId) {
                toast.warning('请先选择一个 CPA 服务');
                return;
            }
            const data = await api.post('/cliproxy/patrol/test-replenish', { service_id: serviceId });
            toast.success(data.message);
            // 这里可以给个小提示，让用户去首页看
        } catch (err) {
            toast.error(err.message || '测试失败');
        }
    });
    /* 
    // 已由 toggleDrawer 代替，HTML 中无此按钮
    document.getElementById('toggle-patrol-btn').addEventListener('click', async () => {
        // 切换 enabled 状态
        patrolEnabledToggle.checked = !patrolEnabledToggle.checked;
        savePatrolConfig();
    });
    */

    async function savePatrolConfig() {
        const formData = new FormData(patrolForm);
        const config = {};
        formData.forEach((value, key) => {
            if (key === 'enabled' || key === 'auto_replenish') {
                config[key] = value === 'true' || value === 'on';
            } else if (['interval_minutes', 'patrol_workers', 'replenish_threshold', 'replenish_count', 'replenish_concurrency', 'replenish_interval_min', 'replenish_interval_max'].includes(key)) {
                config[key] = parseInt(value);
            } else if (key === 'replenish_email_service_id') {
                // 如果是 0，说明选了 tempmail，这在后台处理为 null
                config[key] = (value && value !== "0") ? parseInt(value) : null;
            } else {
                config[key] = value;
            }
        });

        // 显式处理 checkbox 可能缺失的情况
        config.enabled = patrolEnabledToggle.checked;
        config.emergency_defense = document.getElementById('patrol-emergency-toggle').checked;

        // 紧急防御数值处理
        const thresholdPct = parseInt(document.getElementById('emergency-threshold-pct').value) || 50;
        config.emergency_threshold = thresholdPct / 100;
        config.emergency_cooldown_minutes = parseInt(patrolForm.emergency_cooldown_minutes.value) || 5;

        config.auto_replenish = patrolForm.auto_replenish.checked;

        try {
            await api.post('/cliproxy/patrol/config', config);
            toast.show('巡检配置已更新');
            loadPatrolStatus();
        } catch (err) {
            toast.show('更新失败: ' + err.message, 'error');
        }
    }

    // ---------------- 列表同步逻辑 ----------------
    cpaServiceSelect.addEventListener('change', () => {
        cpaServiceMain.value = cpaServiceSelect.value;
        clearCurrentCpaView();
        fetchAccountList();
        syncPatrolSelectionFromMain();
    });
    cpaServiceMain.addEventListener('change', () => {
        cpaServiceSelect.value = cpaServiceMain.value;
        clearCurrentCpaView();
        fetchAccountList();
        syncPatrolSelectionFromMain();
    });
    patrolCpaServiceSelect.addEventListener('change', () => {
        clearPatrolHistoryForLoading();
        loadPatrolStatus(true);
        loadPatrolHistory();
    });

    // 开关状态同步到隐藏域
    patrolEnabledToggle.addEventListener('change', () => {
        document.getElementById('patrol-enabled-hidden').value = patrolEnabledToggle.checked;
    });

    // 将逻辑移到外部全局函数或确保在 DOM 里能访问


    scanForm.target_type.addEventListener('input', debounce(() => fetchAccountList(), 800));

    async function fetchAccountList() {
        const serviceId = parseInt(cpaServiceSelect.value);
        const targetType = scanForm.target_type.value || 'codex';
        if (!serviceId) return;

        try {
            clearAccountSummaryForLoading();
            resultsTableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 60px; color: var(--text-muted);">列表同步中...</td></tr>';
            const data = await api.get(`/cliproxy/list?service_id=${serviceId}&target_type=${targetType}`);
            // 初始同步进去的状态应该是 pending (待检测) 而不是全部在线
            scanResults = data.accounts.map(f => ({
                name: f.name,
                email: f.email || f.account || f.name,
                status: f.disabled ? 'error' : 'pending',
                quota: null,
                error: f.disabled ? '平台已禁用' : '初始导入，待检测'
            }));
            renderResults();
            updateStats();
            addLog('info', `[同步] 已从 CPA 加载 ${scanResults.length} 个账号元数据`);
        } catch (err) {
            clearAccountSummaryForLoading();
            addLog('error', `[同步失败] ${err.message}`);
            toast.error('同步账号列表失败');
        }
    }

    function clearAccountSummaryForLoading() {
        scanResults = [];
        statTotal.textContent = '-';
        stat401.textContent = '-';
        statExhausted.textContent = '-';
        statOk.textContent = '-';
        selectionInfo.textContent = '已选中 0 个';
        if (selectedCountBold) selectedCountBold.textContent = '0';
        selectionActions.style.display = 'none';
        masterCheckbox.checked = false;
    }

    function clearPatrolHistoryForLoading() {
        const serviceId = getCurrentPatrolServiceId();
        const serviceName = cpaServiceMap[serviceId] || `CPA-${serviceId || '-'}`;
        const container = document.getElementById('patrol-history-list');

        if (patrolHistoryTitle) {
            patrolHistoryTitle.textContent = `检测通知 [${serviceName}] (最近 20 条)`;
        }

        if (container) {
            container.innerHTML = `<div style="text-align: center; padding: 40px; color: var(--text-muted); font-size: 13px;">正在加载 [${serviceName}] 的检测通知...</div>`;
        }
    }

    function clearCurrentCpaView() {
        clearAccountSummaryForLoading();
        resultsTableBody.innerHTML = '<tr><td colspan="5" style="text-align: center; padding: 60px; color: var(--text-muted);">正在加载当前 CPA 的账号列表...</td></tr>';
        clearPatrolHistoryForLoading();
    }

    // ---------------- 列表渲染组件 ----------------
    function renderResults() {
        // 1. 先进行组合过滤
        let filtered = scanResults;

        // 搜索过滤
        if (searchQuery) {
            filtered = filtered.filter(r =>
                r.email.toLowerCase().includes(searchQuery) ||
                r.name.toLowerCase().includes(searchQuery)
            );
        }

        // 状态筛选
        if (currentFilter === '401') {
            filtered = filtered.filter(r => r.status === '401' || r.status === 'invalid_401');
        } else if (currentFilter === 'exhausted') {
            filtered = filtered.filter(r => r.status === 'exhausted');
        } else if (currentFilter === 'error') {
            filtered = filtered.filter(r => r.status === 'error');
        }

        // 2. 渲染 DOM
        if (filtered.length === 0) {
            resultsTableBody.innerHTML = `<tr><td colspan="5" style="text-align: center; padding: 60px; color: var(--text-muted); font-size: 13px;">${searchQuery ? '未找到匹配的账号。' : '当前条件下无数据。'}</td></tr>`;
            return;
        }

        resultsTableBody.innerHTML = filtered.map(res => `
            <tr>
                <td><input type="checkbox" class="selection-checkbox" data-name="${res.name}"></td>
                <td style="font-weight: 500;">${res.email}</td>
                <td><span class="status-pill ${res.status}">${mapStatus(res.status)}</span></td>
                <td>${res.quota !== null ? res.quota.toFixed(1) + '%' : '- %'}</td>
                <td class="text-secondary" style="font-size: 11px;">${res.error || (res.status === 'ok' ? '验证通过' : '-')}</td>
            </tr>
        `).join('');

        updateSelectionUI();
    }

    function mapStatus(raw) {
        const types = {
            'pending': '待检测',
            'ok': '在线',
            'exhausted': '额度空',
            '401': 'Token 401',
            'invalid_401': 'Token 401',
            'error': '检测异常'
        };
        return types[raw] || raw;
    }

    function updateStats() {
        if (!scanResults || scanResults.length === 0) {
            statTotal.textContent = '0';
            stat401.textContent = '0';
            statExhausted.textContent = '0';
            statOk.textContent = '0';
            return;
        }

        const summary = {
            total: scanResults.length,
            invalid_401: 0,
            invalid_quota: 0,
            errors: 0,
            ready: 0
        };

        scanResults.forEach(r => {
            if (r.status === '401' || r.status === 'invalid_401') summary.invalid_401++;
            else if (r.status === 'exhausted') summary.invalid_quota++;
            else if (r.status === 'error') summary.errors++;
            else if (r.status === 'ok') summary.ready++;
        });

        statTotal.textContent = summary.total;
        stat401.textContent = summary.invalid_401;
        statExhausted.textContent = summary.invalid_quota;
        statOk.textContent = summary.ready;
    }

    // ---------------- 批量选择控制 ----------------
    masterCheckbox.addEventListener('change', () => {
        resultsTableBody.querySelectorAll('.selection-checkbox').forEach(cb => cb.checked = masterCheckbox.checked);
        updateSelectionUI();
    });

    document.getElementById('select-all-btn').addEventListener('click', () => {
        resultsTableBody.querySelectorAll('.selection-checkbox').forEach(cb => cb.checked = true);
        updateSelectionUI();
    });

    document.getElementById('deselect-all-btn').addEventListener('click', () => {
        resultsTableBody.querySelectorAll('.selection-checkbox').forEach(cb => cb.checked = false);
        updateSelectionUI();
    });

    delegate(resultsTableBody, 'change', '.selection-checkbox', updateSelectionUI);

    // 行点击选择 (排除点击 checkbox 本身)
    delegate(resultsTableBody, 'click', 'tr', (e, target) => {
        if (e.target.tagName === 'INPUT' && e.target.type === 'checkbox') return;
        const cb = target.querySelector('.selection-checkbox');
        if (cb) {
            cb.checked = !cb.checked;
            updateSelectionUI();
        }
    });

    async function handleBatchDelete() {
        const checked = resultsTableBody.querySelectorAll('.selection-checkbox:checked');
        const names = Array.from(checked).map(cb => cb.dataset.name);
        if (names.length === 0) return;

        const confirmed = await confirm(`即将从 CPA 平台【永久删除】 ${names.length} 个账号。此操作不可撤销，确定执行吗？`);
        if (!confirmed) return;

        try {
            const serviceId = parseInt(document.getElementById('cpa-service').value);
            currentActionNames = names; // 暂存
            currentTaskType = 'action';
            const res = await api.post('/cliproxy/action', { service_id: serviceId, action: 'delete', names });
            showProgressModal(`执行批量删除操作...`);
            startPolling(res.batch_id, 'action');
            connectWebSocket(res.batch_id);
        } catch (err) {
            toast.error(`删除动作异常: ${err.message}`);
        }
    }

    function updateSelectionUI() {
        const all = resultsTableBody.querySelectorAll('.selection-checkbox');
        const checked = resultsTableBody.querySelectorAll('.selection-checkbox:checked');

        const count = checked.length;
        selectionInfo.textContent = `已选中 ${count} 个`;
        if (selectedCountBold) selectedCountBold.textContent = count;

        // 显示/隐藏底部操作条
        if (count > 0) {
            selectionActions.style.display = 'flex';
        } else {
            selectionActions.style.display = 'none';
        }

        masterCheckbox.checked = (all.length > 0 && checked.length === all.length);
    }

    // 批量执行动作 (仅支持删除)
    document.getElementById('batch-delete-btn').addEventListener('click', handleBatchDelete);

    // ---------------- 轮询与通知组件 ----------------
    async function loadCpaServices() {
        try {
            const services = await api.get('/cpa-services?enabled=true');
            cpaServiceMap = {}; // 清空并重填
            const options = services.map(s => {
                cpaServiceMap[s.id] = s.name;
                return `<option value="${s.id}">${s.name}</option>`;
            }).join('');
            cpaServiceSelect.innerHTML = options;
            cpaServiceMain.innerHTML = options;
            patrolCpaServiceSelect.innerHTML = options;
            if (services.length > 0) {
                const firstId = String(services[0].id);
                if (!cpaServiceSelect.value) cpaServiceSelect.value = firstId;
                if (!cpaServiceMain.value) cpaServiceMain.value = firstId;
                if (!patrolCpaServiceSelect.value) patrolCpaServiceSelect.value = firstId;
                fetchAccountList();
                loadPatrolOverview();
                loadPatrolStatus(true);
                loadPatrolHistory();
            }
        } catch (err) { }
    }

    function getCurrentPatrolServiceId() {
        return parseInt(patrolCpaServiceSelect.value || cpaServiceMain.value || cpaServiceSelect.value || 0);
    }

    function syncPatrolSelectionFromMain() {
        const currentId = cpaServiceMain.value || cpaServiceSelect.value;
        if (currentId) {
            patrolCpaServiceSelect.value = currentId;
            loadPatrolStatus(true);
            loadPatrolHistory();
        }
    }

    async function loadPatrolOverview() {
        try {
            const data = await api.get('/cliproxy/patrol/overview');
            patrolOverview = data.items || [];
            updatePatrolOverviewLabel();
        } catch (err) { }
    }

    function updatePatrolOverviewLabel() {
        if (!patrolStatusLabel) return;
        if (!patrolOverview.length) {
            patrolStatusLabel.textContent = '未运行';
            return;
        }

        const parts = patrolOverview.map(item => {
            const suffix = item.status === 'running' ? '巡检中' : '已开启';
            return `[${item.service_name}] ${suffix}`;
        });
        patrolStatusLabel.textContent = parts.join('\n');
    }

    async function loadReplenishEmailServices() {
        try {
            const data = await api.get('/registration/available-services');
            let options = '<option value="">请选择邮箱服务...</option>'; // 添加默认空项
            for (const type in data) {
                if (data[type].available) {
                    data[type].services.forEach(s => {
                        // 特殊处理 ID 为空的情况，比如 tempmail，给它一个 0
                        const val = s.id !== null ? s.id : 0;
                        options += `<option value="${val}">${s.name} (${type})</option>`;
                    });
                }
            }
            replenishEmailSelect.innerHTML = options;
            // 数据加载后再根据当前 patrolStatus 回填一次，由于异步可能后面才回来
            if (patrolStatus && patrolStatus.config && patrolStatus.config.replenish_email_service_id !== undefined) {
                replenishEmailSelect.value = patrolStatus.config.replenish_email_service_id || 0;
            }
        } catch (err) { }
    }

    async function loadPatrolStatus(isInitial = false) {
        try {
            const serviceId = getCurrentPatrolServiceId();
            patrolStatus = serviceId ? await api.get(`/cliproxy/patrol/status?service_id=${serviceId}`) : { status: 'stopped' };
            updatePatrolOverviewLabel();

            // 核心修复：只有在初始加载时，或者抽屉处于关闭状态且确定要刷新时，才回填表单
            // 防止用户正在修改配置时，被 5 秒一次的定时器强行重置回旧状态
            if (isInitial) {
                const selectedId = serviceId ? String(serviceId) : '';
                if (selectedId) patrolCpaServiceSelect.value = selectedId;
            }

            if (isInitial && patrolStatus.config) {
                const cfg = patrolStatus.config;
                // 手动填充表单
                Object.keys(cfg).forEach(key => {
                    const el = patrolForm.querySelector(`[name="${key}"]`);
                    if (el) {
                        if (el.type === 'checkbox') el.checked = !!cfg[key];
                        else {
                            if (el.tagName === 'SELECT' && el.options.length <= 1 && key === 'replenish_email_service_id') {
                                // 暂存给异步加载
                            } else {
                                el.value = cfg[key] === null ? (key === 'replenish_email_service_id' ? "0" : "") : cfg[key];
                            }
                        }
                    }
                });

                patrolEnabledToggle.checked = !!cfg.enabled;
                const emergencyToggle = document.getElementById('patrol-emergency-toggle');
                if (emergencyToggle) {
                    emergencyToggle.checked = !!cfg.emergency_defense;
                    // 回填数值
                    const thresholdPct = Math.round((cfg.emergency_threshold || 0.5) * 100);
                    document.getElementById('emergency-threshold-pct').value = thresholdPct;
                    patrolForm.emergency_cooldown_minutes.value = cfg.emergency_cooldown_minutes || 5;
                    toggleEmergencyConfigVisibility();
                }
                if (patrolForm.auto_replenish) patrolForm.auto_replenish.checked = !!cfg.auto_replenish;

                // 触发一次显隐切换
                toggleReplenishVisibility();
            } else if (isInitial && !patrolStatus.config) {
                patrolEnabledToggle.checked = false;
                const hidden = document.getElementById('patrol-enabled-hidden');
                if (hidden) hidden.value = 'false';
                const emergencyToggle = document.getElementById('patrol-emergency-toggle');
                if (emergencyToggle) emergencyToggle.checked = true;
                document.getElementById('emergency-threshold-pct').value = 50;
                if (patrolForm.auto_replenish) patrolForm.auto_replenish.checked = false;
                toggleEmergencyConfigVisibility();
                toggleReplenishVisibility();
            }
        } catch (err) { }
    }

    async function loadPatrolHistory() {
        try {
            const serviceId = getCurrentPatrolServiceId();
            const serviceName = cpaServiceMap[serviceId] || `CPA-${serviceId || '-'}`;
            const container = document.getElementById('patrol-history-list');

            clearPatrolHistoryForLoading();

            const data = await api.get(`/cliproxy/patrol/history?service_id=${serviceId}`);
            const history = data.history || [];

            if (history.length === 0) {
                container.innerHTML = '<div style="text-align: center; padding: 40px; color: var(--text-muted); font-size: 13px;">暂无检测记录...</div>';
                return;
            }

            container.innerHTML = history.map(item => {
                let statusInfo = '';
                if (item.emergency) {
                    const readyCount = item.ready ?? Math.max(0, (item.total || 0) - (item.invalid_401 || 0) - (item.invalid_quota || 0) - (item.errors || 0));
                    const cooldown = item.cooldown_minutes || 5;
                    statusInfo = `<span class="notif-stats" style="color: #FF9500;">触发紧急防御 | 本轮未清理 | 就绪: ${readyCount}/${item.total || 0} | 将在 ${cooldown} 分钟后重试</span>`;
                } else {
                    statusInfo = `
                        <span class="notif-stats">
                            有效: ${item.total - item.invalid_401 - item.invalid_quota - item.errors}, 
                            401: ${item.invalid_401}, 
                            额度耗尽: ${item.invalid_quota}, 
                            异常: ${item.errors}
                        </span>
                        ${item.cleared > 0 ? `<span class="notif-cleared">已清理 ${item.cleared} 个账号</span>` : ''}
                    `;
                }

                let replenishMsg = '';
                if (item.replenish) {
                    const threadInfo = item.replenish.threads ? `${item.replenish.threads}个线程执行` : '';
                    replenishMsg = `<div style="font-size: 11px; color: var(--accent); margin-top: 4px;">触发自动补货: ${threadInfo}[${item.replenish.method}] 补货 ${item.replenish.count} 个</div>`;
                }

                return `
                    <div class="notification-item">
                        <div class="notif-dot"></div>
                        <div class="notif-content">
                            <div class="notif-time">${item.time}</div>
                            <div class="notif-text">
                                [${item.service_name || serviceName}] ${item.emergency ? '自动检测扫描异常结束' : '自动检测扫描结束'} | 
                                ${statusInfo}
                                ${replenishMsg}
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        } catch (err) {
            const serviceId = getCurrentPatrolServiceId();
            const serviceName = cpaServiceMap[serviceId] || `CPA-${serviceId || '-'}`;
            const container = document.getElementById('patrol-history-list');
            if (container) {
                container.innerHTML = `<div style="text-align: center; padding: 40px; color: var(--text-muted); font-size: 13px;">[${serviceName}] 的检测通知加载失败</div>`;
            }
        }
    }

    function addLog(type, message) {
        const line = document.createElement('div');
        line.className = `log-line ${type}`;
        line.innerHTML = `<span class="log-time">${new Date().toLocaleTimeString()}</span> <span class="log-content">${message}</span>`;
        consoleLog.appendChild(line);
        consoleLog.scrollTop = consoleLog.scrollHeight;

        syncPreparationStatusFromLog(message);
    }

    function clearLogs() { consoleLog.innerHTML = ''; }

    function syncPreparationStatusFromLog(message) {
        if (!message) return;

        if (message.includes('拉取账号列表') || message.includes('获取认证文件列表') || message.includes('快速路径不可用')) {
            latestPreparationMessage = '正在从 CPA 拉取账号列表...';
        } else if (message.includes('过滤可检测账号') || message.includes('识别到')) {
            latestPreparationMessage = '正在过滤可检测账号...';
        } else if (message.includes('已准备完成，开始并发检测')) {
            latestPreparationMessage = '已准备完成，开始并发检测...';
        }

        const currentText = progressStatus.textContent || '';
        if (currentText.includes('准备') || currentText.includes('同步') || currentText.includes('过滤')) {
            progressStatus.textContent = latestPreparationMessage;
        }

        const bgProgress = document.getElementById('background-task-progress');
        if (bgProgress) {
            const bgCurrent = bgProgress.textContent || '';
            if (bgCurrent.includes('同步') || bgCurrent.includes('准备') || bgCurrent.includes('过滤')) {
                bgProgress.textContent = latestPreparationMessage;
            }
        }
    }

    function showProgressModal(title) {
        progressTitle.textContent = title;
        progressBar.style.width = '0%';
        latestPreparationMessage = '正在从 CPA 拉取账号列表...';
        progressStatus.textContent = latestPreparationMessage;
        progressModal.classList.add('active');
    }

    function buildScanSummary(status) {
        const summary = status.summary || {};
        const total = summary.total ?? status.total ?? 0;
        const invalid401 = summary.invalid_401 ?? 0;
        const exhausted = summary.invalid_quota ?? 0;
        const errors = summary.errors ?? 0;
        const ready = summary.ready ?? Math.max(0, total - invalid401 - exhausted - errors);
        return `CPA 检测完成：总数 ${total}，就绪 ${ready}，401 ${invalid401}，额度耗尽 ${exhausted}，异常 ${errors}`;
    }

    function buildActionSummary(status) {
        const total = status.total || 0;
        const success = status.success || 0;
        const failed = status.failed || 0;
        return `CPA 批量操作完成：总数 ${total}，成功 ${success}，失败 ${failed}`;
    }

    function startPolling(batchId, type) {
        if (pollingInterval) clearInterval(pollingInterval);
        pollingInterval = setInterval(async () => {
            try {
                const status = await api.get(`/cliproxy/batch/${batchId}`);
                const total = status.total || 0;
                const completed = status.completed || 0;
                const percent = total > 0 ? Math.round((completed / total) * 100) : 0;

                // 更新模态框进度
                progressBar.style.width = `${percent}%`;
                progressStatus.textContent = total > 0 ? `任务进度: ${completed} / ${total}` : latestPreparationMessage;

                // 更新后台任务栏进度 (如果有)
                const bgTitle = document.getElementById('background-task-title');
                const bgProgress = document.getElementById('background-task-progress');
                const bgBar = document.getElementById('background-task-progress-bar');
                const bgState = document.getElementById('background-task-state');

                if (bgTitle) bgTitle.textContent = progressTitle.textContent;
                if (bgProgress) bgProgress.textContent = total > 0 ? `任务进度: ${completed} / ${total}` : latestPreparationMessage;
                if (bgBar) bgBar.style.width = `${percent}%`;
                if (bgState) {
                    bgState.textContent = status.finished ? '已结束' : '进行中';
                    bgState.className = `status-pill ${status.finished ? 'ok' : 'pending'}`;
                }

                if (status.finished) {
                    stopPolling();
                    progressModal.classList.remove('active');
                    if (type === 'scan' && status.results) {
                        // 核心修复：只更新被检测的账号，不覆盖全量
                        const newResultsMap = new Map(status.results.map(r => [r.name, r]));
                        scanResults = scanResults.map(existing => {
                            if (newResultsMap.has(existing.name)) {
                                return newResultsMap.get(existing.name);
                            }
                            return existing;
                        });
                        renderResults();
                        updateStats();
                        toast.success(buildScanSummary(status), 5000);
                    } else if (type === 'action') {
                        // 核心优化：不再全量刷新列表，而是从本地数据中移除选中的项目
                        if (currentActionNames && currentActionNames.length > 0) {
                            scanResults = scanResults.filter(r => !currentActionNames.includes(r.name));
                            currentActionNames = []; // 清空
                            renderResults();
                            updateStats();
                            toast.success(buildActionSummary(status), 5000);
                        } else {
                            fetchAccountList(); // 保底方案
                            toast.success(buildActionSummary(status), 5000);
                        }
                    }

                    // 任务结束，如果后台栏开着，也给个提示
                    if (bgState) bgState.textContent = '完成';
                    currentTaskType = null;
                }
            } catch (err) {
                stopPolling();
                hideProgressModal();
                currentTaskType = null;
            }
        }, 2000);
    }

    function stopPolling() { clearInterval(pollingInterval); pollingInterval = null; }

    function connectWebSocket(batchId) {
        if (socket) socket.close();
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws/batch/${batchId}`;
        socket = new WebSocket(wsUrl);
        socket.onmessage = (e) => {
            const data = JSON.parse(e.data);
            if (data.type === 'log') addLog('info', data.message);
        };
    }

    function hideProgressModal() {
        progressModal.classList.remove('active');
        // 显示右下角后台进度条
        const bgBar = document.getElementById('background-task-bar');
        if (bgBar) bgBar.classList.add('active');
    }

    function showProgressModalFromBackground() {
        progressModal.classList.add('active');
        // 隐藏右下角后台进度条
        const bgBar = document.getElementById('background-task-bar');
        if (bgBar) bgBar.classList.remove('active');
    }

    document.getElementById('hide-progress-btn').addEventListener('click', hideProgressModal);

    // 后台任务栏按钮
    const bgOpenBtn = document.getElementById('background-task-open-btn');
    if (bgOpenBtn) bgOpenBtn.addEventListener('click', showProgressModalFromBackground);

    const bgCloseBtn = document.getElementById('background-task-close-btn');
    if (bgCloseBtn) bgCloseBtn.addEventListener('click', () => {
        document.getElementById('background-task-bar').classList.remove('active');
    });
});

/** 补货注册模式显隐控制 (全局函数) **/
function toggleReplenishVisibility() {
    const modeEl = document.getElementById('replenish-reg-mode');
    const parallelOptions = document.getElementById('replenish-concurrency-group');
    const pipelineOptions = document.getElementById('replenish-pipeline-group');
    if (!modeEl || !parallelOptions || !pipelineOptions) return;

    const isParallel = modeEl.value === 'parallel';

    // 并行模式：显示线程数，隐藏间隔
    // 串行模式：隐藏线程数，显示间隔
    parallelOptions.style.display = isParallel ? 'block' : 'none';
    pipelineOptions.style.display = isParallel ? 'none' : 'block';

    console.log(`[显隐切换执行器] 并行模式: ${isParallel}, Select: ${modeEl.value}`);
}
// 注册一个初始化触发
window.onload = function () {
    setTimeout(() => {
        toggleReplenishVisibility();
        toggleEmergencyConfigVisibility();
    }, 500);
};

function toggleEmergencyConfigVisibility() {
    const toggle = document.getElementById('patrol-emergency-toggle');
    const group = document.getElementById('emergency-config-group');
    if (toggle && group) {
        group.style.display = toggle.checked ? 'block' : 'none';
    }
}
