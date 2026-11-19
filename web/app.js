/* mcp-gateway management console. No framework, no build step.
 * All dynamic content is HTML-escaped: catalog entries originate from
 * upstream MCP servers and must never be trusted as markup. */

'use strict';

const TOKEN_KEY = 'mcp-gateway-console-token';
let token = localStorage.getItem(TOKEN_KEY) || '';
let activeTab = 'health';
let health = null;
let catalogCache = { upstream: '', tools: [], prompts: [], resources: [] };
let authTokens = [];   // [{name, fingerprint?, token?}] token only for new entries
let consoleTokens = []; // [{fingerprint?, token?}]
let refreshTimer = null;
let catalogTimer = null;

const $ = sel => document.querySelector(sel);

function esc(s) {
  return String(s ?? '').replace(/[&<>"']/g, c =>
    ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
}

function toast(msg, cls) {
  const t = $('#toast');
  t.textContent = msg;
  t.className = 'toast show' + (cls ? ' ' + cls : '');
  clearTimeout(t._h);
  t._h = setTimeout(() => t.classList.remove('show'), 3200);
}

function showLogin() {
  $('#loginMask').classList.remove('hidden');
}

function setAuthBadge() {
  const b = $('#authBadge');
  if (token) {
    b.textContent = 'console token: ••••' + token.slice(-4);
    b.classList.add('ok');
  } else {
    b.textContent = '未认证';
    b.classList.remove('ok');
  }
}

async function api(path, opts = {}) {
  const headers = Object.assign({}, opts.headers || {});
  if (token) headers['Authorization'] = 'Bearer ' + token;
  if (opts.body !== undefined) headers['Content-Type'] = 'application/json';
  const resp = await fetch(path, Object.assign({}, opts, { headers }));
  if (resp.status === 401) {
    showLogin();
    throw new Error('unauthorized');
  }
  if (!resp.ok) {
    let msg = resp.statusText;
    try {
      const j = JSON.parse(await resp.text());
      if (j.error) msg = j.error;
    } catch (_) { /* keep statusText */ }
    throw new Error(msg);
  }
  return resp.json();
}

/* ---------- tabs ---------- */

function switchTab(tab) {
  activeTab = tab;
  document.querySelectorAll('nav button').forEach(b =>
    b.classList.toggle('active', b.dataset.tab === tab));
  ['health', 'catalog', 'config', 'logs'].forEach(t =>
    $('#tab-' + t).classList.toggle('hidden', t !== tab));
  if (tab === 'health') refreshHealth();
  if (tab === 'catalog') { fillCatalogUpstreams(); loadCatalog(); }
  if (tab === 'config') loadConfig();
  if (tab === 'logs') loadLogs();
}

/* ---------- health ---------- */

function renderHealth(h) {
  $('#verBadge').textContent = h.version ? 'v' + h.version : '';
  const ok = h.status === 'ok';
  const cb = $('#connBadge');
  cb.textContent = ok ? '● 已连接' : '● 异常';
  cb.classList.toggle('ok', ok);
  cb.classList.toggle('bad', !ok);
  $('#hStatus').textContent = h.status === 'ok' ? 'OK' : h.status;
  $('#hStatus').style.color = ok ? 'var(--green)' : 'var(--red)';
  $('#hTime').textContent = h.time ? '服务器时间 ' + new Date(h.time).toLocaleString('zh-CN', { hour12: false }) : '';
  $('#hUptime').textContent = h.uptime || '…';
  const ups = h.upstreams || [];
  const act = ups.filter(u => u.enabled !== false);
  const upCount = act.filter(u => u.state === 'up').length;
  $('#hUpstreams').innerHTML = esc(String(upCount)) +
    ' <span style="font-size:14px;color:var(--muted)">/ ' + esc(String(act.length)) + ' 正常</span>';
  $('#hUpstreamsSub').textContent = (act.length - upCount) > 0 ? (act.length - upCount) + ' 个上游已标记 down' : '全部上游正常';
  if (h.aggregate) {
    $('#hAgg').textContent = '/mcp/' + h.aggregate.name;
    $('#hAggSub').textContent = '聚合端点已启用';
  } else {
    $('#hAgg').textContent = '未启用';
    $('#hAggSub').textContent = '配置 aggregate 后显示';
  }
  $('#upHealthList').innerHTML = ups.map(u => {
    const disabled = u.enabled === false;
    const quarantined = u.quarantined === true;
    // Config-disabled or runtime-quarantined: not serving either way, so
    // the toggle button must offer "enable" (resume clears quarantine).
    const off = disabled || quarantined;
    const cls = disabled ? 'unknown' : u.state === 'up' ? 'up' : u.state === 'down' ? 'down' : 'unknown';
    const stateLabel = disabled ? '已停用' : (quarantined ? u.state + ' · 已自动禁用' : u.state);
    return '<div class="up-card"><div class="dot ' + cls + '"></div><div>' +
      '<div class="name">' + esc(u.name) + (disabled ? ' <span class="tag dis">已停用</span>' : '') +
      (quarantined ? ' <span class="tag err">已自动禁用</span>' : '') + '</div>' +
      '<div class="url">' + esc(u.url) + '</div>' +
      '<div class="count">tools <b>' + esc(u.tools) + '</b></div>' +
      '<div class="count">prompts <b>' + esc(u.prompts) + '</b></div>' +
      '<div class="count">resources <b>' + esc(u.resources) + '</b></div>' +
      (u.lastError ? '<div class="err">last error: ' + esc(u.lastError) + '</div>' : '') +
      '</div><div class="meta">' + esc(stateLabel) + ' · 失败 ' + esc(u.failures) +
      (u.lastTransition ? '<br>最近切换 ' + esc(new Date(u.lastTransition).toLocaleString('zh-CN', { hour12: false })) : '') +
      '<br><button class="ghost" data-action="toggle-upstream" data-name="' + esc(u.name) + '" data-enabled="' + off + '" type="button">' +
      (off ? '启用' : '停用') + '</button>' +
      '</div></div>';
  }).join('') || '<div class="muted">暂无上游</div>';
}

async function refreshHealth() {
  try {
    health = await api('/api/health');
    renderHealth(health);
  } catch (e) {
    if (e.message !== 'unauthorized') {
      const cb = $('#connBadge');
      cb.textContent = '● 请求失败';
      cb.classList.add('bad');
    }
  }
}

/* ---------- catalog ---------- */

function fillCatalogUpstreams() {
  const sel = $('#catUp');
  const names = (health && health.upstreams ? health.upstreams : [])
    .filter(u => u.enabled !== false).map(u => u.name);
  const cur = sel.value;
  sel.innerHTML = names.map(n => '<option value="' + esc(n) + '">上游：' + esc(n) + '</option>').join('')
    || '<option value="">（暂无上游）</option>';
  if (names.includes(cur)) sel.value = cur;
}

function catalogTable(id, rows, empty) {
  const el = $(id);
  if (!rows.length) {
    el.innerHTML = '<tr><td colspan="3" style="color:var(--muted);text-align:center;padding:20px">' + esc(empty) + '</td></tr>';
    return;
  }
  el.innerHTML = '<tr><th style="width:32%">名称</th><th>描述</th><th style="width:18%">详情</th></tr>' +
    rows.map(r => '<tr><td class="mono">' + esc(r.name || r.uri) + '</td><td>' + esc(r.description) + '</td>' +
      '<td><details><summary>详情</summary><pre>' +
      esc(r.inputSchema ? JSON.stringify(r.inputSchema, null, 2)
        : r.arguments ? JSON.stringify(r.arguments, null, 2)
        : [r.mimeType, r.uri].filter(Boolean).join(' · ')) +
      '</pre></details></td></tr>').join('');
}

async function loadCatalog() {
  const up = $('#catUp').value;
  const q = $('#catQ').value.trim();
  if (!up) { catalogCache = { upstream: '', tools: [], prompts: [], resources: [] }; }
  else {
    try {
      const [t, p, r] = await Promise.all(['tools', 'prompts', 'resources'].map(s =>
        api('/api/catalog?upstream=' + encodeURIComponent(up) + '&section=' + s + '&limit=1000&q=' + encodeURIComponent(q))));
      catalogCache = { upstream: up, tools: t.items, prompts: p.items, resources: r.items };
    } catch (e) {
      if (e.message !== 'unauthorized') toast('目录加载失败: ' + e.message, 'err');
      return;
    }
  }
  catalogTable('#toolsTable', catalogCache.tools, '无匹配工具（或上游 down，目录已清空）');
  catalogTable('#promptsTable', catalogCache.prompts, '无匹配 prompts');
  catalogTable('#resourcesTable', catalogCache.resources, '无匹配 resources');
}

/* ---------- config ---------- */

function tokenRows(el, list, named) {
  el.innerHTML = '<tr><th>' + (named ? '名称' : '指纹') + '</th><th style="width:150px">操作</th></tr>' +
    (list.length ? list.map((t, i) => '<tr>' +
      (named ? '<td class="mono">' + esc(t.name || '（未命名）') + '</td>' : '') +
      '<td class="mono" style="color:var(--muted)">' + esc(t.fingerprint || '（新令牌）') + '</td>' +
      '<td>' + (named ? '<button class="ghost" data-action="rename-auth" data-i="' + i + '" type="button">改名</button> ' : '') +
      '<button class="danger" data-action="del-' + (named ? 'auth' : 'console') + '" data-i="' + i + '" type="button">删除</button></td></tr>').join('')
      : '<tr><td colspan="2" class="muted" style="text-align:center;padding:14px">（无）</td></tr>');
}

function renderTokens() {
  tokenRows($('#authTokenTable'), authTokens, true);
  tokenRows($('#consoleTokenTable'), consoleTokens, false);
}

function headerText(headers) {
  return Object.entries(headers || {}).map(([k, v]) => k + ': ' + v).join('\n');
}

function renderUpstreamCards(ups) {
  $('#upList').innerHTML = ups.map((u, i) => '<div class="card" style="margin-bottom:12px" data-idx="' + i + '">' +
    '<div class="row"><div class="field" style="flex:1"><label>name</label><input type="text" class="up-name" value="' + esc(u.name) + '"></div>' +
    '<div class="field" style="flex:2"><label>url</label><input type="text" class="up-url" value="' + esc(u.url) + '"></div></div>' +
    '<div class="row"><div class="field" style="flex:1"><label>callTimeoutMs</label><input type="number" class="up-timeout" value="' + esc(u.callTimeoutMs || '') + '" placeholder="60000"></div>' +
    '<div class="field" style="flex:1"><label>probeIntervalMs（留空=全局）</label><input type="number" class="up-probe" value="' + esc(u.probeIntervalMs || '') + '" placeholder="15000"></div>' +
    '<div class="field" style="flex:2"><label>headers（每行一条 Key: ***；*** 表示保持不变）</label><textarea class="up-headers" placeholder="Authorization: ***">' + esc(headerText(u.headers)) + '</textarea></div>' +
    '<label class="muted" style="flex:none;white-space:nowrap"><input type="checkbox" class="up-enabled" ' + (u.enabled !== false ? 'checked' : '') + '> 启用</label>' +
    '<button class="danger" data-action="del-upstream" data-i="' + i + '" type="button">删除</button></div></div>').join('');
}

async function loadConfig() {
  let cfg;
  try {
    cfg = await api('/api/config');
  } catch (e) {
    if (e.message !== 'unauthorized') toast('配置加载失败: ' + e.message, 'err');
    return;
  }
  $('#cfgListen').value = cfg.listen || '';
  $('#cfgAggregate').value = cfg.aggregate || '';
  $('#cfgProbe').value = cfg.probeIntervalMs;
  $('#cfgMaxProbe').value = cfg.maxProbeIntervalMs;
  $('#cfgAutoDisable').value = cfg.autoDisableAfter;
  $('#cfgConnect').value = cfg.connectTimeoutMs;
  $('#cfgList').value = cfg.listTimeoutMs;
  $('#cfgFailure').value = cfg.failureThreshold;
  authTokens = (cfg.auth && cfg.auth.tokens || []).map(t => ({ name: t.name || '', fingerprint: t.fingerprint }));
  consoleTokens = (cfg.console && cfg.console.tokens || []).map(t => ({ fingerprint: t.fingerprint }));
  renderTokens();
  renderUpstreamCards(cfg.upstreams || []);
  $('#saveResult').innerHTML = '';
}

function collectConfig() {
  const num = id => { const v = parseInt($(id).value, 10); return Number.isFinite(v) ? v : 0; };
  const edit = {
    listen: $('#cfgListen').value.trim(),
    aggregate: $('#cfgAggregate').value.trim(),
    probeIntervalMs: num('#cfgProbe'),
    maxProbeIntervalMs: num('#cfgMaxProbe'),
    autoDisableAfter: num('#cfgAutoDisable'),
    connectTimeoutMs: num('#cfgConnect'),
    listTimeoutMs: num('#cfgList'),
    failureThreshold: num('#cfgFailure'),
    auth: { tokens: authTokens.map(t => t.token ? { name: t.name, token: t.token } : { name: t.name, fingerprint: t.fingerprint }) },
    console: { tokens: consoleTokens.map(t => t.token ? { token: t.token } : { fingerprint: t.fingerprint }) },
    upstreams: []
  };
  document.querySelectorAll('#upList .card').forEach(card => {
    const headers = {};
    (card.querySelector('.up-headers').value || '').split('\n').forEach(line => {
      const idx = line.indexOf(':');
      if (idx <= 0) return;
      const k = line.slice(0, idx).trim();
      const v = line.slice(idx + 1).trim();
      if (k && v) headers[k] = v;
    });
    edit.upstreams.push({
      name: card.querySelector('.up-name').value.trim(),
      url: card.querySelector('.up-url').value.trim(),
      callTimeoutMs: parseInt(card.querySelector('.up-timeout').value, 10) || 0,
      probeIntervalMs: parseInt(card.querySelector('.up-probe').value, 10) || 0,
      enabled: card.querySelector('.up-enabled').checked,
      headers
    });
  });
  return edit;
}

async function saveConfig() {
  const persist = $('#cfgPersist').checked;
  if (!confirm('保存并热更新配置？\n\n修改上游会重建其连接，进行中的调用可能失败。')) return;
  try {
    const res = await api('/api/config' + (persist ? '' : '?persist=false'), {
      method: 'PUT',
      body: JSON.stringify(collectConfig())
    });
    $('#saveResult').innerHTML =
      '<div class="okbox">✔ 配置校验通过并已热更新生效' + (res.persisted ? '（已写回 gateway.json）' : '（仅内存生效，重启后回到文件内容）') + '</div>' +
      (res.warnings && res.warnings.length ? '<div class="warnbox">⚠ ' + res.warnings.map(esc).join('<br>') + '</div>' : '');
    toast('保存成功，配置已热更新');
    await refreshHealth();
  } catch (e) {
    if (e.message !== 'unauthorized') {
      $('#saveResult').innerHTML = '<div class="warnbox">✘ 保存失败：' + esc(e.message) + '</div>';
      toast('保存失败: ' + e.message, 'err');
    }
  }
}

async function reloadConfig() {
  try {
    await api('/api/reload', { method: 'POST' });
    toast('已从 gateway.json 重新加载');
    await loadConfig();
  } catch (e) {
    if (e.message !== 'unauthorized') toast('重载失败: ' + e.message, 'err');
  }
}

async function toggleUpstream(name, enabled) {
  if (!confirm(enabled ? '启用上游 ' + name + ' 并立即探测？' : '停用上游 ' + name + '？将停止探测并从目录与聚合端点移除。')) return;
  try {
    await api('/api/upstream/toggle', {
      method: 'POST',
      body: JSON.stringify({ name, enabled })
    });
    toast((enabled ? '已启用 ' : '已停用 ') + name);
    await refreshHealth();
    if (activeTab === 'catalog') { fillCatalogUpstreams(); loadCatalog(); }
  } catch (e) {
    if (e.message !== 'unauthorized') toast('操作失败: ' + e.message, 'err');
  }
}

/* ---------- logs ---------- */

function renderLogs(entries) {
  $('#logTable').innerHTML = '<tr><th>时间</th><th>调用方</th><th>来源</th><th>类型</th><th>名称</th><th>上游</th><th>结果</th><th>耗时</th></tr>' +
    (entries.length ? entries.map(l => {
      const isEvent = l.kind === 'event';
      const typeTag = isEvent
        ? '<span class="tag">' + esc(l.event) + '</span>'
        : '<span class="tag">' + esc(l.kind) + '</span>';
      const result = isEvent
        ? '<span class="tag">' + esc(l.event === 'down' ? '状态' : l.event === 'up' ? '状态' : '配置') + '</span>'
        : l.ok ? '<span class="tag ok">ok</span>' : '<span class="tag err">fail</span>';
      return '<tr><td class="mono">' + esc(new Date(l.time).toLocaleTimeString('zh-CN', { hour12: false })) + '</td>' +
        '<td class="mono">' + esc(l.caller || '—') + '</td>' +
        '<td class="mono">' + esc(l.srcIP || '—') + '</td>' +
        '<td>' + typeTag + '</td>' +
        '<td class="mono">' + esc(l.name || l.detail || '') + '</td>' +
        '<td class="mono">' + esc(l.upstream || '—') + '</td>' +
        '<td>' + result + (!isEvent && !l.ok && l.detail ? ' <span class="muted">' + esc(l.detail) + '</span>' : '') + '</td>' +
        '<td class="mono">' + (l.durMs ? l.durMs + 'ms' : '—') + '</td></tr>';
    }).join('') : '<tr><td colspan="8" class="muted" style="text-align:center;padding:20px">暂无记录</td></tr>');
}

async function loadLogs() {
  const type = $('#logType').value;
  try {
    const res = await api('/api/logs?limit=200' + (type ? '&type=' + type : ''));
    renderLogs(res.entries || []);
  } catch (e) {
    if (e.message !== 'unauthorized') toast('日志加载失败: ' + e.message, 'err');
  }
}

/* ---------- auth / login ---------- */

function doLogin() {
  const t = $('#tokenInput').value.trim();
  if (t.length < 16) {
    $('#loginErr').textContent = 'token 至少 16 个字符';
    return;
  }
  token = t;
  localStorage.setItem(TOKEN_KEY, t);
  $('#loginMask').classList.add('hidden');
  $('#loginErr').textContent = '';
  setAuthBadge();
  boot();
}

function showReauth() {
  $('#tokenInput').value = token;
  showLogin();
}

/* ---------- boot & timers ---------- */

function boot() {
  refreshHealth().then(() => {
    if (activeTab === 'catalog') { fillCatalogUpstreams(); loadCatalog(); }
  });
  if (activeTab === 'config') loadConfig();
  if (activeTab === 'logs') loadLogs();
}

clearInterval(refreshTimer);
refreshTimer = setInterval(() => {
  if (!document.hidden && token) {
    refreshHealth();
    if (activeTab === 'logs') loadLogs();
  }
}, 5000);

document.querySelectorAll('nav button').forEach(b =>
  b.addEventListener('click', () => switchTab(b.dataset.tab)));
$('#btnLogin').addEventListener('click', doLogin);
$('#tokenInput').addEventListener('keydown', e => { if (e.key === 'Enter') doLogin(); });
$('#btnReauth').addEventListener('click', showReauth);
$('#btnSave').addEventListener('click', saveConfig);
$('#btnReloadCfg').addEventListener('click', reloadConfig);
$('#btnRefreshLogs').addEventListener('click', loadLogs);
$('#logType').addEventListener('change', loadLogs);
$('#catUp').addEventListener('change', loadCatalog);
$('#catQ').addEventListener('input', () => {
  clearTimeout(catalogTimer);
  catalogTimer = setTimeout(loadCatalog, 300);
});
$('#btnAddAuth').addEventListener('click', () => {
  const tok = $('#newAuthTok').value.trim();
  if (tok.length < 16) { toast('token 至少 16 个字符', 'warn'); return; }
  authTokens.push({ name: $('#newAuthName').value.trim(), token: tok });
  $('#newAuthName').value = ''; $('#newAuthTok').value = '';
  renderTokens();
  toast('已添加（保存后生效；明文不回显）');
});
$('#btnAddConsole').addEventListener('click', () => {
  const tok = $('#newConsoleTok').value.trim();
  if (tok.length < 16) { toast('token 至少 16 个字符', 'warn'); return; }
  consoleTokens.push({ token: tok });
  $('#newConsoleTok').value = '';
  renderTokens();
  toast('已添加管理令牌（保存后生效）');
});
$('#btnAddUpstream').addEventListener('click', () => {
  renderUpstreamCards(collectConfig().upstreams.concat([{ name: '', url: '', callTimeoutMs: 60000, headers: {} }]));
  toast('已添加上游卡片，填写后保存生效');
});

/* dynamic buttons (rename/delete) via delegation */
document.addEventListener('click', e => {
  const btn = e.target.closest('[data-action]');
  if (!btn) return;
  const i = parseInt(btn.dataset.i, 10);
  switch (btn.dataset.action) {
    case 'rename-auth': {
      const n = prompt('新名称（仅改名字，令牌保持不变）:', authTokens[i].name || '');
      if (n === null) return;
      authTokens[i].name = n.trim();
      renderTokens();
      toast('已改名；保存后按指纹沿用原令牌');
      break;
    }
    case 'del-auth':
      if (!confirm('删除该下游令牌？使用它的客户端将立即 401。')) return;
      authTokens.splice(i, 1);
      renderTokens();
      break;
    case 'del-console':
      if (consoleTokens.length === 1 && !confirm('这是最后一个管理令牌，删除后将无法再访问控制台（只能修改 gateway.json 恢复）。确定删除？')) return;
      consoleTokens.splice(i, 1);
      renderTokens();
      break;
    case 'del-upstream': {
      const ups = collectConfig().upstreams;
      ups.splice(i, 1);
      renderUpstreamCards(ups);
      break;
    }
    case 'toggle-upstream':
      toggleUpstream(btn.dataset.name, btn.dataset.enabled === 'true');
      break;
  }
});

setAuthBadge();
if (token) boot(); else showLogin();
