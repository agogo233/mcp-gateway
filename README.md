# mcp-gateway

一个 MCP 故障隔离网关：为每个上游 MCP 服务器暴露一个独立的 Streamable HTTP 端点（`/mcp/<name>`），
并用探测制健康检查镜像其目录。opencode 等客户端只连接网关——上游宕机、黑洞或变慢时，
网关的列举请求**立即**用本地目录应答（绝不阻塞等待上游），从而避免客户端初始化被拖死。

## 架构

```
[opencode] --mcp.ida-----> http://mcp-gateway:9200/mcp/ida    ──probe──> http://192.0.2.10:8737/mcp
[opencode] --mcp.x64dbg--> http://mcp-gateway:9200/mcp/x64dbg ──probe──> http://192.0.2.20:9094/
[其他客户端] --可选--------> http://mcp-gateway:9200/mcp/all    （聚合端点，见下文）
```

- 每个上游一个独立 `mcp.Server` 实例 + 独立探测 goroutine + 独立目录缓存。
- 工具/提示/资源名称**原样透传**（不加前缀），因此 opencode 侧配置键不变、工具名不变。
- 能力通告常开（tools/prompts/resources），即使目录为空也通告——否则客户端会永久跳过列举。

## 故障语义

| 场景 | 行为 |
|------|------|
| 上游可达 | 目录镜像（含 Description/InputSchema 等元数据），调用透传 |
| 上游不可达/变慢 | `tools/list` 等立即返回本地目录（宕机时为空）；调用快速失败（unknown tool 或 isError），绝不挂起 |
| 连续 `failureThreshold`(默认2) 次探测失败 | 标记 down，清空目录，回收会话；单次抖动不杀在途调用 |
| 上游恢复 | 一个探测周期(默认15s)内自动重新镜像 |
| 上游不支持 prompts/resources | 按能力声明裁剪列举，不会因此误判 down；对应分节返回空 |
| 某分节列举失败 | 只清空该分节，不影响其他分节 |
| 调用超时 | 按上游 `callTimeoutMs`（默认 60s；x64dbg 300s），每次列举请求独立计时 |

## 配置热更新

修改挂载的 `gateway.json` 后，网关**无需重启**即可生效：

- 文件 mtime 变化（2s 轮询）或向进程发送 `SIGHUP`（`docker kill -s HUP mcp-gateway`）触发重载；
- 上游按 diff 增/删/重建（URL、headers、超时任一变化即重建并重连）；
- 全局参数（探测周期、各超时、失败阈值）立即对新探测生效；
- 新配置解析/校验失败时**保留现役配置**并打日志，绝不因坏配置中断服务；
- 例外：`listen` 变更仍需重启进程。

## 聚合单端点（可选）

配置 `"aggregate": "all"` 后，网关额外暴露 `/mcp/all`，把所有**健康**上游的目录合并为一个：

- 命名规则：工具/提示 `"<上游名>_<原名>"`；资源 URI `"<上游名>##<原URI>"`；
- 调用按**注册归属**路由回对应上游（资源按 `##` 分隔符拆分），上游名互为前缀也不会串到错误上游；
- 某上游 down 只从聚合目录摘除它的条目，其余上游照常可用；
- 上游名互为前缀时可能产生重名冲突（按上游名排序先者胜出，告警一次），配置校验会对前缀关系打 warning；
- 适合"新增上游不想改客户端配置"的场景。**opencode 建议继续用 `/mcp/<name>`**——聚合端点的名字会被 opencode 再叠一层配置键前缀（如 `gw_ida_echo`）。

## 配置

见 `gateway.example.json`：

- `listen`：监听地址，默认 `:9200`
- `probeIntervalMs`：探测周期，默认 15000
- `connectTimeoutMs`：与上游 initialize 握手超时，默认 3000
- `listTimeoutMs`：单次 ping/列举请求超时，默认 3000
- `failureThreshold`：连续失败阈值，默认 2
- `shutdownTimeoutMs`：优雅停机宽限，默认 5000
- `aggregate`：聚合端点路径段（如 `"all"` → `/mcp/all`），缺省关闭
- `watchConfig`：是否监听配置文件变化自动热更新，默认 true
- `auth.tokens`：网关接入鉴权的 Bearer token 列表（可选，见下文"鉴权"）
- `upstreams[].callTimeoutMs`：单次调用超时，默认 60000
- `upstreams[].headers`：发往上游的附加头（如 Bearer token）

上游名不得含 `/` 或 `##`，不得与 `aggregate` 同名。

## 鉴权

在 `gateway.json` 中配置 `auth.tokens` 后，所有 `/mcp/*` 端点（含聚合端点）都要求
`Authorization: Bearer <token>`，未携带或错误的请求一律返回 `401`。`/healthz` 保持免鉴权
（未带有效 token 时只返回 `{"status":"ok"}` 等存活信息，**不泄露上游拓扑**），供容器
healthcheck 使用；携带有效 token 访问 `/healthz` 才返回含上游 URL、失败计数等完整健康详情。

```json
{
  "auth": { "tokens": ["<openssl rand -hex 32 产生的64位hex>"] }
}
```

- token 生成：`openssl rand -hex 32`；最短 16 字符，禁止重复；
- token 与上游凭据（如 x64dbg 的 Bearer）是**两个独立秘密**，不要混用；
- 比较使用 SHA-256 + 常数时间比较，不泄露 token 内容与长度；
- **零停机轮换**：把新 token 追加进 `tokens` → 热更新生效 → 客户端切换 → 再删掉旧 token → 再次热更新；
- token 只放在 gitignore 的 `gateway.json` 与客户端配置里，不要提交、不要写进 `gateway.example.json`（那里只有占位符）。

客户端（opencode）在对应 mcp 条目里加 headers 即可：

```jsonc
"mcp": {
  "ida": {
    "type": "remote",
    "url": "http://mcp-gateway:9200/mcp/ida",
    "headers": { "Authorization": "Bearer <token>" },
    "enabled": true
  }
}
```

因此**把网关暴露到局域网前必须先启用 auth**，并建议 compose `ports` 绑定宿主机具体 IP
（如 `"192.168.31.10:9200:9200"`）再配合防火墙限源；仅容器网络内使用时无需暴露端口。

## 本地开发

```sh
go build ./... && go vet ./... && go test ./... -race -count=1
```

## 部署（Docker）

```sh
docker build -t mcp-gateway:latest .
docker run -d --name mcp-gateway \
  -v $PWD/gateway.json:/etc/mcp-gateway/gateway.json:ro \
  --network <opencode所在网络> \
  mcp-gateway:latest
curl http://127.0.0.1:9200/healthz
# 热更新：改完宿主机上的 gateway.json 后
docker kill -s HUP mcp-gateway
```

compose 接入片段见 `compose-fragment.yml`。注意：网关只需加入 opencode 所在的 compose 网络，
**不要**把 9200 端口发布到宿主机/局域网；确有局域网接入需求时，必须先启用 auth（见"鉴权"），
并用 `ports` 绑定宿主机具体 IP、配合防火墙限源。

## opencode 侧接入

`opencode.jsonc` 的 `mcp` 配置只改 URL，键名保持不变（工具名因此完全不变）：

```jsonc
"mcp": {
  "ida":    { "type": "remote", "url": "http://mcp-gateway:9200/mcp/ida",    "enabled": true },
  "x64dbg": { "type": "remote", "url": "http://mcp-gateway:9200/mcp/x64dbg", "enabled": true }
}
```

（原来直连上游时需要的 Authorization 头移到 gateway.json 的 `headers` 中。）

## 已知限制

- 网关是单点；重启网关后，opencode 已初始化目录的会话不会自动重验，建议网关重启后同时重启 opencode（热更新不需要重启网关）。
- progress/logging 通知不透传。
- `listen` 变更不支持热更新，需重启网关。
- 资源订阅（resources/subscribe）与资源模板不做镜像（仅列 resources）。
- 上游 down 时，调用未注册工具返回 JSON-RPC "unknown tool" 错误（协议标准行为），快速失败。
- 聚合端点的名字拼接在极端情况下可能歧义（上游名互为前缀），重名按上游名排序先者胜出、告警一次；调用按注册归属路由，不会执行错误上游的工具。
