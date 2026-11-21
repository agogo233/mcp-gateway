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
- `maxProbeIntervalMs`：探活退避封顶，默认 300000（5 分钟）。上游 down 后，探测间隔按连续失败次数翻倍（15s→30s→…），到此封顶，避免对持续故障的上游无谓空转；探测成功立即复位
- `autoDisableAfter`：连续失败自动熔断阈值，默认 0（关闭）。达到次数后上游进入"已自动禁用"（quarantined）运行态：完全停止探测，直至通过控制台"启用"按钮、修改该上游配置或重启进程恢复；该状态只存在于内存，不写回 gateway.json
- `connectTimeoutMs`：与上游 initialize 握手超时，默认 3000
- `listTimeoutMs`：单次 ping/列举请求超时，默认 3000
- `failureThreshold`：连续失败阈值，默认 2
- `shutdownTimeoutMs`：优雅停机宽限，默认 5000
- `aggregate`：聚合端点路径段（如 `"all"` → `/mcp/all`），缺省关闭
- `watchConfig`：是否监听配置文件变化自动热更新，默认 true
- `auth.tokens`：网关接入鉴权的 Bearer token 列表（可选，见下文"鉴权"）
- `console.tokens`：管理控制台（`/api/*`）专用 Bearer token 列表（可选，见下文"管理控制台"）
- `authRateLimit`：认证失败限流（可选，见下文"鉴权"），默认 1 分钟窗口内失败 10 次即封禁该来源 60 秒
- `upstreams[].callTimeoutMs`：单次调用超时，默认 60000
- `upstreams[].probeIntervalMs`：该上游的探测周期覆盖（如目录较重的上游可放慢），留空用全局值
- `upstreams[].enabled`：该上游启停，缺省 true；false 时停止探测、目录与聚合端点剔除、`/mcp/<name>` 404
- `upstreams[].headers`：发往上游的附加头（如 Bearer token）

上游名不得含 `/` 或 `##`，不得与 `aggregate` 同名。

## 鉴权

网关涉及**三种相互独立的凭据**，不得混用（配置校验会拒绝 console 与 auth 复用同一 token）：

| 凭据 | 谁在用 | 用在哪 | 配置位置 |
|------|--------|--------|----------|
| 上游凭据 | 网关 → 上游 MCP | 访问上游时携带 | `upstreams[].headers` |
| 下游接入凭据 | opencode 等客户端 → 网关 | 调用 `/mcp/*` | `auth.tokens` |
| 管理凭据 | 管理员浏览器 → 控制台 | 登录 Web 控制台、`/api/*` | `console.tokens` |

`auth.tokens` 支持两种写法（可混用，旧格式完全兼容）：

```json
{
  "auth": {
    "tokens": [
      "openssl rand -hex 32 产生的64位hex",
      {"name": "opencode", "token": "……"},
      {"name": "ci-bot", "token": "……"}
    ]
  },
  "console": {
    "tokens": ["另一个 openssl rand -hex 32"]
  }
}
```

在 `gateway.json` 中配置 `auth.tokens` 后，所有 `/mcp/*` 端点（含聚合端点）都要求
`Authorization: Bearer <token>`，未携带或错误的请求一律返回 `401`。`/healthz` 保持免鉴权
（未带有效 token 时只返回 `{"status":"ok"}` 等存活信息，**不泄露上游拓扑**），供容器
healthcheck 使用；携带有效 token 访问 `/healthz` 才返回含上游 URL、失败计数等完整健康详情。

- token 生成：`openssl rand -hex 32`；最短 16 字符，禁止重复；
- 命名 token 的 `name` 仅用于控制台展示与调用日志归因（日志只记名字或指纹前 12 位，绝不记完整 token）；
- 三种凭据（上游/下游/管理）是**相互独立的秘密**，不要混用；
- 比较使用 SHA-256 + 常数时间比较，不泄露 token 内容与长度；
- **零停机轮换**：把新 token 追加进 `tokens` → 热更新生效 → 客户端切换 → 再删掉旧 token → 再次热更新；
- token 只放在 gitignore 的 `gateway.json` 与客户端配置里，不要提交、不要写进 `gateway.example.json`（那里只有占位符）。

### 认证失败限流

`/mcp/*` 与 `/api/*` 的认证失败按**来源 IP** 分别限流（两套独立计数，互不连累）：
默认 1 分钟窗口内失败满 10 次，该来源被封禁 60 秒——封禁期内即使携带正确 token 也返回
`429`（带 `Retry-After`）。失败日志只记每窗口首次与之后每第 10 次，防止刷爆运行日志。
封禁状态只在内存，重启即清零。

```json
{
  "authRateLimit": {
    "windowMs": 60000,
    "maxFails": 10,
    "blockMs": 60000,
    "disabled": false
  }
}
```

注意：若网关前置了反向代理，所有请求的来源都是代理地址，限流按代理地址聚合；
经 NAT 出口共享公网 IP 的用户也存在被牵连封禁 60 秒的可能。这是按来源限流的固有权衡。
不需要时设 `"disabled": true` 关闭（鉴权本身不受影响）。

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
（如 `"10.0.0.10:9200:9200"`）再配合防火墙限源；仅容器网络内使用时无需暴露端口。

**传输加密（TLS）**：客户端↔网关、网关↔上游当前均为**明文 HTTP**，上表三类 Bearer 凭据
在链路上可被嗅探。仅建议在可信网络（容器内网、防火墙限源的局域网）明文运行；需要经过
不可信网络时，请在网关前置 TLS 反向代理（Nginx/Caddy/Traefik 等）终结 HTTPS，网关本身
不要直接暴露公网。

## 管理控制台

网关内嵌中文管理控制台（纯静态单页，`go:embed` 打包进二进制，无额外依赖）：

- 浏览器打开 `http://<网关地址>:9200/` 即可使用；
- **健康总览**：进程状态、各上游 up/down/已停用/已自动禁用、失败计数、最近错误、目录条数、聚合端点；每个上游带**启停按钮**（调用 `/api/upstream/toggle`，写回 gateway.json 并热更新生效）；
- **工具目录**：按上游浏览 tools/prompts/resources 及其 schema（本地镜像，分页 + 名称过滤，仅启用的上游）；
- **配置管理**：结构化表单编辑全局参数、上游（增删改、headers、启停、探活频率）、auth/console 令牌；保存后校验并**原子写回 gateway.json**，随即热更新生效（也可取消勾选写盘，仅内存生效）；
- **最近活动**：内存环形缓冲（最近 500 条，重启清空）的调用与状态事件时间线，含调用方 token 归因、来源 IP、成败与耗时；不记录调用参数。

访问控制与安全边界：

- 页面静态资源免鉴权（不含敏感数据）；所有 `/api/*` 要求 `console.tokens` 中的 Bearer token，与下游 `auth.tokens` 完全独立，未配置 `console.tokens` 时控制台 API 一律 `403`；
- 配置读取接口**脱敏**：headers 值一律显示 `***`，token 只显示指纹（SHA-256 前 12 位）；保存时 `***`/指纹表示沿用原值，因此控制台永远不会回显明文秘密；
- 修改 `listen` 仍需重启进程；变更上游会重建其连接，进行中的调用可能失败（保存后页面会给出警告）；
- 保存会**重写 gateway.json**（统一为命名 token 对象格式、权限 0600、仅保留网关认识的字段）；容器只读挂载（`:ro`）时保存失败并在响应中说明，需改为可写挂载；
- 控制台具备配置写权限，**切勿暴露到不可信网络**：局域网访问务必启用 `console.tokens`、`ports` 绑定可信来源（deploy 栈默认 `127.0.0.1`，局域网用 `.env` 的 `MCP_GATEWAY_ADDR=0.0.0.0` 或宿主机 IP）并配合防火墙限源。

## 本地开发

```sh
go build ./... && go vet ./... && go test ./... -race -count=1
```

## 部署（Docker）

推荐使用 `deploy/` 独立部署栈（网关 + go-github-mcp，加入 opencode 所在的 `opencode-net` 网络，
与 OpenCodeUI 的 compose 完全解耦）：

```sh
cd deploy
cp ../gateway.json gwconfig/   # 配置拷入挂载目录（目录已 gitignore）
cp .env.example .env           # 填入 GITHUB_PERSONAL_ACCESS_TOKEN（细粒度 PAT）
docker compose up -d --build
```

配置按**目录**挂载且容器以 `user: "0:0"` 运行：控制台的配置保存与上游启停需要在
配置目录内做"临时文件 + rename"原子写，单文件只读挂载无法支持（会报 persist failed）。

`deploy/.env` 可调项：`GITHUB_TOOLSETS`（`default` 或 `all`）、`GITHUB_MCP_READ_ONLY`、
`MCP_GATEWAY_ADDR`（控制台/MCP 暴露地址：默认 `127.0.0.1` 仅宿主机本机；局域网访问设 `0.0.0.0`
或宿主机 IP，并建议配合防火墙限源）。

单容器手动运行（备选）：

```sh
docker build -t mcp-gateway:latest .
mkdir -p gwrun && cp gateway.json gwrun/
docker run -d --name mcp-gateway \
  -v $PWD/gwrun:/etc/mcp-gateway \
  -p 127.0.0.1:9200:9200 \
  --network <opencode所在网络> \
  mcp-gateway:latest
curl http://127.0.0.1:9200/healthz
# 热更新：改完宿主机上的 gateway.json 后
docker kill -s HUP mcp-gateway
```

（手动运行的容器默认以镜像内置的 `app` 用户运行；需要通过管理控制台保存配置或
启停上游时，请追加 `--user 0:0` 以获得挂载目录的写权限。）

## opencode 侧接入

`opencode.jsonc` 的 `mcp` 配置只改 URL。逐上游接入（键名不变，工具名不变）：

```jsonc
"mcp": {
  "ida":    { "type": "remote", "url": "http://mcp-gateway:9200/mcp/ida",    "enabled": true },
  "x64dbg": { "type": "remote", "url": "http://mcp-gateway:9200/mcp/x64dbg", "enabled": true }
}
```

或聚合单端点（`deploy/opencode.jsonc` 模板即此模式，工具名带 `all_` 前缀）：

```jsonc
"mcp": {
  "all": {
    "type": "remote",
    "url": "http://mcp-gateway:9200/mcp/all",
    "headers": { "Authorization": "Bearer <网关token>" },
    "enabled": true
  }
}
```

（原来直连上游时需要的 Authorization 头移到 gateway.json 的 `headers` 中；启用 auth 后
opencode 侧也需按"鉴权"一节加上网关 token 的 headers。）

## 已知限制

- 网关是单点；重启网关后，opencode 已初始化目录的会话不会自动重验，建议网关重启后同时重启 opencode（热更新不需要重启网关）。
- progress/logging 通知不透传。
- `listen` 变更不支持热更新，需重启网关。
- 资源订阅（resources/subscribe）与资源模板不做镜像（仅列 resources）。
- 上游 down 时，调用未注册工具返回 JSON-RPC "unknown tool" 错误（协议标准行为），快速失败。
- 聚合端点的名字拼接在极端情况下可能歧义（上游名互为前缀），重名按上游名排序先者胜出、告警一次；调用按注册归属路由，不会执行错误上游的工具。
- `/mcp/*` 请求体上限 4MiB（go-sdk `StreamableHTTPHandler` 的 `MaxRequestBodyBytes` 内置保护），超出返回 413。
