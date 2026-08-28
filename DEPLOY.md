# RID Monitor 云端部署

目标环境：Ubuntu x86_64、Docker Engine、Docker Compose v2，以及由宝塔管理的
Nginx 域名和 HTTPS 证书。宿主机只有一个应用入口：

```text
Internet -> Baota HTTPS -> 127.0.0.1:18081 -> rid-edge
                                                |-- /ws -> rid-monitor:18082
                                                `-- 其他 -> rid-monitor:18081
```

宝塔只配置一个反向代理目标 `http://127.0.0.1:18081` 并开启 WebSocket。Python 应用
不向宿主发布任何端口，`18081/18082` 只存在于 Docker 内部网络。公网只开放宝塔的
`80/443`。

生产访问控制分工如下：

- 大屏登录使用服务端校验和签名的 HttpOnly session Cookie，读 API 和 WebSocket
  都在后端验证该 session。
- `POST /api/ingest` 不需要大屏 session，只接受独立 Bearer token。
- 宝塔负责公网 HTTPS；容器内 `rid-edge` 负责统一入口、WS 分流、安全响应头和请求
  限制。

默认不要在宝塔叠加站点 Basic Auth，否则浏览器会出现双重登录，而且 ingest 客户端
也会被同一个单入口门禁拦截。

## 1. 初始化应用密钥

进入本仓库根目录，以随机十六进制值填充服务器专用 `.env`：

```bash
	cd /www/wwwroot/rid-yunshao
umask 077
cp .env.example .env

RID_INGEST_TOKEN_VALUE="$(openssl rand -hex 32)"
RID_ADMIN_PASSWORD_VALUE="$(openssl rand -hex 16)"
RID_SESSION_SECRET_VALUE="$(openssl rand -hex 32)"

sed -i "s/^RID_INGEST_TOKEN=.*/RID_INGEST_TOKEN=${RID_INGEST_TOKEN_VALUE}/" .env
sed -i "s/^RID_ADMIN_PASSWORD=.*/RID_ADMIN_PASSWORD=${RID_ADMIN_PASSWORD_VALUE}/" .env
sed -i "s/^RID_SESSION_SECRET=.*/RID_SESSION_SECRET=${RID_SESSION_SECRET_VALUE}/" .env
chmod 600 .env

printf '请立即保存大屏 admin 密码: %s\n' "$RID_ADMIN_PASSWORD_VALUE"
unset RID_INGEST_TOKEN_VALUE RID_ADMIN_PASSWORD_VALUE RID_SESSION_SECRET_VALUE
```

`.env` 只保存在服务器，不能提交到 Git、截图或放进前端代码。Dockerfile 只复制明确
列出的运行文件，`.dockerignore` 也排除了 `.env`、SQLite 和 spool 数据。拥有 Docker
管理权限的用户仍可查看容器环境变量，因此服务器 Docker 权限等同 root 权限。

按实际高德 Web JS API 应用填写 `.env` 中的 `AMAP_KEY` 和
`AMAP_SECURITY_CODE`。`RID_WS_PATH` 保持 `/ws`，`RID_COOKIE_SECURE` 在 HTTPS
生产环境必须保持 `1`。
`RID_MONITOR_LAT/RID_MONITOR_LON` 使用 WGS-84；未设置时使用公开演示坐标
`39.9042/116.4074`。

空域导入默认限制由 `.env.example` 中四个 `RID_AIRSPACE_*` 变量控制（默认 20 MiB、
50000 个要素、单要素 20000 个顶点、总计 1000000 个顶点）。UOM 的生产
endpoint 和凭据不是公开 Key；未取得 USS/UOM 正式授权前，保持三个
`RID_UOM_AIRSPACE_*` 变量为空。当前版本会明确返回“未配置”，不会抓网页接口或把
手工导入标记为官方授权同步。

## 2. 构建并启动

```bash
docker compose config --quiet
docker compose pull rid-edge
docker compose build --pull rid-monitor
docker compose up -d
docker compose ps
docker compose logs --tail=100 rid-monitor rid-edge
```

两项服务都应显示 `healthy`。宿主机本地验收统一经过 edge：

```bash
curl --fail --silent --show-error http://127.0.0.1:18081/healthz
test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  http://127.0.0.1:18081/api/status)" = 401
ss -lnt | grep '127.0.0.1:18081'
! ss -lnt | grep -q ':18082 '
docker compose exec rid-edge nginx -t
docker compose exec rid-edge id
docker compose exec rid-monitor id
docker compose exec rid-monitor sh -c 'test ! -w /app && test -w /app/data'
```

`rid-edge` 应为 UID/GID `101`，应用应为 UID/GID `10001`。两个容器根文件系统都只读；
应用只有 `/app/data` SQLite 卷和 `/tmp` 可写，edge 只有 `/tmp` 可写。SQLite 文件位于
`/app/data/rid_history.db`，持久化在命名卷 `tdisplay-s3-rid-data`。

低资源限制如下：应用 1 CPU、256 MiB、128 PID；edge 0.25 CPU、32 MiB、32 PID。
应用只连接 `backend` 内部网络，edge 是唯一同时连接前端与后端网络的服务。

## 3. 配置宝塔单入口反代

在宝塔创建域名站点、启用 HTTPS 并强制 HTTP 跳转 HTTPS。然后只创建一条反向代理：

- 代理名称：`rid-monitor`
- 目标 URL：`http://127.0.0.1:18081`
- 发送域名：当前域名
- WebSocket：开启

若使用站点 Nginx 配置文件而不是宝塔 UI，将 `nginx-baota.conf` 中唯一的
`location /` 粘贴进现有 `server {}`。若已有 `location /`，替换它，不能保留重复块。
重载前检查：

```bash
/www/server/nginx/sbin/nginx -t
```

宝塔必须把 `Host`、`Origin`、Cookie、Authorization 和外层 HTTPS 协议转给 edge。
随仓库提供的 snippet 已处理这些请求头，并覆盖不可信客户端传入的
`X-Forwarded-For`，避免登录限速被伪造来源绕过。
snippet 的外层请求上限是 20 MiB，以便空域文件通过；容器 edge 仍将其他普通 API
限制为 1 MiB，只对 `/api/airspace/import` 放宽到 20 MiB。

域名验收：

```bash
curl --fail --silent --show-error https://rid.example.com/healthz
test "$(curl --silent --output /dev/null --write-out '%{http_code}' \
  https://rid.example.com/api/status)" = 401
```

浏览器打开 `https://rid.example.com/dashboard.html`，用 `.env` 中的
`RID_ADMIN_USER/RID_ADMIN_PASSWORD` 登录。登录后开发者工具中 `/api/status` 应为
`200`，WebSocket 应连接 `wss://rid.example.com/ws`。未登录或跨 Origin 的 WS 会被
应用关闭。

## 4. 验证空域数据边界

登录后页面的“空域与事件 -> 空域参考数据”应显示“未配置正式 UOM 空域接口和凭据”。
点击“同步 UOM”必须得到明确未配置提示，而不是成功状态。可导入合法取得的 WGS-84
GeoJSON 或标准响应；手工导入的来源必须显示 `manual_import`、`authoritative=false`。

`/api/airspace/catalog` 应显示 31 个大陆省级行政区、6 组已观察 UOM WMS 图层和
30 个已观察区划代码，并把未出现在该 WMS 清单中的北京 `110000` 单列。新数据库还应
自动出现来源 `uom-beijing-110000`：其区域类别为 `prohibited`，但
`authoritative=false`，边界是行政区参考面。该内置规则参考不能显示成正式授权同步，
也不能替代 UOM 高精度矢量或实际飞行审批结果。

服务端可用带 session Cookie 的请求复核：

```bash
curl --fail --silent --show-error \
  --cookie 'rid_session=登录后取得的会话值' \
  https://rid.example.com/api/airspace/status
curl --fail --silent --show-error \
  --cookie 'rid_session=登录后取得的会话值' \
  https://rid.example.com/api/airspace/catalog
curl --fail --silent --show-error \
  --cookie 'rid_session=登录后取得的会话值' \
  'https://rid.example.com/api/airspace/zones?bbox=121.1,30.9,121.4,31.2'
curl --fail --silent --show-error \
  --cookie 'rid_session=登录后取得的会话值' \
  'https://rid.example.com/api/airspace/zones?bbox=115.4,39.4,117.5,41.1&classes=prohibited'
```

不要在没有正式授权的情况下填写或猜测 `RID_UOM_AIRSPACE_*`。接口规范和监管语义见
`PRODUCT_RESEARCH.md`。

## 5. 验证独立 ingest 鉴权

ingest 走同一个域名和同一个宝塔反代入口，不需要大屏 session，只使用 Bearer token：

```bash
RID_TOKEN="$(sed -n 's/^RID_INGEST_TOKEN=//p' .env)"
curl --fail-with-body --include \
  -X POST https://rid.example.com/api/ingest \
  -H "Authorization: Bearer ${RID_TOKEN}" \
  -H 'Content-Type: application/json' \
  --data '{"t":"snap","n":0,"ch":0,"bat":-1,"drones":[]}'
unset RID_TOKEN
```

正常响应是 HTTP `202`。不带 Bearer token 或 token 错误必须由应用返回 `401`。
登录大屏后查看 `/api/status`，`latestSnapshotAt` 应已更新。

## 6. Windows 现场网关

连接 T-Display-S3 的 Windows 电脑运行 `gateway.py`，将串口快照写入本地 SQLite
spool 后可靠上传；断网、DNS 或云端故障不会丢掉队列。使用环境变量传 token，避免
把它留在命令历史和进程参数中：

```powershell
	cd "C:\path\to\rid-yunshao"
python -m pip install -r requirements.txt
$env:RID_INGEST_TOKEN = "服务器 .env 中的 RID_INGEST_TOKEN"
	python gateway.py --port COMx `
	  --url https://rid.example.com/api/ingest `
	  --station-id station-01 --station-name "示例哨站"
Remove-Item Env:RID_INGEST_TOKEN
```

无硬件端到端测试可把一行 snap JSON 管道给 `--stdin`：

```powershell
'{"t":"snap","n":0,"ch":0,"bat":-1,"drones":[]}' |
  python gateway.py --stdin --url https://rid.example.com/api/ingest `
    --station-id test-01
```

## 7. 数据源规则

生产实时层只接收 T-Display-S3 经电脑 USB `gateway.py` 上传的数据。无网关心跳时
实时层归零；独立 `simulator.py` 的来源标记和 User-Agent 会被 `/api/ingest` 拒绝。
需要验证模拟目标时，在板子开机页选择“模拟 RID”，再运行
`-Action gateway -Port COMx`。这些数据仍来自真实连接的板子，并带 `simulated=true`。

## 8. 日常运维

查看状态和日志：

```bash
docker compose ps
docker compose logs --follow --tail=200 rid-monitor rid-edge
```

更新代码和 edge 镜像，不会删除 SQLite 命名卷：

```bash
git pull --ff-only
cd display-server
docker compose pull rid-edge
docker compose build --pull rid-monitor
docker compose up -d --remove-orphans
docker compose ps
```

edge 使用 Docker 内置 DNS 动态解析应用服务；应用容器升级换 IP 后无需手工改 upstream。

备份整个 SQLite 卷时先短暂停应用，确保数据库和 WAL 一致：

```bash
mkdir -p backups
docker compose stop rid-monitor
docker run --rm --platform linux/amd64 \
  -v tdisplay-s3-rid-data:/data:ro \
  -v "$PWD/backups:/backup" \
  alpine:3.20 sh -c 'tar -czf /backup/rid-data-$(date +%Y%m%d-%H%M%S).tar.gz -C /data .'
docker compose start rid-monitor
```

不要执行 `docker compose down -v`，该命令会删除历史数据库卷。

## 9. 故障定位

- Compose 报变量缺失：`.env` 必须与 `docker-compose.yml` 在同一目录，且
  `RID_INGEST_TOKEN`、`RID_ADMIN_PASSWORD`、`RID_SESSION_SECRET` 都非空。
- `rid-monitor` unhealthy：查看应用日志，并在容器内请求
  `http://127.0.0.1:18081/healthz`。
- `rid-edge` unhealthy：运行 `docker compose exec rid-edge nginx -t`，查看 edge
  日志，并执行 `docker compose exec rid-edge wget -qO- http://rid-monitor:18081/healthz`。
- 宝塔返回 `502`：先从宿主请求 `http://127.0.0.1:18081/healthz`；失败则检查
  `docker compose ps` 和两个容器日志。
- 页面正常但实时数据断开：确认宝塔已开启 WebSocket，外层只反代 `18081`，浏览器
  URL 是 `/ws`，应用 session 尚未过期。
- ingest 返回 `401`：确认 Bearer token 与服务器 `.env` 一致；不要给宝塔单入口增加
  会拦截现场网关的 Basic Auth。
- 宿主出现 `18082` 监听：当前 Compose 配置不应发布该端口，检查是否仍有旧容器或
  手工启动的服务。
- 旧卷出现权限错误：停服后检查卷内文件归属，应用进程需要 UID/GID `10001` 的
  写权限。
