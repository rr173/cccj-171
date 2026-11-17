# repcache — HTTP 表示复用客户端

供采集器调用的线程安全 HTTP 缓存客户端。多个采集任务共享同一个会按
`Accept-Language` 等请求头返回不同正文的公开接口时，repcache 按
**URL + Vary 选择规则 + 调用方取值** 复用表示：每个任务拿到属于自己
语言/区域的正文，相同任务合并重复取件，过期表示用 ETag 重验证而不是
重新下载正文。

* 仅 Python 标准库（`http.client` + `sqlite3`），无第三方依赖。
* 缓存选择、在途请求协调、重验证逻辑全部自实现，不委托代理或缓存库。
* 单进程多线程安全；缓存库可关闭后重新打开继续复用（在途协调只存在于
  内存，重开不会被残留标记卡死）。

## 安装与运行

```bash
pip install .                  # 或 pip install -e .
python -m unittest discover -s tests   # 测试（stdlib unittest，无公网）
python -m repcache.scrape_demo         # 采集演练（本地双语服务）
repcache-demo                          # 安装后的等价入口
```

## API

```python
from repcache import RepCacheClient

client = RepCacheClient("cache.sqlite", max_bytes=1 << 20, timeout=10.0)
res = client.get("http://example.com/dir", {"Accept-Language": "zh"})
print(res.body, res.source, res.validated)
res2 = client.refresh("http://example.com/dir", {"Accept-Language": "zh"})
client.close()
```

* `get(url, headers=None, *, stale_if_error=None) -> FetchResult` — 普通调用：
  命中新鲜表示直接返回；过期表示按 ETag 条件重验证（无 ETag 用无条件
  GET 替换）；无表示时取件。并发调用按在途键合并（见下）。
  `stale_if_error` 为按次启用的故障兜底秒数上限，默认 `None`（不启用），
  见下文「stale-if-error 故障兜底」。
* `refresh(url, headers=None) -> FetchResult` — 管理员操作：永远发无条件
  GET，不加入早已开始的普通取件，可与其并行；成功 200 按常规规则发布。
  失败永远照常抛出/返回，**不**做 stale-if-error 兜底。
* `close()` / 上下文管理器 — 关闭缓存库。
* `clock` 可注入（测试用假时钟推进时间），生产默认 `time.time`。

### FetchResult 字段

| 字段 | 含义 |
| --- | --- |
| `status` | 表示的有效状态（缓存命中/304 确认均为 200；未保存的状态如 404 为线上状态） |
| `body` | 完整响应正文，可直接交给解析程序；304 确认返回的是已存正文，**不会**是空正文 |
| `headers` | 有效表示头（304 合并后；逐跳头已剔除） |
| `source` | `NETWORK`（本次网络 200）/ `CACHE`（仍新鲜的本地表示）/ `REVALIDATED`（304 确认后的旧正文）/ `STALE_IF_ERROR`（故障兜底返回的过期正文） |
| `network_status` | 本次实际线上状态（200/304/其他；故障兜底时是使兜底生效的 500/502/503/504，传输失败为 `None`），未发请求为 `None` |
| `validated` | 本次是否发生了生效的条件重验证 |
| `vary` | 所选表示的 Vary 字段名元组 |
| `age` | `AgeInfo`：`q`/`r`/`now`/`date_epoch`/`age_header`/`initial_age`/`current_age`/`lifetime`/`must_validate` |
| `stale` | 正文是否因故障兜底而带着已过期的年龄返回 |
| `stale_fallback` | 兜底时的 `StaleFallback`（见下）；否则 `None` |
| `from_cache` / `ok` / `text` | 便捷属性 |

`StaleFallback` 明确记录兜底依据：`upstream_status`（触发兜底的上游 HTTP
状态，或 `None`）、`upstream_error`（传输错误文本，或 `None`）、
`staleness`（`current_age - lifetime`）、`allowed_seconds`（服务端许可与
调用方上限中更严格者）、`server_limit`、`caller_limit`。兜底结果**不会**
伪装成新鲜命中或成功取件：`source` 为 `STALE_IF_ERROR`、`stale=True`，
`ok=False`（仅该表示本身的状态仍为 200，可用 `status` 判断）。

### stale-if-error 故障兜底（按次启用）

上游偶发超时或 503 时，部分采集任务宁可使用有明确时效上限的旧目录。给
`get(..., stale_if_error=秒数)` 即可只为这一次调用启用兜底，默认关闭：

* 仅对**连接失败、超时、连接重置、断流**，以及 HTTP **500/502/503/504**
  生效。超限（`OversizeError`）、长度不符（`LengthMismatchError`）、无效
  响应元数据/其他协议错误（`ProtocolError`）、非法分帧
  （`MalformedResponseError`）以及上述以外的状态码，一律按原失败结果处理。
* 响应必须在声明中带有 `Cache-Control: stale-if-error=秒`，且不能含
  `no-store`、`no-cache` 或 `must-revalidate`；没有许可就不兜底。
* 允许的过期时长取**服务端 `stale-if-error` 与调用方上限中更严格者**；
  实际过期秒数（`current_age - lifetime`）**等于**上限仍可用，超过一点
  即不可用。
* 旧正文必须仍匹配本次请求在当前 Vary 规则下的取值，绝不跨语言/区域
  返回别的正文；规则或表示被替换后也不会复活。
* 兜底只读：不延长新鲜寿命、不改写存储时间/元数据，也不覆盖上游错误
  信息（`network_status` 与 `stale_fallback` 都保留失败来源）。
* 共享同一次在途失败时，每个等待者按**失败发生后**的当前缓存状态与
  各自上限独立判断，不接收领头调用挑出的正文——同一次失败的等待者可能
  一个拿到旧目录、另一个拿到上游错误。
* 若等待期间 `refresh` 已发布新表示，迟到失败会重新选中该新表示（新鲜
  命中）或报告原上游失败，绝不会让被替换的旧表示重新可用。

### 失败语义（异常即明确失败，绝不把错误缓存成成功）

| 异常 | 含义 |
| --- | --- |
| `RequestRejected` | 调用方请求被拒绝：Authorization/Cookie/Range/自带条件头等禁用头、重复头名、URL 带用户信息、非 http/https、头值含控制字符、非法 `stale_if_error` 参数。未产生任何网络/缓存动作 |
| `ProtocolError` | 服务器违反协议子集：非法或冲突的寿命（`max-age=-1`、重复 `max-age`、非法/重复 `stale-if-error`）、非法 ETag、非法 Date/Age、非 identity 的 Content-Encoding、冲突 Content-Length、304 无前件/标签不符/Vary 集合不同。**此前缓存保留**，本次失败不冒充成功 |
| `NetworkError` | 线上层失败的基类。子类见下 |
| `TransportError` | 连接失败、超时、连接被重置、断流（截短）。唯一允许被显式启用的 stale-if-error 兜底的失败族 |
| `OversizeError` | 响应超过 `max_bytes`。不入缓存、不被兜底掩盖 |
| `LengthMismatchError` | 正文长度与合法的声明 `Content-Length` 不符。不入缓存、不被兜底掩盖 |
| `MalformedResponseError` | 对端返回本客户端不支持的 HTTP 分帧。不被兜底掩盖 |
| `CacheClosed` | 客户端已关闭 |

除 200 与符合条件的 304 外，其他 HTTP 状态原样交给调用方（`source=NETWORK`），
不保存、不自动返回过期数据、不跟随重定向。`NetworkError` 仍然捕获全部
线上层失败（子类均继承它），既有调用方无需改动。

## 支持的协议子集

* 仅无凭据 HTTP/HTTPS GET；拒绝 `Authorization`、`Cookie`、`Range`、
  调用者自带条件头（`If-*`）及传输层头；拒绝重复请求头名。
  `Accept-Encoding` 由客户端固定为 `identity`（调用者同名头被覆盖）。
* URL 身份：scheme/host 小写化、去默认端口（80/443）、去 fragment；
  path/query 原样参与身份，不改写查询参数。
* 响应大小受 `max_bytes` 限制并核对声明的 `Content-Length`。
* `Vary`：字段名忽略大小写、去重；值按调用方单个字符串精确比较，
  缺失与显式空串不同；不做语言/列表语义合并。`Vary: *` 不保存不共享。
  200 改变某 URL 的 Vary 字段集合时，旧集合下所有表示立即失去复用资格
  （epoch 递增并删除），迟到的旧响应不能复活已淘汰的规则或表示。
* `Cache-Control`：`s-maxage`（优先）、`max-age`、`no-cache`（可保存但每次
  使用须验证）、`no-store`（不保存不共享；304 声明时本次仍返回已验证正文，
  随后删除该表示）、`must-revalidate`（禁止 stale-if-error）、
  `stale-if-error`（允许在指定秒数的过期范围内、且调用方按次许可时返回
  旧正文，见上）。无显式寿命必须验证，不做启发式缓存。
* ETag：支持一个合法实体标签（含弱标签），原样放入 `If-None-Match`；
  不支持 Last-Modified 回退。ETag 只属于产生它的那个表示，不会把同 URL
  其他语言的 ETag 发给服务器。
* 新鲜度：`initial_age = max(max(0, r-Date), Age+(r-q))`，
  `current_age = initial_age + max(0, now-r)`；仅 `current_age` 严格小于
  寿命且未要求每次验证时直接复用。Date 缺失用 `r`，Age 缺失为 0。
  重开后若时钟早于保存的 `r`，保守地要求验证。
* 304：必须对应当次送出的 `If-None-Match` 与当时选中的表示；带 ETag 必须
  一致；Vary 集合不同则拒绝。生效 304 保留正文、覆盖本次提供的
  Cache-Control/Date/Age 等元数据（缺失字段保留原值），年龄按本次交换
  重新计算。正文与合并后元数据同事务可见。

## 在途合并规则

* Vary 未知：同 URL 且**完全相同**请求头的并发普通调用只发一次 GET；
  请求头不同的调用各自取件。
* Vary 已知：规则证明选中同一表示的调用共用一次取件/重验证；无关头
  （如 `X-Trace`）取值不同不妨碍复用。不同表示的请求并行进行，没有
  持有到响应结束的 URL 总锁。
* 等待者被唤醒后按最终响应的 Vary 与可共享性重新判断：Vary 改变导致
  不匹配、或响应为 no-store / `Vary:*` 时，独立取件而不是收下他人正文。
* `refresh` 发布新正文后，旧表示的在途 304 到达不会覆盖新正文/新头，
  也不会恢复已撤销的 Vary 规则；普通调用重新选择当前表示。

## 演练与测试

`repcache/scrape_demo.py` 用本地双语目录服务（`repcache/demo_server.py`，
含服务端接收日志与同步闸门）逐步演练：冷请求合并、语言拆分、已知 Vary
的验证合并与跨语言并行、Vary 变更后区域等待者重新取件、refresh 与迟到
304 竞态、no-store/`Vary:*` 不共享、Age 到期等号、no-cache 每次验证、
304 改写寿命、304 无前件/错标签、断流重试、关闭重开复用，以及
stale-if-error 兜底（默认关闭、服务端/调用方上限取更严格者、过期边界
等号与越界、503/500 与传输失败、其他状态与协议/超限失败不兜底、共享
失败的两个等待者因上限不同得到不同结果、refresh 替换与迟到失败交错、
上游恢复后重新取件并按新响应更新缓存）。所有出网断言均针对服务端接收
日志，时间由注入时钟推进。

`tests/` 为 unittest 套件（68 例），同样只访问 127.0.0.1；其中传输层
兜底用脚本化的本地套接字服务制造连接重置、分块流截断与非法分帧。运行
产生的缓存库都在临时目录，不随仓库提交。
