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

* `get(url, headers=None) -> FetchResult` — 普通调用：命中新鲜表示直接
  返回；过期表示按 ETag 条件重验证（无 ETag 用无条件 GET 替换）；无表示
  时取件。并发调用按在途键合并（见下）。
* `refresh(url, headers=None) -> FetchResult` — 管理员操作：永远发无条件
  GET，不加入早已开始的普通取件，可与其并行；成功 200 按常规规则发布。
* `close()` / 上下文管理器 — 关闭缓存库。
* `clock` 可注入（测试用假时钟推进时间），生产默认 `time.time`。

### FetchResult 字段

| 字段 | 含义 |
| --- | --- |
| `status` | 表示的有效状态（缓存命中/304 确认均为 200；未保存的状态如 404 为线上状态） |
| `body` | 完整响应正文，可直接交给解析程序；304 确认返回的是已存正文，**不会**是空正文 |
| `headers` | 有效表示头（304 合并后；逐跳头已剔除） |
| `source` | `NETWORK`（本次网络 200）/ `CACHE`（仍新鲜的本地表示）/ `REVALIDATED`（304 确认后的旧正文） |
| `network_status` | 本次实际线上状态（200/304/其他），未发请求为 `None` |
| `validated` | 本次是否发生了生效的条件重验证 |
| `vary` | 所选表示的 Vary 字段名元组 |
| `age` | `AgeInfo`：`q`/`r`/`now`/`date_epoch`/`age_header`/`initial_age`/`current_age`/`lifetime`/`must_validate` |
| `from_cache` / `ok` / `text` | 便捷属性 |

### 失败语义（异常即明确失败，绝不把错误缓存成成功）

| 异常 | 含义 |
| --- | --- |
| `RequestRejected` | 调用方请求被拒绝：Authorization/Cookie/Range/自带条件头等禁用头、重复头名、URL 带用户信息、非 http/https、头值含控制字符。未产生任何网络/缓存动作 |
| `ProtocolError` | 服务器违反协议子集：非法或冲突的寿命（`max-age=-1`、重复 `max-age`）、非法 ETag、非法 Date/Age、非 identity 的 Content-Encoding、冲突 Content-Length、304 无前件/标签不符/Vary 集合不同。**此前缓存保留**，本次失败不冒充成功 |
| `NetworkError` | 超时、断流（截短）、声明长度不符、响应超限。截短/超限正文**不入缓存**；协调状态释放并唤醒全部等待者 |
| `CacheClosed` | 客户端已关闭 |

除 200 与符合条件的 304 外，其他 HTTP 状态原样交给调用方（`source=NETWORK`），
不保存、不自动返回过期数据、不跟随重定向。

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
  随后删除该表示）。无显式寿命必须验证，不做启发式缓存。
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
304 改写寿命、304 无前件/错标签、断流重试、关闭重开复用。所有出网断言
均针对服务端接收日志，时间由注入时钟推进。

`tests/` 为 unittest 套件（31 例），同样只访问 127.0.0.1。运行产生的
缓存库都在临时目录，不随仓库提交。
