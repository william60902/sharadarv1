# Sharadar 官方文件來源索引

本檔是 `sharadarv1` 的官方文件入口與可重現來源清單。內容只索引、分類並摘要 Sharadar 公開文件；不保存 API key、不呼叫訂閱資料，也不把官方文件大段逐字複製進 repository。

## 快照資訊

- 抓取日期：2026-08-31（Asia/Taipei）
- 文件入口：https://sharadar.com/docs/intro
- 官方站台：https://sharadar.com
- 驗證方式：未登入、未帶 API key 的公開 HTTP GET
- 結果：下列 36 個 sitemap `/docs` URL 均回傳 HTTP 200；19 個 canonical 文件頁可公開讀取。

## 官方發現來源

| 來源 | 用途 | 公開狀態 | 本次 SHA-256 |
| --- | --- | --- | --- |
| https://sharadar.com/robots.txt | 宣告全站允許一般 crawler，並指向 sitemap | HTTP 200 | `d19454663a44262b8f9913e235c7737a477d2006bca97e536f5603e0f310daa7` |
| https://sharadar.com/sitemap.xml | sitemap index，指向 `sitemap-0.xml` | HTTP 200 | `053d511bd0ecf68234059656dd64101d2c36aa4ee0dae8ea006ae87644c293ec` |
| https://sharadar.com/sitemap-0.xml | 完整站台 URL 清單；本索引的 docs URL 主來源 | HTTP 200 | `f40f5d985a77992c3c585e30252568ce090b1eb4176b83aae223b2f008082299` |
| https://sharadar.com/llms.txt | 官方機器可讀導覽、table/legacy code 對照與 API 概覽 | HTTP 200 | `26c88187523e943a8d5f17e19f63504385ecb1236730fe1e226b7997d1374f2c` |
| https://sharadar.com/docs | 文件根目錄；HTML `meta refresh` 導向 `/docs/intro` | HTTP 200 | 動態頁，不固定雜湊 |

> Sitemap 在本次抓取時把 docs URL 標為 `changefreq=daily`，`lastmod` 約為 `2026-08-30T03:20:04.969Z`。雜湊只用來識別本次公開來源快照，不代表上游內容不會更新。

## Canonical 文件結構

### API 使用方式

| 官方 URL | 文件角色 | 公開狀態 |
| --- | --- | --- |
| https://sharadar.com/docs/intro | API 與資料表總覽、快速入口 | HTTP 200，免 API key |
| https://sharadar.com/docs/auth | API key 驗證方式 | HTTP 200，免 API key |
| https://sharadar.com/docs/getting-started | 查詢參數、filter、format、limit 與範例 | HTTP 200，免 API key |
| https://sharadar.com/docs/bulk | 5 年、10 年與 full-history bulk ZIP 下載流程 | HTTP 200，免 API key；實際資料受訂閱限制 |
| https://sharadar.com/docs/faqs | ticker 變更、adjustment、survivorship bias 與 schema 常見問題 | HTTP 200，免 API key |

### Reference 與 fundamentals

| 官方 URL | Table / 主題 | 文件角色 | 公開狀態 |
| --- | --- | --- | --- |
| https://sharadar.com/docs/descriptions | `descriptions` | 指標與欄位字典 | HTTP 200，免 API key |
| https://sharadar.com/docs/tickers | `tickers` | securities master、ticker metadata、stable identity | HTTP 200，免 API key |
| https://sharadar.com/docs/fundamentals | `fundamentals` / `SF1` | 公司財報與基本面資料 | HTTP 200，免 API key |
| https://sharadar.com/docs/daily | `daily` | 每日基本面快照 | HTTP 200，免 API key |
| https://sharadar.com/docs/actions | `actions` | 股利、拆併股、ticker 變更等公司行動 | HTTP 200，免 API key |
| https://sharadar.com/docs/events | `events` | 重大公司事件 | HTTP 200，免 API key |
| https://sharadar.com/docs/sp500 | `sp500` | S&P 500 歷史成分 | HTTP 200，免 API key |

### Prices

| 官方 URL | Table / legacy code | 文件角色 | 公開狀態 |
| --- | --- | --- | --- |
| https://sharadar.com/docs/stocks | `stocks` / `SEP` | 美股 EOD OHLCV 與調整後價格 | HTTP 200，免 API key |
| https://sharadar.com/docs/funds | `funds` / `SFP` | 基金 EOD 價格 | HTTP 200，免 API key |
| https://sharadar.com/docs/metrics | `metrics` | price-based metrics | HTTP 200，免 API key |

### Investors

| 官方 URL | Table / legacy code | 文件角色 | 公開狀態 |
| --- | --- | --- | --- |
| https://sharadar.com/docs/insiders | `insiders` / `SF2` | insider transactions | HTTP 200，免 API key |
| https://sharadar.com/docs/holdings | `holdings` / `SF3` | institutional holdings 明細 | HTTP 200，免 API key |
| https://sharadar.com/docs/holdings-ticker | `holdings_ticker` / `SF3A` | 依 security 彙總的 institutional holdings | HTTP 200，免 API key |
| https://sharadar.com/docs/holdings-investor | `holdings_investor` / `SF3B` | 依 investor 彙總的 institutional holdings | HTTP 200，免 API key |

## Sitemap 中的 legacy/相容 URL

這些 URL 不是額外資料表。它們均回傳 HTTP 200，HTML canonical tag 指回右欄的現代文件頁。

| Sitemap URL | Canonical 文件 |
| --- | --- |
| `https://sharadar.com/docs/SF1`, `https://sharadar.com/docs/sf1` | https://sharadar.com/docs/fundamentals |
| `https://sharadar.com/docs/SEP`, `https://sharadar.com/docs/sep` | https://sharadar.com/docs/stocks |
| `https://sharadar.com/docs/SFP`, `https://sharadar.com/docs/sfp` | https://sharadar.com/docs/funds |
| `https://sharadar.com/docs/SF2`, `https://sharadar.com/docs/sf2` | https://sharadar.com/docs/insiders |
| `https://sharadar.com/docs/SF3`, `https://sharadar.com/docs/sf3` | https://sharadar.com/docs/holdings |
| `https://sharadar.com/docs/SF3A`, `https://sharadar.com/docs/sf3a` | https://sharadar.com/docs/holdings-ticker |
| `https://sharadar.com/docs/SF3B`, `https://sharadar.com/docs/sf3b` | https://sharadar.com/docs/holdings-investor |
| `https://sharadar.com/docs/indicator`, `https://sharadar.com/docs/indicators` | https://sharadar.com/docs/descriptions |

## 完整性結論

- `/docs/intro` 的側欄列出 19 個 canonical docs 頁面。
- `sitemap-0.xml` 另外列出 `/docs` 根頁及 16 個 legacy/相容 alias，合計 36 個 `/docs` URL。
- `/docs` 是 HTML meta refresh，不是 HTTP 3xx；抓取器應直接以 `/docs/intro` 為入口。
- Legacy URL 目前直接回 HTTP 200 並以 canonical tag 指向現代頁；pipeline 內仍應使用現代 table 名稱，把 legacy code 僅當相容別名。
- 文件頁與 sitemap 都是公開來源；資料下載權限是另一層。能讀 docs 不代表能下載訂閱資料。

## 可重現檢查

以下命令只讀公開 metadata，不含 API key：

```bash
curl --fail --show-error --location --compressed https://sharadar.com/robots.txt
curl --fail --show-error --location --compressed https://sharadar.com/sitemap.xml
curl --fail --show-error --location --compressed https://sharadar.com/sitemap-0.xml
curl --fail --show-error --location --compressed https://sharadar.com/llms.txt
```

從 sitemap 重建 docs URL 清單：

```bash
curl --fail --show-error --location --compressed https://sharadar.com/sitemap-0.xml \
  | grep -o 'https://sharadar.com/docs[^<]*' \
  | sort -u
```

更新此索引時，應重新核對 sitemap、canonical tag、HTTP status 與抓取日期；不要假設 2026-08-31 的 URL 集合永久不變。
