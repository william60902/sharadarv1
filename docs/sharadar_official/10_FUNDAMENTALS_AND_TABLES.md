# Sharadar Fundamentals 與核心輔助表：官方語意摘要

更新日期：2026-08-31
資料來源範圍：只使用 Sharadar 官方文件與官方資料頁。本文是工程摘要，不是官方文件的鏡像，也不包含任何 API credential。

## 1. 先講最重要的結論

對歷史因子回測而言，SF1 `fundamentals` 的基礎輸入應以 **ARQ / ARY / ART** 為主：

- `AR` 是 As Reported，排除後續 restatement，並以 Form 10 送交 SEC 的日期建立時間索引。
- `MR` 是 Most-Recent Reported，包含 restatement，並以財務／報告期間建立時間索引。
- 因此，`MRQ / MRY / MRT` 適合看「目前已知的最新重編財務狀態」，不適合直接拿來重建當時市場可知資訊。
- 原始 AR 與 MR 必須分開保存，不能用 MR 覆寫 AR。

Sharadar 自己也把 AR 描述為通常適合 backtesting；MR 則較適合在合併、分拆或重編之後評估公司的最新營運狀態。

還有一個重要限制：AR 的時間點是 Form 10（10-Q / 10-K）正式提交日，但相同資訊可能已在更早的 Form 8（8-K）或新聞稿公布。換句話說，AR 是可靠且保守的 filing-date PIT，不一定是資訊第一次進入市場的最早時刻。

官方來源：

- [Fundamentals / SF1](https://sharadar.com/docs/fundamentals)
- [Material Corporate Events](https://sharadar.com/docs/events)

## 2. 六種 reporting dimensions

| 維度 | 期間長度 | 是否 As Reported | Restatement 行為 | 回測角色 |
|---|---|---:|---|---|
| `ARQ` | 單季 | 是 | 排除後續重編 | 單季財務因子 PIT 基礎 |
| `ARY` | 年度 | 是 | 排除後續重編 | 年度財務因子 PIT 基礎 |
| `ART` | 滾動十二個月 | 是 | 排除後續重編 | TTM 財務因子 PIT 基礎 |
| `MRQ` | 單季 | 否 | 反映目前最新重編 | 研究／對帳，不直接作歷史 PIT |
| `MRY` | 年度 | 否 | 反映目前最新重編 | 研究／對帳，不直接作歷史 PIT |
| `MRT` | 滾動十二個月 | 否 | 反映目前最新重編 | 研究／對帳，不直接作歷史 PIT |

時間跨度的官方定義：

- `Y`：一年長度的年度觀察值。
- `T`：每季一筆、每筆涵蓋一年的 trailing-twelve-month 觀察值。
- `Q`：單季長度；官方註明只適用於美國本土公司，外國公司沒有 Quarterly 維度。

AR 可能在同一季出現多筆 observation，因為同季可能提交不只一份 filing。少數延遲申報案例也可能缺季；若公司同日提交多份文件追趕落後期數，Sharadar 表內可能只留下該日最新 reporting period。pipeline 因此不能假設每家公司永遠「一季恰好一筆」。

## 3. `fundamentals`（SF1）

### 3.1 定位與覆蓋

- 超過 100 個基本面指標。
- 官方表示覆蓋接近 18,000 家 active 與 delisted 美國上市公司。
- 涵蓋 Nasdaq、NYSE、NYSEMKT 的主要普通股類別。
- 歷史自 1998-01 起。
- 每日約在美東 17:30 與 23:30 更新，官方 reporting lag 小於一天。
- 官方稱資料約 99% survivorship-bias free；工程上仍應保存 delisted 名單與 acquisition/delisting actions，不能只用今天仍上市的 ticker 反推歷史 universe。

### 3.2 識別欄位與日期欄位

官方 metadata 將以下四欄標為 primary key：

```text
ticker + dimension + date + reportperiod
```

| 欄位 | 官方名稱 | 在 pipeline 的正確理解 |
|---|---|---|
| `ticker` | Ticker Symbol | 可變的市場代碼；不應是跨 ticker-change 的永久 subject id。 |
| `dimension` | Dimension | `ARQ/ARY/ART/MRQ/MRY/MRT`；必須進入 artifact identity。 |
| `date` | Date Key | AR 維度下是 Form 10 提交日／PIT 可用日期；MR 維度按 reporting period 索引。不可在未看 dimension 時把 `date` 解讀成同一種日期。 |
| `reportperiod` | Report Period | 公司實際財務報告期末；它描述數字屬於哪個期間，不代表市場當時已知。 |
| `calendardate` | Calendar Date | Sharadar 的 calendar-date 欄位，可用於查詢／對齊；不應替代 AR `date` 作資訊可用日。 |
| `fiscalperiod` | Fiscal Period | 公司會計年度／季度標籤。官方 metadata 的 type 標示與官方樣本外觀可能不同，落地時應以實際 schema 與 canary 驗證。 |
| `lastupdated` | Last Updated Date | 供找出 vendor 最近變更的紀錄；是 ingestion watermark，不是市場資訊可用日。 |

最後兩句中的工程判斷很重要：

- **PIT join key 是 AR `date`，不是 `reportperiod`、`calendardate` 或 `lastupdated`。**
- **`lastupdated` 只回答「Sharadar 何時更新這列」，不回答「市場何時知道這份財報」。**

### 3.3 指標形態

表內同時包含：

- 三大表原始／標準化項目，例如 `revenue`、`gp`、`assets`、`debt`、`equity`、`ncfo`、`capex`。
- 比率與 margin，例如 `grossmargin`、`netmargin`、`roa`、`roe`、`roic`。
- 每股欄位，例如 `eps`、`bvps`、`fcfps`。
- 美元轉換欄位，例如 `revenueusd`、`equityusd`，並提供 `fxusd`。
- 與價格結合的欄位，例如 `marketcap`、`ev`、`pe`、`pb`、`ps`。

這代表 `fundamentals` 不完全等於「未加工 SEC line items」。原始抓取層應完整保留 vendor row；Factor PDATA 再明確記錄用了哪個 dimension、欄位與公式，不把 vendor ratio 和 Medina 自算 ratio 混成同一個 measurement。

### 3.4 Bulk 與 incremental

- 官方提供 5 年、10 年與 full-history 壓縮 CSV bulk download；下載會 redirect 到有時效的 URL。
- API 查詢預設 `limit=10000`，可用 `skip`／`offset` 分頁。
- `lastupdated.gte=...` 可用於抓最近被修改的列。
- `from`／`to` 在 fundamentals 文件中描述為 filing-date 範圍。

建議 ingest contract：

1. 初始 full-history bulk 原封不動存成 immutable capture。
2. 每日以 `lastupdated` watermark 抓增量，採 overlap window，不能只記單一最大日期後做嚴格大於。
3. 依官方 composite primary key upsert normalized table，但保留 ingest timestamp、來源檔 hash 與 capture id。
4. 定期重新抓 bulk 進行 reconciliation；不要把 bulk 檔直接覆蓋成唯一歷史證據。

以上 1–4 是依官方介面做出的 Medina 工程設計，不是 Sharadar 的原文要求。

## 4. `daily`（Daily Fundamentals）

### 4.1 定位

這張表提供每日的基本面／價格混合指標：

- `marketcap`
- `ev`
- `pe`
- `ps`
- `pb`
- `evebit`
- `evebitda`

官方明確把它描述為 Point-in-time / As-Reported。覆蓋 fundamentals universe，歷史自 1998-12，約在美東 19:00 每日更新，reporting lag 小於一天。

Primary key 是：

```text
ticker + date
```

其中 `date` 的欄位名稱是 Price Date；`lastupdated` 仍是 vendor 更新 watermark。估值欄位的官方單位中，`marketcap` 與 `ev` 是 USD millions，比率欄位則是 ratio，落地時不可和 fundamentals 表內標示為 USD 的同名欄位未經單位正規化就直接 union。

### 4.2 使用邊界

- 它適合快速取得每日 PIT valuation factor input。
- 它不是財報原始明細，不能取代 ARQ/ARY/ART。
- 同名 valuation 若也由價格和 AR fundamentals 自算，必須用不同 measurement id 並保留 formula lineage。
- 官方 daily 文件把 `from/to` 文字描述成「filing date」，但表內日期欄明確叫 Price Date；這很可能是文件模板用語。正式大量抓取前只需做一個最小 AAPL date-range canary 確認實際過濾行為。

官方來源：[Daily Fundamentals](https://sharadar.com/docs/daily)

## 5. `tickers`（Securities Master）

### 5.1 定位

`tickers` 是 Sharadar 各資料集的 securities master，涵蓋 active 與 delisted securities，以及 fundamentals、stocks、funds、insiders、holdings 相關 entities。歷史覆蓋說明自 1990-06，資料每日更新兩次，reporting lag 小於一天。

官方 metadata 將下列欄位標為 primary key：

```text
table + permaticker + ticker
```

`permaticker` 被官方定義為 Sharadar 特有、對 issuer 唯一且不變的識別碼。因此：

- Medina normalized layer 應以 `permaticker` 作穩定 issuer identity。
- `ticker` 保留為時點市場代碼，不可假設永遠不變。
- `table` 表示該筆 identity 屬於哪個 Sharadar table universe。

### 5.2 可用 metadata

核心欄位包括 exchange、delisted flag、CUSIP、FIGI、SIC code/sector/industry、Fama industry、sector、industry、currency、location、related tickers、first/last price date、first/last quarter、SEC filings URL 與公司網站。

### 5.3 PIT 限制

官方明確說 tickers bulk 是一張 **snapshot**：要求 5、10 或 full 都會拿到相同完整快照。文件沒有提供 sector／industry／exchange 等欄位的歷史版本維度。

因此：

- 不能直接把今日 tickers snapshot 的 sector/industry 當成 10 年前的 PIT industry classification。
- 若需要 industry-neutral historical backtest，必須將每日/每次 tickers capture 版本化，或使用另有歷史有效期的分類來源。
- `lastupdated` 適合偵測現在有哪些 metadata 被 vendor 修改，但無法單獨重建修改前的值；只有自行保存歷次 snapshot 才能做到。

官方來源：[Tickers and Metadata](https://sharadar.com/docs/tickers)

## 6. `actions`（Corporate Actions）

### 6.1 定位與內容

官方列出的內容包括：

- ticker changes
- stock splits
- cash dividends
- spinoffs
- ADR ratio changes
- listing / delisting dates 與 delist reasons
- acquisition counterparties
- 同一 issuer 不同 securities 之間的關係

涵蓋 fundamentals、stocks、funds universe 的 active 與 delisted tickers，歷史自 1998-01，每日約在美東 17:30 與 23:30 更新。

欄位很精簡：`date`、`action`、`ticker`、`name`、`value`、`contraticker`、`contraname`。對 acquisition 等雙方事件，`contraticker`／`contraname` 用來表示對手方。

### 6.2 工程角色

- 建立 ticker identity bridge 與 corporate-action lineage。
- 說明 delisting、acquisition、spinoff 等 universe 進出原因。
- 支援 price/share adjustment 的事件對帳。
- 不應只抓 split/dividend；否則 ticker changes 與 delisted 名單仍會造成歷史 universe identity 斷裂。

官方文件沒有列出 `lastupdated` filter。因此增量機制不可照搬 fundamentals；可以按日期 overlap 重抓，並以官方 primary-key 欄位／完整 row hash 去重，另安排低頻 bulk reconciliation。

官方來源：[Corporate Actions](https://sharadar.com/docs/actions)

## 7. `events`（Material Corporate Events）

### 7.1 定位

`events` 來自 SEC Form 8-K，涵蓋 fundamentals universe 中有向 SEC 提交相關資訊的 active 與 delisted tickers。歷史自 2004-01，每日約在美東 19:00 更新，reporting lag 小於一天。

Primary key：

```text
ticker + date
```

payload 只有 `eventcodes`，所以它是事件索引，不是完整 8-K 文本，也不是財務數值明細。

### 7.2 與 PIT fundamentals 的關係

- `events` 可以用來標示某日有 8-K material event。
- 它能協助研究「Form 10 filing date 之前是否已有事件披露」。
- 但只有 event code 不能證明某個 revenue／EPS 數值已在該日完整公開，也不能直接把 AR 財務值的 availability date 往前移。
- 若未來要做 earnings-announcement-time PIT，需要另外取得 8-K 本文／附件與可引用數值，再建立獨立 lineage。

官方文件同樣沒有列出 `lastupdated` filter；建議採 date overlap + row hash + 定期 bulk reconciliation。

官方來源：[Material Corporate Events](https://sharadar.com/docs/events)

## 8. 五張表如何一起用

```text
tickers.permaticker
  └─ 穩定 issuer identity
       ├─ fundamentals ARQ/ARY/ART
       │    └─ filing-date PIT 財務 measurement
       ├─ daily
       │    └─ 每日 PIT valuation measurement
       ├─ actions
       │    └─ ticker / split / dividend / delisting / M&A lineage
       └─ events
            └─ 8-K material-event index
```

建議 normalized relationship：

1. 先以 tickers 建 `ticker ↔ permaticker` identity map，但要對 snapshot 做版本化。
2. fundamentals 原始列完整保存；回測 consumption view 預設只暴露 AR 維度。
3. daily 另存為每日 vendor-derived valuation，不混入季度 fundamentals row。
4. actions 建 event-sourced identity/corporate-action table。
5. events 建 filing-event flag；未取得文件值以前不修改 fundamentals availability date。

## 9. 不可踩的 PIT 錯誤

1. 用 `MRQ/MRY/MRT` 回填歷史日期，造成 restatement look-ahead。
2. 用 `reportperiod` 當市場可用日；財報期末通常早於公開日。
3. 用 `lastupdated` 當市場可用日；它是 vendor maintenance watermark。
4. 把今天的 sector/industry snapshot 套到全部歷史日期。
5. 只保存 ticker，不保存 `permaticker` 與 ticker-change actions。
6. 把 `events.date` 自動當作所有財務數值的可用日。
7. 看到同一季多筆 AR 就任意 deduplicate；這可能是多次 filing，而非髒資料。
8. 未檢查單位就把 fundamentals 與 daily 的同名欄位直接拼接。

## 10. 目前可直接採用的 ingestion defaults

| 決策 | 預設 |
|---|---|
| 歷史基本面回測 dimension | `ARQ`, `ARY`, `ART` |
| MR dimensions | 保存但標為 non-PIT consumption，供最新狀態與 reconciliation |
| Issuer ID | `permaticker` |
| Fundamental availability | AR `date` |
| Incremental watermark | `lastupdated`，加 overlap window |
| Industry classification | tickers snapshot 版本化；未證明前不稱歷史 PIT |
| 8-K events | 事件 flag；不自動前移財務值 availability |
| Raw storage | immutable capture + checksum + fetched_at |
| Normalized storage | 保留 source table、dimension、所有日期欄與 lineage |

## 11. 官方參考連結

- [Sharadar Documentation Introduction](https://sharadar.com/docs/intro)
- [Fundamentals / SF1](https://sharadar.com/docs/fundamentals)
- [Daily Fundamentals](https://sharadar.com/docs/daily)
- [Tickers and Metadata](https://sharadar.com/docs/tickers)
- [Corporate Actions](https://sharadar.com/docs/actions)
- [Material Corporate Events](https://sharadar.com/docs/events)
- [Querying the Data](https://sharadar.com/docs/getting-started)
- [Authentication](https://sharadar.com/docs/auth)
