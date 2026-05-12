# isdict-data

`isdict-data` 是 isdict 词典系统的共享数据访问层模块，提供面向进程内调用的词典查询能力，供 `isdict-api` 和 `isdict-web` 复用。它从原有服务中提取并稳定化了数据访问、业务编排、查询参数校验和 PostgreSQL 扩展检查逻辑，避免 SSR 场景再经过一次 HTTP 跳转。

模块路径：`github.com/simp-lee/isdict-data`

## 设计目标

- 通过 Go 包直接暴露词典查询能力，不引入 HTTP、日志、外部通用缓存等运行时耦合。
- 保持当前查询语义清晰，包括词头解析、变体匹配、搜索排序、去重和 pg_trgm 模糊匹配行为。
- 非测试代码的直接依赖收敛到 `github.com/simp-lee/isdict-commons`、`gorm.io/gorm` 与 PostgreSQL 数组扫描所需的 `github.com/lib/pq`。
- 遵循 Go 的常见约定：接收接口，返回具体类型；接口由使用方按需定义。

## 包结构

### `repository`

负责词典数据读取与查询实现。

- 导出 `WordRepository` 接口，包含 12 个数据访问方法。
- 导出 `NewRepository(db *gorm.DB) *Repository`。
- 导出 `BatchVariantMatch`、`FeaturedCandidate`、`ErrWordNotFound`、`ErrVariantNotFound`。
- 基于 `isdict-commons v1.1.2` 的 21 张 active schema 表读取数据；active schema 不包含旧 `lexical_relations` 表。
- 完整词条路径会覆盖 import run、entry/source CEFR 证据、词源、中文摘要、IPA、音频、forms/aliases、sense gloss、sense labels、examples、entry-level school 内容和学习信号。
- 词汇关系只读取 `headword_relation_edges`，来源为 Open English WordNet 2025 Edition，是 headword + POS 级别关系。
- School 数据源只作为学习信号和 entry-level 内容来源：`entry_learning_signals.school_level/school_run_id`、`entry_definitions(source='school')`、`entry_examples(source='school')`。School 原始 synonyms/antonyms/related/memory/exam 等字段不进入词汇关系模块。
- 保持现有查询行为，包括：
  - `GetWordByHeadword` 的精确匹配、规范化匹配、entry_forms 解析链路。
  - `GetEntryGroupByHeadword` 按 normalized headword 返回同形不同词性的多个 entries，并按 POS 读取 OEWN 关系分组。
  - `SearchWords`、`SuggestWords`、`SearchPhrases` 基于 `entry_search_terms` read model 的匹配优先级与稳定排序，并纳入 `school_level` 学习信号。
  - 基于 PostgreSQL `pg_trgm` 扩展的模糊搜索能力。
  - `ListFeaturedCandidates` 基于上游 `featured_candidates` read model 按质量排序一次性枚举 featured 候选 entry；候选资格由 `isdict-commons` 统一定义，本包不在运行时重建或扩大候选池。

### `service`

负责业务编排、返回模型转换、批量查询与参数限流。响应 DTO 由本包 `service` 提供；`isdict-commons v1` 不再导出 API 响应结构体。

- 导出 `WordService` 结构体及 12 个公开方法。
- 导出 `NewWordService(repo repository.WordRepository, cfg ServiceConfig) *WordService`。
- 通过变量别名重导出仓储层 not-found 错误：
  - `service.ErrWordNotFound`
  - `service.ErrVariantNotFound`
- 额外导出以下错误：
	- `service.ErrBatchLimitExceeded`
	- `service.ErrFeaturedLimitInvalid`
	- `service.ErrFeaturedSourceUnavailable`
	- `service.ErrFeaturedCandidatesExhausted`
	- `service.ErrFeaturedBatchIncomplete`
- 公开方法可按用途分为四组：
	- 词条查询：`GetEntryGroupByHeadword`、`GetWordByHeadword`、`GetHeadwordRelationGroups`、`GetWordsByVariant`、`GetWordsBatch`
	- 搜索与联想：`SearchWords`、`SuggestWords`、`SearchPhrases`
	- 明细读取：`GetPronunciations`、`GetSenses`
	- 首页推荐：`RandomFeaturedWords`、`RandomFeaturedPhrases`
- 使用极简配置结构体：

```go
type ServiceConfig struct {
	BatchMaxSize    int
	SearchMaxLimit  int
	SuggestMaxLimit int
}
```

默认值如下：

- `BatchMaxSize`: `100`
- `SearchMaxLimit`: `100`
- `SuggestMaxLimit`: `50`

其中 `BatchMaxSize` 同时约束批量查词和 featured 词条采样上限。

### `queryvalidation`

负责查询词的基础校验与批量输入清洗。

- 导出 `MinQueryLength`
- 导出 `TrimmedRuneCount`
- 导出 `NormalizedRuneCount`
- 导出 `NormalizeBatchWords`

其中 `NormalizeBatchWords` 会：

- 去除空白输入
- 对完全相同的裁剪后输入去重
- 保留大小写差异输入，例如 `Polish` 和 `polish`
- 保留分隔符差异输入，例如 `re-sign` 和 `resign`

### `postgresutil`

负责 PostgreSQL 必需扩展检查。

- 导出 `RequiredExtensionName`，当前固定为 `pg_trgm`
- 导出 `CheckRequiredExtensionPresent(ctx context.Context, db *sql.DB) error`
- 导出 `EnsureRequiredExtensionsEnabled(db *gorm.DB) error`

建议在服务启动阶段调用：先尝试启用扩展，再检查扩展是否可用。

## 快速接入

### 1. 添加依赖

```bash
go get github.com/simp-lee/isdict-data
```

消费端自己负责创建 `*gorm.DB`，因此通常还需要在应用侧引入 PostgreSQL 驱动：

```bash
go get gorm.io/driver/postgres
```

### 2. 初始化仓储与服务

```go
package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/simp-lee/isdict-data/postgresutil"
	"github.com/simp-lee/isdict-data/repository"
	"github.com/simp-lee/isdict-data/service"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

func main() {
	dsn := "host=127.0.0.1 user=isdict password=isdict dbname=isdict sslmode=disable"

	gormDB, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic(err)
	}

	if err := postgresutil.EnsureRequiredExtensionsEnabled(gormDB); err != nil {
		panic(err)
	}

	sqlDB, err := gormDB.DB()
	if err != nil {
		panic(err)
	}
	defer sqlDB.Close()

	if err := postgresutil.CheckRequiredExtensionPresent(context.Background(), sqlDB); err != nil {
		panic(err)
	}

	repo := repository.NewRepository(gormDB)
	wordService := service.NewWordService(repo, service.ServiceConfig{
		BatchMaxSize:    100,
		SearchMaxLimit:  100,
		SuggestMaxLimit: 50,
	})

	resp, err := wordService.GetEntryGroupByHeadword(context.Background(), "learn", service.EntryGroupOptions{
		IncludeVariants:       true,
		IncludePronunciations: true,
		IncludeSenses:         true,
	})
	if err != nil {
		if errors.Is(err, service.ErrWordNotFound) {
			fmt.Println("word not found")
			return
		}
		panic(err)
	}

	fmt.Println(resp.Headword, len(resp.Entries))
}
```

如果你的应用已经自己管理连接生命周期，不需要像示例那样在 `main` 中关闭 `sql.DB`。

## 常见调用方式

### Featured 候选池

```go
candidates, err := repo.ListFeaturedCandidates(ctx)
if err != nil {
	panic(err)
}

for _, candidate := range candidates {
	// 将高质量候选 entry 交给应用侧 featured 推荐池
	_ = candidate.EntryID
	_ = candidate.Headword
}
```

这个能力用于构建 featured 推荐候选池，直接读取上游 `featured_candidates` read model，按 `quality_rank, entry_id` 返回已筛选 entry，避免从全量词库中随机抽取。返回结构包含 `entry_id` 和 `headword`，service hydrate 时会按 `entry_id` 回填，避免同一 headword 下多 POS entry 被误选。最佳实践是把候选资格、同一 normalized headword 的唯一 entry 选择和质量排序都保留在 `isdict-commons` read model 里统一维护；在 commons v1.1.2 中，该候选池由上游根据 `frequency_rank`、`cefr_level` 或 `school_level` 信号选出每个 normalized headword 的 canonical candidate，本包只消费结果。

如果业务只是想拿首页推荐词或短语，不建议直接消费这份列表；应优先调用 `service.RandomFeaturedWords` / `service.RandomFeaturedPhrases`，由 service 层负责候选池缓存、分组和精确词头 hydrate。

### 获取词页 entry group

词页推荐优先使用 `GetEntryGroupByHeadword`：

```go
resp, err := svc.GetEntryGroupByHeadword(ctx, "head", service.EntryGroupOptions{
	IncludeVariants:       true,
	IncludePronunciations: true,
	IncludeSenses:         true,
})
```

该方法会按 normalized headword 返回所有 entries，例如 `head` 可以同时返回 noun、verb、adjective 等同形不同词性词条。`relation_groups_by_pos` 基于 `headword_relation_edges.source_headword_normalized + source_pos_code` 查询，关系展示在 POS section 层级；第一版不做 OEWN sense 与 Wiktionary sense 对齐。

默认词页只公开核心学习关系，顺序固定为：

- `synonym`
- `antonym`
- `hypernym`
- `hyponym`
- `meronym`
- `holonym`
- `similar_to`
- `also_see`

OEWN 中的 `derivation`、`pertainym`、`domain_topic`、`event`、`agent`、`undergoer` 等高级关系会保留在表中，但不会出现在默认词页响应里。Wiktionary/Kaikki 的 relation arrays 和 derived 不进入词汇关系模块，也不是关系数据来源。

### 获取单个词条

```go
resp, err := svc.GetWordByHeadword(ctx, "programmed", nil, true, true, true)
```

`GetWordByHeadword` 是单 entry 查询：它会按当前排序选择一个最佳 entry，适合只需要一个 entry 的明细读取，不适合作为词页主 API。对于 `head`、`bank`、`light` 这类同形多词性词页，应使用 `GetEntryGroupByHeadword`，避免只展示一个 entry。

当输入既不是主词头，也不是任何变体时，返回的错误可通过 `errors.Is(err, service.ErrWordNotFound)` 判断。

### 通过变体反查主词条

```go
kind := "form"
items, err := svc.GetWordsByVariant(ctx, "learnt", &kind, true, true)
```

`kind` 目前支持：

- `form`
- `alias`

### 批量查词

```go
items, meta, err := svc.GetWordsBatch(ctx, &service.BatchRequest{
	Words: []string{"learn", "learnt", "  learn  "},
})
```

批量查询会先清洗输入，再按主词头批量命中，未命中的部分通过 `entry_forms` 批量解析。超出配置上限时返回 `service.ErrBatchLimitExceeded`。

### 搜索与联想

```go
results, meta, err := svc.SearchWords(ctx, "learn", service.SearchOptions{Limit: 20})
suggestions, err := svc.SuggestWords(ctx, "lea", service.SuggestOptions{Limit: 10})
phrases, err := svc.SearchPhrases(ctx, "look", 10)
```

这些方法的参数约束分别是：

- `SearchWords`: 关键字最少 3 个规范化字符、最多 100 个裁剪后字符；`limit <= 0` 时默认 `20`，并受 `ServiceConfig.SearchMaxLimit` 限制；负 `offset` 会被重置为 `0`
- `SuggestWords`: 前缀最少 3 个规范化字符、最多 50 个裁剪后字符；`limit <= 0` 时默认 `10`，并受 `ServiceConfig.SuggestMaxLimit` 限制
- `SearchPhrases`: 关键字最少 1 个字符、最多 50 个字符；`limit <= 0` 时默认 `10`，最大固定为 `50`

`SearchWords` / `SuggestWords` 使用 `SearchOptions` / `SuggestOptions` 传入过滤与分页参数，支持 `POS`、`CEFRLevel`、`OxfordLevel`、`CETLevel`、`SchoolLevel`、`MaxFrequencyRank`、`MinCollinsStars` 等过滤。`SearchWords` 的 `POS`、`GetSenses` 的 `posCode` 和 `GetPronunciations` 的 `accentCode` 均使用 `isdict-commons/model` 中的文本 code，例如 `model.POSNoun`、`model.AccentBritish`。

实现细节上，搜索、联想和短语搜索都读取上游 `entry_search_terms` read model，不再对 `entries` / `entry_forms` 做运行时 trigram union；学习信号过滤也直接使用该 read model 中冗余的 `cefr_level`、`frequency_rank` 等字段。

### 首页 featured 词条与短语

```go
featuredWords, err := svc.RandomFeaturedWords(ctx, 6)
featuredPhrases, err := svc.RandomFeaturedPhrases(ctx, 4)
```

这两个方法都返回 `[]service.SuggestResponse`，并共享同一套错误语义：

- `service.ErrFeaturedLimitInvalid`: `limit <= 0` 或超过 `ServiceConfig.BatchMaxSize`
- `service.ErrFeaturedCandidatesExhausted`: 当前候选池不足以满足请求数量
- `service.ErrFeaturedBatchIncomplete`: 候选采样成功，但后续精确词头 hydrate 未返回完整结果
- `service.ErrFeaturedSourceUnavailable`: 加载候选池或读取词条时的上游故障

实现细节上，service 会按单词/短语分组缓存候选池，再对抽样结果按 `entry_id` 做精确 entry 回填；featured 请求只用 canonical headword 作为批量 hydrate key，不走 `entry_forms` 解析，避免返回错误 canonical headword 或错误 POS entry。

### 读取发音与释义

```go
pronunciations, err := svc.GetPronunciations(ctx, "learn", nil)
senses, err := svc.GetSenses(ctx, "learn", nil, "both")
```

`GetPronunciations` 会先解析词条，再按 `entry_id` 读取 IPA 发音；完整词条响应还会在 `pronunciation_audios` 中返回 commons v1 的音频文件记录。`GetSenses` 支持按 commons v1 的文本 POS code 过滤，`lang` 通常使用 `both`、`en`、`zh` 三个值控制释义和例句的语言裁剪，未命中这三个值时当前实现等价于保留双语字段。

## 返回字段说明

词条相关响应中的注解字段使用本包的 `service.WordAnnotations`。其中 `school_level` 为聚合学习阶段信号，不代表具体教材、出现次数或考试来源。响应会同时透传 `school_level_name` 和内部来源 `school_run_id`，当前约定如下：

- `0`: unknown
- `1`: 初中
- `2`: 高中
- `3`: 大学

`cefr_level`、`cet_level`、`oxford_level`、`collins_stars` 等学习信号按 commons v1 的数值 code 原样透传；`cefr_level_name` 只作为便捷展示字段。

完整词条响应会直接暴露 commons v1 新增数据，避免丢失上游信息：

- 词条级：`source_run`、`cefr_source_signals`、`etymology`、`entry_definitions`、`entry_examples`
- 发音级：`pronunciations` 和 `pronunciation_audios`
- 义项级：`definitions_en`、`definitions_zh`、`labels`、`examples`、`cefr_source_signals`
- 变体级：`variants` 中包含 `form_text`、`relation_kind`、`form_type`、`source_relations` 和 `display_order`

词页 group 响应额外包含：

- `entries`: 同一 normalized headword 下的多个 `WordResponse`，每个 entry 都包含 `pos` 和 `pos_name`
- `relation_groups_by_pos`: 按 POS 分组的 OEWN headword 关系，每个 relation type 默认最多 10 个本地已有 target entry
- `queried_variant`: 当输入通过 `entry_forms` 解析到 canonical headword group 时返回

`entry_definitions` / `entry_examples` 是 entry-level 内容，`sense_id` 可以为空；第一版不会强制把 school 内容与 Wiktionary sense 对齐展示。搜索和联想支持并使用 `school_level` 学习信号排序；`SearchWords` / `SuggestWords` 还提供可选 `SchoolLevel` 过滤。Featured 候选直接消费上游 `featured_candidates` 的筛选与质量排序结果，包含上游 materialized 的 `school_level` 信号，但不由本包重新判定候选资格。

## 错误语义

模块统一使用仓储层哨兵错误作为 not-found 来源，service 层只做重导出，不再做二次翻译。

```go
if errors.Is(err, service.ErrWordNotFound) {
	// 词条未找到
}

if errors.Is(err, service.ErrVariantNotFound) {
	// 变体未找到
}
```

由于 `service.ErrWordNotFound` 与 `repository.ErrWordNotFound` 指向同一实例，消费端按任一包判断都可以工作，但通常推荐上层只依赖 `service` 包。

## 与消费端的边界

`isdict-data` 不负责以下内容：

- 创建数据库连接
- 提供 HTTP handler 或路由
- 提供日志实现
- 提供面向消费端暴露的通用缓存层
- 执行数据库 migration

推荐的职责划分是：

- 应用入口负责创建 `*gorm.DB`
- 启动阶段调用 `postgresutil`
- `repository` 负责查询数据库
- `service` 负责业务编排与响应转换
- handler 或页面层自行定义所需接口并依赖 `*service.WordService`

## 测试

运行全部测试：

```bash
go test ./...
```

建议在提交前额外运行：

```bash
go test ./... -race
go vet ./...
```

仓储层包含真实 PostgreSQL 集成测试。未设置环境变量时，这部分测试会自动跳过。

必需环境变量：

- `TEST_POSTGRES_DSN`: 指向可销毁的 PostgreSQL 测试库；测试会执行 migration 并重置相关表数据

可选环境变量：

- `ISDICT_ALLOW_NONLOCAL_TEST_POSTGRES=1`: 允许对非本地 PostgreSQL 执行带破坏性的测试初始化。默认会拒绝远程地址，避免误伤。

示例：

```bash
TEST_POSTGRES_DSN='host=127.0.0.1 user=test password=test dbname=isdict_test sslmode=disable' go test ./repository
```

### 只读真实库 smoke 与性能测试

真实库只读测试不会使用 `TEST_POSTGRES_DSN`，避免触发集成测试里的 migration/reset 逻辑。它们通过 build tag 显式启用，并用 PostgreSQL read-only transaction 包裹查询。

只读 smoke 测试：

```bash
READONLY_SMOKE_DSN='host=127.0.0.1 port=5432 user=isdict password=... dbname=isdict_db sslmode=disable TimeZone=Asia/Shanghai' \
go test ./repository -tags readonlydb -run TestReadOnlyProductionDatabaseSmoke -count=1 -v
```

该 smoke 测试会校验 commons v1.1.2 的 21 张 active schema 表存在，确认旧 `lexical_relations` 表不存在，`entry_search_terms` 与 `entries` / `entry_forms` 计数一致、`featured_candidates` 等于 eligible distinct normalized headword 数量、没有重复 normalized headword、`quality_rank` 连续、school 内容与 OEWN 关系可读，并实际执行完整词条、entry group、变体、搜索、联想、短语、发音和义项读取。

只读性能基准：

```bash
READONLY_PERF_DSN='host=127.0.0.1 port=5432 user=isdict password=... dbname=isdict_db sslmode=disable TimeZone=Asia/Shanghai' \
go test ./repository -tags perfdb -run '^$' -bench BenchmarkReadOnly -benchtime=1x -count=1
```

性能基准覆盖 repository 与 service 的主要读取路径，包括精确词头、规范化词头、`entry_forms` 解析的最小/完整加载、变体反查、批量查询、featured 候选池、搜索/联想的基础路径、POS 与学习信号过滤、`entry_forms` 命中、offset、no-result、短语搜索、发音读取与 accent 过滤、义项读取与 POS 过滤，以及 featured cold-cache / warm-cache 查询。默认建议先用 `-benchtime=1x` 做全路径巡检，再对慢查询单独提高 benchtime 重复测量。

## 稳定约束

这个模块的首要目标是提供进程内词典查询能力，而不是重写查询内核。因此在后续维护中，以下内容应被视为行为敏感区域：

- `GetWordByHeadword` 的 headword / `entry_forms` 解析链路
- `SearchWords` / `SuggestWords` / `SearchPhrases` 对 `entry_search_terms` 的排序与去重
- `featured_candidates` 候选池读取语义
- `pg_trgm` 相关查询能力
- service 层对 not-found、featured 和批量限制错误的公开语义
- featured 词条对 canonical headword 的精确回填语义，以及其候选池缓存行为

如果要调整这些行为，应该以独立变更进行，并附带明确的回归测试。
