# isdict-data

`isdict-data` 是 isdict 词典系统的共享数据访问层模块，提供面向进程内调用的词典查询能力，供 `isdict-api` 和 `isdict-web` 复用。它从原有服务中提取并稳定化了数据访问、业务编排、查询参数校验和 PostgreSQL 扩展检查逻辑，避免 SSR 场景再经过一次 HTTP 跳转。

模块路径：`github.com/simp-lee/isdict-data`

## 设计目标

- 通过 Go 包直接暴露词典查询能力，不引入 HTTP、日志、缓存等运行时耦合。
- 保持原查询语义稳定，包括词头回退、变体匹配、搜索排序、去重和 pg_trgm 模糊匹配行为。
- 生产代码直接依赖收敛到 `github.com/simp-lee/isdict-commons` 与 `gorm.io/gorm`。
- 遵循 Go 的常见约定：接收接口，返回具体类型；接口由使用方按需定义。

## 包结构

### `repository`

负责词典数据读取与查询实现。

- 导出 `WordRepository` 接口，包含 9 个数据访问方法。
- 导出 `NewRepository(db *gorm.DB) *Repository`。
- 导出 `BatchVariantMatch`、`ErrWordNotFound`、`ErrVariantNotFound`。
- 保持现有查询行为，包括：
  - `GetWordByHeadword` 的精确匹配、规范化匹配、变体回退链路。
  - `SearchWords`、`SuggestWords`、`SearchPhrases` 的匹配优先级与稳定排序。
  - 基于 PostgreSQL `pg_trgm` 扩展的模糊搜索能力。

### `service`

负责业务编排、返回模型转换、批量查询与参数限流。

- 导出 `WordService` 结构体及 8 个公开方法。
- 导出 `NewWordService(repo repository.WordRepository, cfg ServiceConfig) *WordService`。
- 通过变量别名重导出仓储层 not-found 错误：
  - `service.ErrWordNotFound`
  - `service.ErrVariantNotFound`
- 额外导出批量限制错误：`service.ErrBatchLimitExceeded`。
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

	resp, err := wordService.GetWordByHeadword(context.Background(), "learn", nil, true, true, true)
	if err != nil {
		if errors.Is(err, service.ErrWordNotFound) {
			fmt.Println("word not found")
			return
		}
		panic(err)
	}

	fmt.Println(resp.Headword)
}
```

如果你的应用已经自己管理连接生命周期，不需要像示例那样在 `main` 中关闭 `sql.DB`。

## 常见调用方式

### 获取单个词条

```go
resp, err := svc.GetWordByHeadword(ctx, "programmed", nil, true, true, true)
```

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
import "github.com/simp-lee/isdict-commons/model"

items, meta, err := svc.GetWordsBatch(ctx, &model.BatchRequest{
	Words: []string{"learn", "learnt", "  learn  "},
})
```

批量查询会先清洗输入，再按主词头批量命中，未命中的部分自动回退到变体匹配。超出配置上限时返回 `service.ErrBatchLimitExceeded`。

### 搜索与联想

```go
results, meta, err := svc.SearchWords(ctx, "learn", nil, nil, nil, nil, nil, nil, 20, 0)
suggestions, err := svc.SuggestWords(ctx, "lea", nil, nil, nil, nil, nil, 10)
phrases, err := svc.SearchPhrases(ctx, "look", 10)
```

这些方法的参数约束分别是：

- `SearchWords`: 关键字最少 3 个规范化字符、最多 100 个裁剪后字符；`limit <= 0` 时默认 `20`，并受 `ServiceConfig.SearchMaxLimit` 限制；负 `offset` 会被重置为 `0`
- `SuggestWords`: 前缀最少 3 个规范化字符、最多 50 个裁剪后字符；`limit <= 0` 时默认 `10`，并受 `ServiceConfig.SuggestMaxLimit` 限制
- `SearchPhrases`: 关键字最少 1 个字符、最多 50 个字符；`limit <= 0` 时默认 `10`，最大固定为 `50`

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
- 提供缓存层
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

仓储层包含真实 PostgreSQL 集成测试。未设置环境变量时，这部分测试会自动跳过。

必需环境变量：

- `TEST_POSTGRES_DSN`: 指向可销毁的 PostgreSQL 测试库

可选环境变量：

- `ISDICT_ALLOW_NONLOCAL_TEST_POSTGRES=1`: 允许对非本地 PostgreSQL 执行带破坏性的测试初始化。默认会拒绝远程地址，避免误伤。

示例：

```bash
TEST_POSTGRES_DSN='host=127.0.0.1 user=test password=test dbname=isdict_test sslmode=disable' go test ./repository
```

## 稳定约束

这个模块的首要目标是复用现有词典查询能力，而不是重写查询内核。因此在后续维护中，以下内容应被视为兼容性敏感区域：

- `GetWordByHeadword` 的多步回退链路
- `SearchWords` / `SuggestWords` / `SearchPhrases` 的排序与去重
- `pg_trgm` 相关查询能力
- service 层对 not-found 错误和批量限制错误的公开语义

如果要调整这些行为，应该以独立变更进行，并附带明确的回归测试。
