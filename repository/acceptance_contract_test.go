package repository

import (
	"context"
	"fmt"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type repositoryContractStub struct{}

func (repositoryContractStub) GetWordByHeadword(context.Context, string, bool, bool, bool) (*Word, *WordVariant, error) {
	return nil, nil, nil
}

func (repositoryContractStub) GetWordsByHeadwords(context.Context, []string, bool, bool, bool) ([]Word, error) {
	return nil, nil
}

func (repositoryContractStub) GetWordsByVariants(context.Context, []string, bool, bool, bool) ([]BatchVariantMatch, error) {
	return nil, nil
}

func (repositoryContractStub) GetWordsByVariant(context.Context, string, *string, bool, bool) ([]Word, []WordVariant, error) {
	return nil, nil, nil
}

func (repositoryContractStub) ListFeaturedCandidateHeadwords(context.Context) ([]string, error) {
	return nil, nil
}

func (repositoryContractStub) SearchWords(context.Context, string, *string, *int, *int, *int, *int, *int, int, int) ([]Word, int64, error) {
	return nil, 0, nil
}

func (repositoryContractStub) SuggestWords(context.Context, string, *int, *int, *int, *int, *int, int) ([]Word, error) {
	return nil, nil
}

func (repositoryContractStub) SearchPhrases(context.Context, string, int) ([]Word, error) {
	return nil, nil
}

func (repositoryContractStub) GetPronunciationsByWordID(context.Context, int64, *string) ([]Pronunciation, error) {
	return nil, nil
}

func (repositoryContractStub) GetSensesByWordID(context.Context, int64, *string) ([]Sense, error) {
	return nil, nil
}

var _ WordRepository = (*repositoryContractStub)(nil)

// AC-1: 导出 WordRepository 接口，保持 10 个方法签名与当前契约一致
func TestRepository_WordRepositoryContractCompile(t *testing.T) {
	t.Helper()

	repoType := reflect.TypeFor[WordRepository]()
	expectedMethodNames := []string{
		"GetWordByHeadword",
		"GetWordsByHeadwords",
		"GetWordsByVariants",
		"GetWordsByVariant",
		"ListFeaturedCandidateHeadwords",
		"SearchWords",
		"SuggestWords",
		"SearchPhrases",
		"GetPronunciationsByWordID",
		"GetSensesByWordID",
	}

	if repoType.NumMethod() != len(expectedMethodNames) {
		t.Fatalf("WordRepository method count = %d, want %d", repoType.NumMethod(), len(expectedMethodNames))
	}

	for _, methodName := range expectedMethodNames {
		if _, ok := repoType.MethodByName(methodName); !ok {
			t.Fatalf("WordRepository missing method %q", methodName)
		}
	}
}

// AC-3: 导出 repository.BatchVariantMatch，字段为 Word 和 Variant
func TestBatchVariantMatch_ExposesWordAndVariantFields(t *testing.T) {
	match := BatchVariantMatch{
		Word: Word{
			Headword: "example",
		},
		Variant: WordVariant{
			FormText: "examples",
		},
	}

	if match.Word.Headword != "example" {
		t.Fatalf("Word.Headword = %q, want %q", match.Word.Headword, "example")
	}

	if match.Variant.FormText != "examples" {
		t.Fatalf("Variant.FormText = %q, want %q", match.Variant.FormText, "examples")
	}

	matchType := reflect.TypeOf(match)
	if matchType.NumField() != 2 {
		t.Fatalf("BatchVariantMatch field count = %d, want 2", matchType.NumField())
	}

	wordField, ok := matchType.FieldByName("Word")
	if !ok {
		t.Fatal("BatchVariantMatch missing Word field")
	}
	if !wordField.IsExported() || wordField.Type != reflect.TypeOf(Word{}) {
		t.Fatalf("Word field = exported:%v type:%v, want exported:true type:%v", wordField.IsExported(), wordField.Type, reflect.TypeOf(Word{}))
	}

	variantField, ok := matchType.FieldByName("Variant")
	if !ok {
		t.Fatal("BatchVariantMatch missing Variant field")
	}
	if !variantField.IsExported() || variantField.Type != reflect.TypeOf(WordVariant{}) {
		t.Fatalf("Variant field = exported:%v type:%v, want exported:true type:%v", variantField.IsExported(), variantField.Type, reflect.TypeOf(WordVariant{}))
	}
}

// AC-13: 运行时代码直接依赖仅收敛到 isdict-commons 与 gorm，不泄漏 HTTP/日志/缓存/驱动到生产 API
func TestProductionPackages_RestrictRuntimeDependencies(t *testing.T) {
	rootDir := repoRootDir(t)
	fileSet := token.NewFileSet()
	var violations []string

	err := filepath.WalkDir(rootDir, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		if entry.IsDir() {
			if entry.Name() == ".agents-work" || entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}

		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		parsedFile, err := parser.ParseFile(fileSet, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}

		for _, imported := range parsedFile.Imports {
			importPath := strings.Trim(imported.Path.Value, "\"")
			if isAllowedProductionImport(importPath) {
				continue
			}

			violations = append(violations, fmt.Sprintf("%s imports %s", filepath.ToSlash(relPath), importPath))
		}

		return nil
	})
	if err != nil {
		t.Fatalf("walk production packages: %v", err)
	}

	sort.Strings(violations)
	if len(violations) > 0 {
		t.Fatalf("unexpected production imports:\n%s", strings.Join(violations, "\n"))
	}
}

func repoRootDir(t *testing.T) string {
	t.Helper()

	workingDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	return filepath.Dir(workingDir)
}

func isAllowedProductionImport(importPath string) bool {
	if isStandardLibraryImport(importPath) {
		return true
	}

	allowedPrefixes := []string{
		"github.com/lib/pq",
		"github.com/simp-lee/isdict-data",
		"github.com/simp-lee/isdict-commons",
		"gorm.io/gorm",
	}

	for _, prefix := range allowedPrefixes {
		if importPath == prefix || strings.HasPrefix(importPath, prefix+"/") {
			return true
		}
	}

	return false
}

func isStandardLibraryImport(importPath string) bool {
	firstSegment := importPath
	if slash := strings.IndexByte(importPath, '/'); slash >= 0 {
		firstSegment = importPath[:slash]
	}

	return !strings.Contains(firstSegment, ".")
}
