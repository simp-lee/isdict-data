package service

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-data/repository"
)

// mockRepository is a mock implementation of the WordRepository interface
type mockRepository struct {
	getWordByHeadwordFunc              func(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error)
	getWordsByHeadwordsFunc            func(ctx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error)
	getWordsByVariantsFunc             func(ctx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error)
	getWordsByVariantFunc              func(ctx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error)
	listFeaturedCandidateHeadwordsFunc func(ctx context.Context) ([]string, error)
	searchWordsFunc                    func(ctx context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error)
	suggestWordsFunc                   func(ctx context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error)
	searchPhrasesFunc                  func(ctx context.Context, keyword string, limit int) ([]repository.Word, error)
	getPronunciationsByWordIDFunc      func(ctx context.Context, wordID int64, accent *string) ([]repository.Pronunciation, error)
	getSensesByWordIDFunc              func(ctx context.Context, wordID int64, pos *string) ([]repository.Sense, error)
}

func (m *mockRepository) GetWordByHeadword(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
	if m.getWordByHeadwordFunc != nil {
		return m.getWordByHeadwordFunc(ctx, headword, includeVariants, includePronunciations, includeSenses)
	}
	return nil, nil, repository.ErrWordNotFound
}

func (m *mockRepository) GetWordsByHeadwords(ctx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
	if m.getWordsByHeadwordsFunc != nil {
		return m.getWordsByHeadwordsFunc(ctx, headwords, includeVariants, includePronunciations, includeSenses)
	}
	return []repository.Word{}, nil
}

func (m *mockRepository) GetWordsByVariants(ctx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
	if m.getWordsByVariantsFunc != nil {
		return m.getWordsByVariantsFunc(ctx, variants, includeVariants, includePronunciations, includeSenses)
	}
	return []repository.BatchVariantMatch{}, nil
}

func (m *mockRepository) GetWordsByVariant(ctx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
	if m.getWordsByVariantFunc != nil {
		return m.getWordsByVariantFunc(ctx, variant, kind, includePronunciations, includeSenses)
	}
	return nil, nil, repository.ErrVariantNotFound
}

func (m *mockRepository) ListFeaturedCandidateHeadwords(ctx context.Context) ([]string, error) {
	if m.listFeaturedCandidateHeadwordsFunc != nil {
		return m.listFeaturedCandidateHeadwordsFunc(ctx)
	}
	return []string{}, nil
}

func (m *mockRepository) SearchWords(ctx context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
	if m.searchWordsFunc != nil {
		return m.searchWordsFunc(ctx, keyword, pos, cefrLevel, oxfordLevel, cetLevel, maxFrequencyRank, minCollinsStars, limit, offset)
	}
	return []repository.Word{}, 0, nil
}

func (m *mockRepository) SuggestWords(ctx context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error) {
	if m.suggestWordsFunc != nil {
		return m.suggestWordsFunc(ctx, prefix, cefrLevel, oxfordLevel, cetLevel, maxFrequencyRank, minCollinsStars, limit)
	}
	return []repository.Word{}, nil
}

func (m *mockRepository) SearchPhrases(ctx context.Context, keyword string, limit int) ([]repository.Word, error) {
	if m.searchPhrasesFunc != nil {
		return m.searchPhrasesFunc(ctx, keyword, limit)
	}
	return []repository.Word{}, nil
}

func (m *mockRepository) GetPronunciationsByWordID(ctx context.Context, wordID int64, accent *string) ([]repository.Pronunciation, error) {
	if m.getPronunciationsByWordIDFunc != nil {
		return m.getPronunciationsByWordIDFunc(ctx, wordID, accent)
	}
	return []repository.Pronunciation{}, nil
}

func (m *mockRepository) GetSensesByWordID(ctx context.Context, wordID int64, pos *string) ([]repository.Sense, error) {
	if m.getSensesByWordIDFunc != nil {
		return m.getSensesByWordIDFunc(ctx, wordID, pos)
	}
	return []repository.Sense{}, nil
}

func createTestConfig() ServiceConfig {
	return ServiceConfig{
		BatchMaxSize:    100,
		SearchMaxLimit:  100,
		SuggestMaxLimit: 50,
	}
}

func wordWithSummary(headword, summary string) repository.Word {
	return repository.Word{
		Headword:    headword,
		SummariesZH: summariesZH(summary),
	}
}

func summariesZH(summary string) []model.EntrySummaryZH {
	if summary == "" {
		return nil
	}
	return []model.EntrySummaryZH{{SummaryText: summary}}
}

func entryLearningSignal(cefrLevel int, cefrSource string) *model.EntryLearningSignal {
	return &model.EntryLearningSignal{
		CEFRLevel:  int16(cefrLevel),
		CEFRSource: cefrSource,
	}
}

func senseLearningSignal(cefrLevel int, cefrSource string) *model.SenseLearningSignal {
	return &model.SenseLearningSignal{
		CEFRLevel:  int16(cefrLevel),
		CEFRSource: cefrSource,
	}
}

func TestNewWordService_NormalizesConfigDefaults(t *testing.T) {
	tests := []struct {
		name     string
		cfg      ServiceConfig
		expected ServiceConfig
	}{
		{
			name:     "zero value config uses defaults",
			cfg:      ServiceConfig{},
			expected: createTestConfig(),
		},
		{
			name: "partial config preserves explicit values and fills missing ones",
			cfg: ServiceConfig{
				BatchMaxSize:    25,
				SearchMaxLimit:  0,
				SuggestMaxLimit: -1,
			},
			expected: ServiceConfig{
				BatchMaxSize:    25,
				SearchMaxLimit:  defaultSearchMaxLimit,
				SuggestMaxLimit: defaultSuggestMaxLimit,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			service := NewWordService(nil, tt.cfg)
			if service.config != tt.expected {
				t.Fatalf("normalized config = %+v, want %+v", service.config, tt.expected)
			}
		})
	}
}

type testContextKey string

func TestWordService_ForwardsContextToRepository(t *testing.T) {
	ctx := context.WithValue(context.Background(), testContextKey("request-id"), "req-123")
	cfg := createTestConfig()

	tests := []struct {
		name string
		run  func(*testing.T, context.Context, ServiceConfig)
	}{
		{name: "GetWordByHeadword", run: runContextForwardingGetWordByHeadword},
		{name: "GetWordsByVariant", run: runContextForwardingGetWordsByVariant},
		{name: "GetWordsBatch", run: runContextForwardingGetWordsBatch},
		{name: "SearchWords", run: runContextForwardingSearchWords},
		{name: "SuggestWords", run: runContextForwardingSuggestWords},
		{name: "SearchPhrases", run: runContextForwardingSearchPhrases},
		{name: "GetPronunciations", run: runContextForwardingGetPronunciations},
		{name: "GetSenses", run: runContextForwardingGetSenses},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t, ctx, cfg)
		})
	}
}

func runContextForwardingGetWordByHeadword(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordByHeadwordFunc: func(callCtx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			assertForwardedContext(t, ctx, callCtx)
			return &repository.Word{Headword: headword}, nil, nil
		},
	}, cfg)
	_, err := service.GetWordByHeadword(ctx, "learn", nil, false, false, false)
	assertNoServiceError(t, err)
}

func runContextForwardingGetWordsByVariant(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordsByVariantFunc: func(callCtx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			assertForwardedContext(t, ctx, callCtx)
			if includePronunciations || includeSenses {
				t.Fatalf("expected include flags to stay false, got pronunciations=%v senses=%v", includePronunciations, includeSenses)
			}
			return []repository.Word{{ID: 1, Headword: "learn"}}, []repository.WordVariant{{WordID: 1, FormText: variant}}, nil
		},
	}, cfg)
	_, err := service.GetWordsByVariant(ctx, "learnt", nil, false, false)
	assertNoServiceError(t, err)
}

func runContextForwardingGetWordsBatch(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	batchVariantCalled := false
	service := NewWordService(&mockRepository{
		getWordsByHeadwordsFunc: func(callCtx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			assertForwardedContext(t, ctx, callCtx)
			if len(headwords) != 2 {
				t.Fatalf("expected 2 headwords, got %d", len(headwords))
			}
			return []repository.Word{{ID: 1, Headword: headwords[0]}}, nil
		},
		getWordsByVariantsFunc: func(callCtx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
			assertForwardedContext(t, ctx, callCtx)
			batchVariantCalled = true
			if !reflect.DeepEqual(variants, []string{"learnt"}) {
				t.Fatalf("expected batch entry_forms lookup for [learnt], got %v", variants)
			}
			return []repository.BatchVariantMatch{{
				Word:    repository.Word{ID: 2, Headword: "learn"},
				Variant: repository.WordVariant{WordID: 2, FormText: "learnt"},
			}}, nil
		},
	}, cfg)
	responses, meta, err := service.GetWordsBatch(ctx, &BatchRequest{Words: []string{"learn", "learnt"}})
	assertNoServiceError(t, err)
	if !batchVariantCalled {
		t.Fatal("expected batch entry_forms repository call")
	}
	if len(responses) != 2 || meta == nil || *meta.Found != 2 {
		t.Fatalf("unexpected batch result: responses=%d meta=%+v", len(responses), meta)
	}
}

func runContextForwardingSearchWords(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		searchWordsFunc: func(callCtx context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
			assertForwardedContext(t, ctx, callCtx)
			return nil, 0, nil
		},
	}, cfg)
	_, _, err := service.SearchWords(ctx, "learn", nil, nil, nil, nil, nil, nil, 20, 0)
	assertNoServiceError(t, err)
}

func runContextForwardingSuggestWords(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		suggestWordsFunc: func(callCtx context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error) {
			assertForwardedContext(t, ctx, callCtx)
			return nil, nil
		},
	}, cfg)
	_, err := service.SuggestWords(ctx, "lea", nil, nil, nil, nil, nil, 10)
	assertNoServiceError(t, err)
}

func runContextForwardingSearchPhrases(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		searchPhrasesFunc: func(callCtx context.Context, keyword string, limit int) ([]repository.Word, error) {
			assertForwardedContext(t, ctx, callCtx)
			return nil, nil
		},
	}, cfg)
	_, err := service.SearchPhrases(ctx, "look", 10)
	assertNoServiceError(t, err)
}

func runContextForwardingGetPronunciations(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordByHeadwordFunc: func(callCtx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			assertForwardedContext(t, ctx, callCtx)
			return &repository.Word{ID: 1, Headword: headword}, nil, nil
		},
		getPronunciationsByWordIDFunc: func(callCtx context.Context, wordID int64, accent *string) ([]repository.Pronunciation, error) {
			assertForwardedContext(t, ctx, callCtx)
			return nil, nil
		},
	}, cfg)
	_, err := service.GetPronunciations(ctx, "learn", nil)
	assertNoServiceError(t, err)
}

func runContextForwardingGetSenses(t *testing.T, ctx context.Context, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordByHeadwordFunc: func(callCtx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			assertForwardedContext(t, ctx, callCtx)
			return &repository.Word{ID: 1, Headword: headword}, nil, nil
		},
		getSensesByWordIDFunc: func(callCtx context.Context, wordID int64, pos *string) ([]repository.Sense, error) {
			assertForwardedContext(t, ctx, callCtx)
			return nil, nil
		},
	}, cfg)
	_, err := service.GetSenses(ctx, "learn", nil, langBoth)
	assertNoServiceError(t, err)
}

func assertForwardedContext(t *testing.T, want, got context.Context) {
	t.Helper()
	if got != want {
		t.Fatalf("expected forwarded context")
	}
}

func assertNoServiceError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestGetWordsBatch_LimitExceeded(t *testing.T) {
	cfg := createTestConfig()
	mockRepo := &mockRepository{}
	service := NewWordService(mockRepo, cfg)

	// Create request with more than max size
	words := make([]string, 101)
	for i := range words {
		words[i] = fmt.Sprintf("test-%d", i)
	}

	req := &BatchRequest{
		Words: words,
	}

	_, _, err := service.GetWordsBatch(context.Background(), req)
	if err == nil {
		t.Fatal("Expected error for batch limit exceeded, got nil")
	}

	if !errors.Is(err, ErrBatchLimitExceeded) {
		t.Errorf("Expected ErrBatchLimitExceeded, got %v", err)
	}
}

func TestGetWordsBatch_RejectsRawOversizedInputBeforeCleanup(t *testing.T) {
	cfg := createTestConfig()
	cfg.BatchMaxSize = 2

	repoCalled := false
	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			repoCalled = true
			return nil, nil
		},
	}

	service := NewWordService(mockRepo, cfg)
	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{" apple ", "", "apple", "pear", "  ", "pear"},
	})
	if err == nil {
		t.Fatal("Expected error for raw oversized batch, got nil")
	}
	if !errors.Is(err, ErrBatchLimitExceeded) {
		t.Fatalf("Expected ErrBatchLimitExceeded, got %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("Expected no responses on error, got %d", len(responses))
	}
	if meta != nil {
		t.Fatalf("Expected nil meta on error, got %+v", meta)
	}
	if repoCalled {
		t.Fatal("Expected repository not to be called when raw batch exceeds limit")
	}
}

func TestGetWordsBatch_PreservesSeparatorDistinctInputs(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			expected := []string{"cooperate", "co-operate"}
			if !reflect.DeepEqual(headwords, expected) {
				t.Fatalf("expected cleaned headwords %v, got %v", expected, headwords)
			}
			return []repository.Word{{ID: 1, Headword: "cooperate"}, {ID: 2, Headword: "co-operate"}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)
	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"cooperate", "co-operate", " cooperate "},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}
	if responses[0].Headword != "cooperate" {
		t.Fatalf("Expected cooperate response, got %q", responses[0].Headword)
	}
	if responses[1].Headword != "co-operate" {
		t.Fatalf("Expected co-operate response, got %q", responses[1].Headword)
	}
	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 2 {
		t.Fatalf("Expected requested=2 after exact-only dedupe, got %d", *meta.Requested)
	}
	if *meta.Found != 2 {
		t.Fatalf("Expected found=2, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 0 {
		t.Fatalf("Expected no not_found entries, got %v", meta.NotFound)
	}
}

func TestGetWordsBatch_EmptyRequest(t *testing.T) {
	cfg := createTestConfig()
	mockRepo := &mockRepository{}
	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("Expected empty response slice, got %d items", len(responses))
	}
	if meta != nil {
		t.Fatalf("Expected nil meta for empty request, got %+v", meta)
	}
}

func TestGetWordsBatch_NilRequest(t *testing.T) {
	cfg := createTestConfig()
	mockRepo := &mockRepository{}
	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), nil)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(responses) != 0 {
		t.Fatalf("Expected empty response slice, got %d items", len(responses))
	}
	if meta != nil {
		t.Fatalf("Expected nil meta for nil request, got %+v", meta)
	}
}

func TestGetWordsBatch_PreservesOrder(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{
				{ID: 3, Headword: "cat"},
				{ID: 1, Headword: "apple"},
				{ID: 2, Headword: "book"},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"apple", "book", "cat"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(responses) != 3 {
		t.Fatalf("Expected 3 responses, got %d", len(responses))
	}

	expectedOrder := []string{"apple", "book", "cat"}
	for i, resp := range responses {
		if resp.Headword != expectedOrder[i] {
			t.Errorf("Expected word at position %d to be %s, got %s", i, expectedOrder[i], resp.Headword)
		}
	}
	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 3 {
		t.Errorf("Expected requested=3, got %d", *meta.Requested)
	}
	if *meta.Found != 3 {
		t.Errorf("Expected found=3, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 0 {
		t.Errorf("Expected no not found words, got %v", meta.NotFound)
	}
}

func TestGetWordsBatch_CaseSensitiveVariants(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{
				{ID: 1, Headword: "Polish"},
				{ID: 2, Headword: "polish"},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"Polish", "polish"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}
	if responses[0].Headword != "Polish" {
		t.Fatalf("Expected first response Polish, got %q", responses[0].Headword)
	}
	if responses[1].Headword != "polish" {
		t.Fatalf("Expected second response polish, got %q", responses[1].Headword)
	}
	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 2 {
		t.Errorf("Expected requested=2, got %d", *meta.Requested)
	}
	if *meta.Found != 2 {
		t.Errorf("Expected found=2, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 0 {
		t.Errorf("Expected no not found words, got %v", meta.NotFound)
	}
}

func TestGetWordsBatch_PartialResults(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{{ID: 1, Headword: "apple"}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"apple", "xyz123", "book"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(responses) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(responses))
	}
	if responses[0].Headword != "apple" {
		t.Fatalf("Expected apple response, got %q", responses[0].Headword)
	}
	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 3 {
		t.Errorf("Expected requested=3, got %d", *meta.Requested)
	}
	if *meta.Found != 1 {
		t.Errorf("Expected found=1, got %d", *meta.Found)
	}
	if !reflect.DeepEqual(meta.NotFound, []string{"xyz123", "book"}) {
		t.Errorf("Expected not found [xyz123 book], got %v", meta.NotFound)
	}
}

func TestSearchWords_LimitValidation(t *testing.T) {
	cfg := createTestConfig()

	callCount := 0
	expectedLimits := []int{100, 20, 20} // max for >max, default for <=0, default for negative

	mockRepo := &mockRepository{
		searchWordsFunc: func(_ context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
			if callCount < len(expectedLimits) {
				if limit != expectedLimits[callCount] {
					t.Errorf("Call %d: Expected limit to be %d, got %d", callCount, expectedLimits[callCount], limit)
				}
			}
			callCount++
			return []repository.Word{}, 0, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	// Test with limit > max (should use max)
	_, _, err := service.SearchWords(context.Background(), "test", nil, nil, nil, nil, nil, nil, 101, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Test with limit <= 0 (should use default)
	_, _, err = service.SearchWords(context.Background(), "test", nil, nil, nil, nil, nil, nil, 0, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Test with negative limit (should use default)
	_, _, err = service.SearchWords(context.Background(), "test", nil, nil, nil, nil, nil, nil, -1, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestSearchWords_OffsetValidation(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		searchWordsFunc: func(_ context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
			if offset != 0 {
				t.Errorf("Expected offset to be reset to 0, got %d", offset)
			}
			return []repository.Word{}, 0, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	_, _, err := service.SearchWords(context.Background(), "test", nil, nil, nil, nil, nil, nil, 20, -10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestSuggestWords_LimitValidation(t *testing.T) {
	cfg := createTestConfig()

	callCount := 0
	expectedLimits := []int{50, 10, 10}

	mockRepo := &mockRepository{
		suggestWordsFunc: func(_ context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error) {
			if callCount < len(expectedLimits) && limit != expectedLimits[callCount] {
				t.Errorf("Call %d: Expected limit to be %d, got %d", callCount, expectedLimits[callCount], limit)
			}
			callCount++
			return []repository.Word{}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	_, err := service.SuggestWords(context.Background(), "test", nil, nil, nil, nil, nil, 51)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = service.SuggestWords(context.Background(), "test", nil, nil, nil, nil, nil, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = service.SuggestWords(context.Background(), "test", nil, nil, nil, nil, nil, -1)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestGetWordsByVariant_ValidKind(t *testing.T) {
	cfg := createTestConfig()

	formKind := model.RelationKindForm
	mockRepo := &mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			if kind == nil {
				t.Error("Expected kind to be set, got nil")
			} else if *kind != formKind {
				t.Errorf("Expected kind=%q, got %q", formKind, *kind)
			}
			if !includePronunciations || !includeSenses {
				t.Fatalf("expected include flags to be forwarded, got pronunciations=%v senses=%v", includePronunciations, includeSenses)
			}
			return []repository.Word{{ID: 1, Headword: "test"}}, []repository.WordVariant{{WordID: 1, FormText: "testing"}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	kindStr := "form"
	results, err := service.GetWordsByVariant(context.Background(), "testing", &kindStr, true, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(results))
	}
}

func TestGetWordByHeadword_NotFound(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			return nil, nil, repository.ErrWordNotFound
		},
	}

	service := NewWordService(mockRepo, cfg)

	_, err := service.GetWordByHeadword(context.Background(), "nonexistent", nil, true, true, true)
	if err == nil {
		t.Fatal("Expected error for word not found, got nil")
	}
	if !errors.Is(err, ErrWordNotFound) {
		t.Errorf("Expected ErrWordNotFound, got %v", err)
	}
}

func TestGetWordByHeadword_NotFoundWrappedError(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			return nil, nil, fmt.Errorf("cache miss: %w", repository.ErrWordNotFound)
		},
	}

	service := NewWordService(mockRepo, cfg)

	_, err := service.GetWordByHeadword(context.Background(), "nonexistent", nil, true, true, true)
	if err == nil {
		t.Fatal("Expected error for word not found, got nil")
	}
	if !errors.Is(err, ErrWordNotFound) {
		t.Errorf("Expected ErrWordNotFound from wrapped repository error, got %v", err)
	}
}

func TestGetWordsByVariant_NotFoundWrappedError(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			return nil, nil, fmt.Errorf("decorator miss: %w", repository.ErrVariantNotFound)
		},
	}

	service := NewWordService(mockRepo, cfg)

	_, err := service.GetWordsByVariant(context.Background(), "nonexistent", nil, false, false)
	if err == nil {
		t.Fatal("Expected error for variant not found, got nil")
	}
	if !errors.Is(err, ErrVariantNotFound) {
		t.Errorf("Expected ErrVariantNotFound from wrapped repository error, got %v", err)
	}
}

func TestGetWordsByVariant_MultipleForms(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			if variant != "lit" {
				t.Fatalf("unexpected variant: %s", variant)
			}

			words := []repository.Word{{
				ID:       1,
				Headword: "light",
				LearningSignal: &model.EntryLearningSignal{
					CEFRLevel:     1,
					FrequencyRank: 150,
				},
			}}

			variants := []repository.WordVariant{
				{
					WordID:          1,
					FormText:        "lit",
					RelationKind:    model.RelationKindForm,
					FormType:        stringPtr("past"),
					SourceRelations: pq.StringArray{"past"},
				},
				{
					WordID:          1,
					FormText:        "lit",
					RelationKind:    model.RelationKindForm,
					FormType:        stringPtr("past_participle"),
					SourceRelations: pq.StringArray{"past_participle"},
				},
			}

			return words, variants, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, err := service.GetWordsByVariant(context.Background(), "lit", nil, true, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	info := results[0].VariantInfo
	if len(info) != 2 {
		t.Fatalf("Expected 2 variant entries, got %d", len(info))
	}
	if info[0].FormType != "past" || len(info[0].SourceRelations) != 1 || info[0].SourceRelations[0] != "past" {
		t.Errorf("Unexpected first variant: %+v", info[0])
	}
	if info[1].FormType != "past_participle" || len(info[1].SourceRelations) != 1 || info[1].SourceRelations[0] != "past_participle" {
		t.Errorf("Unexpected second variant: %+v", info[1])
	}
}

func TestGetWordsByVariant_SourceRelationsRemainNormalized(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			words := []repository.Word{{
				ID:       10,
				Headword: "color",
				LearningSignal: &model.EntryLearningSignal{
					CEFRLevel:     2,
					FrequencyRank: 450,
				},
			}}

			variants := []repository.WordVariant{{
				WordID:          10,
				FormText:        "colour",
				RelationKind:    model.RelationKindAlias,
				SourceRelations: pq.StringArray{"british", "alternative_spelling"},
			}}

			return words, variants, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, err := service.GetWordsByVariant(context.Background(), "colour", nil, true, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	sourceRelations := results[0].VariantInfo[0].SourceRelations
	if len(sourceRelations) != 2 {
		t.Fatalf("Expected 2 source relations, got %d", len(sourceRelations))
	}
	if sourceRelations[0] != "british" {
		t.Errorf("Expected first source relation 'british', got %q", sourceRelations[0])
	}
	if sourceRelations[1] != "alternative_spelling" {
		t.Errorf("Expected second source relation 'alternative_spelling', got %q", sourceRelations[1])
	}
}

func TestGetWordsByVariant_MapsCEFRLevel(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			return []repository.Word{{
					ID:             1,
					Headword:       "light",
					LearningSignal: entryLearningSignal(1, "oxford"),
				}}, []repository.WordVariant{{
					WordID:       1,
					FormText:     "lit",
					RelationKind: model.RelationKindForm,
				}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, err := service.GetWordsByVariant(context.Background(), "lit", nil, false, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].CEFRLevel != 1 || results[0].CEFRLevelName != "A1" {
		t.Fatalf("Expected CEFR level 1/A1, got %d/%q", results[0].CEFRLevel, results[0].CEFRLevelName)
	}
	if results[0].CEFRSource != "oxford" {
		t.Fatalf("Expected CEFR source oxford, got %q", results[0].CEFRSource)
	}
}

func TestSearchWords_MapsCEFRLevelAndSource(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		searchWordsFunc: func(_ context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
			return []repository.Word{{
				ID:             2,
				Headword:       "test",
				Pos:            model.POSNoun,
				LearningSignal: entryLearningSignal(4, "both"),
			}}, 1, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, _, err := service.SearchWords(context.Background(), "test", nil, nil, nil, nil, nil, nil, 20, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].CEFRLevel != 4 || results[0].CEFRLevelName != "B2" {
		t.Fatalf("Expected CEFR level 4/B2, got %d/%q", results[0].CEFRLevel, results[0].CEFRLevelName)
	}

	if results[0].CEFRSource != "both" {
		t.Fatalf("Expected CEFR source both, got %q", results[0].CEFRSource)
	}
}

func TestSuggestWords_MapsCEFRLevel(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		suggestWordsFunc: func(_ context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error) {
			return []repository.Word{{
				Headword:       "learn",
				LearningSignal: entryLearningSignal(5, "cefrj"),
			}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, err := service.SuggestWords(context.Background(), "lea", nil, nil, nil, nil, nil, 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].CEFRLevel != 5 || results[0].CEFRLevelName != "C1" {
		t.Fatalf("Expected CEFR level 5/C1, got %d/%q", results[0].CEFRLevel, results[0].CEFRLevelName)
	}
}

func TestSearchPhrases_MapsCEFRLevel(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		searchPhrasesFunc: func(_ context.Context, keyword string, limit int) ([]repository.Word, error) {
			return []repository.Word{{
				Headword:       "look up",
				LearningSignal: entryLearningSignal(6, "oxford"),
			}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	results, err := service.SearchPhrases(context.Background(), "look", 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}

	if results[0].CEFRLevel != 6 || results[0].CEFRLevelName != "C2" {
		t.Fatalf("Expected CEFR level 6/C2, got %d/%q", results[0].CEFRLevel, results[0].CEFRLevelName)
	}
}

func TestGetWordByHeadword_MapsWordAndSenseCEFRLevels(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			return &repository.Word{
				ID:             3,
				Headword:       "learn",
				Pos:            model.POSNoun,
				LearningSignal: entryLearningSignal(2, "both"),
				Senses: []repository.Sense{{
					ID:             7,
					LearningSignal: senseLearningSignal(3, "cefrj"),
					SenseOrder:     1,
				}},
			}, nil, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	result, err := service.GetWordByHeadword(context.Background(), "learn", nil, false, false, true)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if result.CEFRLevel != 2 || result.CEFRLevelName != "A2" {
		t.Fatalf("Expected word CEFR level 2/A2, got %d/%q", result.CEFRLevel, result.CEFRLevelName)
	}

	if len(result.Senses) != 1 {
		t.Fatalf("Expected 1 sense, got %d", len(result.Senses))
	}

	if result.Senses[0].CEFRLevel != 3 || result.Senses[0].CEFRLevelName != "B1" {
		t.Fatalf("Expected sense CEFR level 3/B1, got %d/%q", result.Senses[0].CEFRLevel, result.Senses[0].CEFRLevelName)
	}

	if result.Senses[0].CEFRSource != "cefrj" {
		t.Fatalf("Expected sense CEFR source cefrj, got %q", result.Senses[0].CEFRSource)
	}
}

func TestSchoolLevel_PassthroughAcrossResponses(t *testing.T) {
	cfg := createTestConfig()

	tests := []struct {
		name string
		run  func(*testing.T, ServiceConfig)
	}{
		{name: "GetWordByHeadword", run: runSchoolLevelGetWordByHeadword},
		{name: "GetWordsByVariant", run: runSchoolLevelGetWordsByVariant},
		{name: "SearchWords", run: runSchoolLevelSearchWords},
		{name: "SuggestWords", run: runSchoolLevelSuggestWords},
		{name: "SearchPhrases", run: runSchoolLevelSearchPhrases},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.run(t, cfg)
		})
	}
}

func runSchoolLevelGetWordByHeadword(t *testing.T, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			return &repository.Word{
				ID:       1,
				Headword: headword,
				LearningSignal: &model.EntryLearningSignal{
					SchoolLevel: 3,
				},
			}, nil, nil
		},
	}, cfg)

	result, err := service.GetWordByHeadword(context.Background(), "learn", nil, false, false, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result.SchoolLevel != 3 {
		t.Fatalf("Expected SchoolLevel 3, got %d", result.SchoolLevel)
	}
}

func runSchoolLevelGetWordsByVariant(t *testing.T, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		getWordsByVariantFunc: func(_ context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]repository.Word, []repository.WordVariant, error) {
			return []repository.Word{{
					ID:       1,
					Headword: "learn",
					LearningSignal: &model.EntryLearningSignal{
						SchoolLevel: 2,
					},
				}}, []repository.WordVariant{{
					WordID:   1,
					FormText: variant,
				}}, nil
		},
	}, cfg)

	results, err := service.GetWordsByVariant(context.Background(), "learnt", nil, false, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].SchoolLevel != 2 {
		t.Fatalf("Expected SchoolLevel 2, got %d", results[0].SchoolLevel)
	}
}

func runSchoolLevelSearchWords(t *testing.T, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		searchWordsFunc: func(_ context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]repository.Word, int64, error) {
			return []repository.Word{{
				ID:       1,
				Headword: keyword,
				Pos:      model.POSNoun,
				LearningSignal: &model.EntryLearningSignal{
					SchoolLevel: 1,
				},
			}}, 1, nil
		},
	}, cfg)

	results, _, err := service.SearchWords(context.Background(), "learn", nil, nil, nil, nil, nil, nil, 20, 0)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].SchoolLevel != 1 {
		t.Fatalf("Expected SchoolLevel 1, got %d", results[0].SchoolLevel)
	}
}

func runSchoolLevelSuggestWords(t *testing.T, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		suggestWordsFunc: func(_ context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]repository.Word, error) {
			return []repository.Word{{
				Headword: prefix,
				LearningSignal: &model.EntryLearningSignal{
					SchoolLevel: 2,
				},
			}}, nil
		},
	}, cfg)

	results, err := service.SuggestWords(context.Background(), "lea", nil, nil, nil, nil, nil, 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].SchoolLevel != 2 {
		t.Fatalf("Expected SchoolLevel 2, got %d", results[0].SchoolLevel)
	}
}

func runSchoolLevelSearchPhrases(t *testing.T, cfg ServiceConfig) {
	service := NewWordService(&mockRepository{
		searchPhrasesFunc: func(_ context.Context, keyword string, limit int) ([]repository.Word, error) {
			return []repository.Word{{
				Headword: keyword + " up",
				LearningSignal: &model.EntryLearningSignal{
					SchoolLevel: 3,
				},
			}}, nil
		},
	}, cfg)

	results, err := service.SearchPhrases(context.Background(), "look", 10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	if results[0].SchoolLevel != 3 {
		t.Fatalf("Expected SchoolLevel 3, got %d", results[0].SchoolLevel)
	}
}

func stringPtr(v string) *string {
	return &v
}

func int64Ptr(v int64) *int64 {
	return &v
}

func TestGetWordByHeadword_ExposesCommonsV1Data(t *testing.T) {
	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	cleanEtymology := "from Old English leornen"
	romanization := "xue2 xi2"
	runID := int64(99)

	word := &repository.Word{
		ID:          10,
		Headword:    "learn",
		Pos:         model.POSVerb,
		SourceRunID: 1,
		SourceRun: &model.ImportRun{
			ID:              1,
			SourceName:      "wiktionary",
			SourcePath:      "/data/enwiktionary.xml",
			PipelineVersion: "v1",
			Status:          model.ImportRunStatusCompleted,
			StartedAt:       now,
		},
		LearningSignal: &model.EntryLearningSignal{
			EntryID:        10,
			CEFRLevel:      2,
			CEFRSource:     model.CEFRSourceOxford,
			CEFRRunID:      int64Ptr(91),
			OxfordLevel:    1,
			OxfordRunID:    int64Ptr(92),
			CETLevel:       1,
			CETRunID:       int64Ptr(93),
			SchoolLevel:    2,
			FrequencyRank:  123,
			FrequencyCount: 456,
			FrequencyRunID: int64Ptr(94),
			CollinsStars:   3,
			CollinsRunID:   int64Ptr(95),
			UpdatedAt:      now,
		},
		CEFRSourceSignals: []model.EntryCEFRSourceSignal{{
			EntryID:    10,
			CEFRSource: model.CEFRSourceCEFRJ,
			CEFRLevel:  3,
			CEFRRunID:  &runID,
			UpdatedAt:  now,
		}},
		SummariesZH: []model.EntrySummaryZH{{
			ID:          11,
			EntryID:     10,
			Source:      "manual",
			SourceRunID: 2,
			SummaryText: "学习",
			UpdatedAt:   now,
		}},
		Etymology: &model.EntryEtymology{
			EntryID:            10,
			Source:             "wiktionary",
			SourceRunID:        3,
			EtymologyTextRaw:   "raw etymology",
			EtymologyTextClean: &cleanEtymology,
			UpdatedAt:          now,
		},
		Pronunciations: []repository.Pronunciation{{
			ID:           20,
			WordID:       10,
			Accent:       model.AccentBritish,
			IPA:          "lɜːn",
			IsPrimary:    true,
			DisplayOrder: 2,
		}},
		PronunciationAudios: []repository.PronunciationAudio{{
			ID:            21,
			WordID:        10,
			Accent:        model.AccentBritish,
			AudioFilename: "LL-Q1860 (eng)-Vealhurl-learn.wav",
			IsPrimary:     true,
			DisplayOrder:  1,
		}},
		Senses: []repository.Sense{{
			ID:         30,
			WordID:     10,
			SenseOrder: 1,
			LearningSignal: &model.SenseLearningSignal{
				SenseID:     30,
				CEFRLevel:   2,
				CEFRSource:  model.CEFRSourceOxford,
				CEFRRunID:   int64Ptr(96),
				OxfordLevel: 1,
				OxfordRunID: int64Ptr(97),
				UpdatedAt:   now,
			},
			CEFRSourceSignals: []model.SenseCEFRSourceSignal{{
				SenseID:    30,
				CEFRSource: model.CEFRSourceOctanove,
				CEFRLevel:  4,
				CEFRRunID:  int64Ptr(98),
				UpdatedAt:  now,
			}},
			GlossesEN: []model.SenseGlossEN{{
				ID:         31,
				SenseID:    30,
				GlossOrder: 1,
				TextEN:     "to gain knowledge",
			}},
			GlossesZH: []model.SenseGlossZH{{
				ID:           32,
				SenseID:      30,
				Source:       "manual",
				SourceRunID:  4,
				GlossOrder:   1,
				TextZHHans:   "学习知识",
				Romanization: &romanization,
				IsPrimary:    true,
			}},
			Labels: []model.SenseLabel{{
				ID:         33,
				SenseID:    30,
				LabelType:  model.LabelTypeRegister,
				LabelCode:  model.RegisterLabelFormal,
				LabelOrder: 1,
			}},
			Examples: []repository.Example{{
				ID:           34,
				SenseID:      30,
				Source:       "wiktionary",
				ExampleOrder: 1,
				SentenceEN:   "I learn quickly.",
			}},
			LexicalRelations: []model.LexicalRelation{{
				ID:                   35,
				EntryID:              10,
				SenseID:              int64Ptr(30),
				RelationType:         model.RelationTypeSynonym,
				TargetText:           "study",
				TargetTextNormalized: "study",
				DisplayOrder:         1,
			}},
		}},
		WordVariants: []repository.WordVariant{{
			ID:              40,
			WordID:          10,
			FormText:        "learnt",
			NormalizedForm:  "learnt",
			RelationKind:    model.RelationKindForm,
			FormType:        stringPtr("past"),
			SourceRelations: pq.StringArray{"past"},
			DisplayOrder:    1,
		}},
		LexicalRelations: []model.LexicalRelation{{
			ID:                   50,
			EntryID:              10,
			RelationType:         model.RelationTypeDerived,
			TargetText:           "learner",
			TargetTextNormalized: "learner",
			DisplayOrder:         2,
		}},
	}

	service := NewWordService(&mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			if headword != "learn" || !includeVariants || !includePronunciations || !includeSenses {
				t.Fatalf("unexpected lookup args: headword=%q variants=%v pronunciations=%v senses=%v", headword, includeVariants, includePronunciations, includeSenses)
			}
			return word, nil, nil
		},
	}, createTestConfig())

	resp, err := service.GetWordByHeadword(context.Background(), "learn", nil, true, true, true)
	if err != nil {
		t.Fatalf("GetWordByHeadword() error = %v", err)
	}

	if resp.SourceRun == nil || resp.SourceRun.SourceName != "wiktionary" {
		t.Fatalf("source run not exposed: %#v", resp.SourceRun)
	}
	if resp.CEFRLevel != 2 || resp.CEFRLevelName != "A2" || resp.CEFRSource != model.CEFRSourceOxford || resp.CEFRRunID == nil || *resp.CEFRRunID != 91 {
		t.Fatalf("entry learning signal not exposed: %#v", resp.WordAnnotations)
	}
	if len(resp.CEFRSourceSignals) != 1 || resp.CEFRSourceSignals[0].Source != model.CEFRSourceCEFRJ || resp.CEFRSourceSignals[0].LevelName != "B1" {
		t.Fatalf("entry CEFR source signals not exposed: %#v", resp.CEFRSourceSignals)
	}
	if resp.Etymology == nil || resp.Etymology.TextClean == nil || *resp.Etymology.TextClean != cleanEtymology {
		t.Fatalf("etymology not exposed: %#v", resp.Etymology)
	}
	if len(resp.PronunciationAudios) != 1 || resp.PronunciationAudios[0].AudioFilename == "" {
		t.Fatalf("pronunciation audio not exposed: %#v", resp.PronunciationAudios)
	}
	if len(resp.Senses) != 1 {
		t.Fatalf("senses len = %d, want 1", len(resp.Senses))
	}
	sense := resp.Senses[0]
	if len(sense.DefinitionsEN) != 1 || len(sense.DefinitionsZH) != 1 || sense.DefinitionsZH[0].Romanization == nil {
		t.Fatalf("full gloss data not exposed: %#v", sense)
	}
	if len(sense.Labels) != 1 || sense.Labels[0].Code != model.RegisterLabelFormal {
		t.Fatalf("labels not exposed: %#v", sense.Labels)
	}
	if len(sense.CEFRSourceSignals) != 1 || sense.CEFRSourceSignals[0].Source != model.CEFRSourceOctanove {
		t.Fatalf("sense CEFR source signals not exposed: %#v", sense.CEFRSourceSignals)
	}
	if len(sense.LexicalRelations) != 1 || sense.LexicalRelations[0].TargetText != "study" {
		t.Fatalf("sense lexical relations not exposed: %#v", sense.LexicalRelations)
	}
	if len(resp.LexicalRelations) != 1 || resp.LexicalRelations[0].TargetText != "learner" {
		t.Fatalf("entry lexical relations not exposed: %#v", resp.LexicalRelations)
	}
	if len(resp.Variants) != 1 || resp.Variants[0].DisplayOrder != 1 {
		t.Fatalf("entry forms not exposed: %#v", resp.Variants)
	}
}

// TestGetWordsBatch_WithEntryFormsResolution tests batched entry_forms resolution
// when an input is not found as a direct entries headword.
func TestGetWordsBatch_WithEntryFormsResolution(t *testing.T) {
	cfg := createTestConfig()

	// Track which queries were made
	getWordsByHeadwordsCalled := false
	getWordsByVariantsCalls := 0

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			getWordsByHeadwordsCalled = true
			// Only return "apple"; "book" is not found as a direct entries headword.
			return []repository.Word{
				{ID: 1, Headword: "apple"},
			}, nil
		},
		getWordsByVariantsFunc: func(_ context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
			getWordsByVariantsCalls++
			expected := []string{"book", "xyz"}
			if !reflect.DeepEqual(variants, expected) {
				t.Fatalf("expected batch variant query %v, got %v", expected, variants)
			}
			return []repository.BatchVariantMatch{{
				Word:    repository.Word{ID: 2, Headword: "book"},
				Variant: repository.WordVariant{WordID: 2, FormText: "book"},
			}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	req := &BatchRequest{
		Words: []string{"apple", "book", "xyz"},
	}

	responses, meta, err := service.GetWordsBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Verify batch query was called
	if !getWordsByHeadwordsCalled {
		t.Error("Expected GetWordsByHeadwords to be called")
	}

	// Verify a single batch entry_forms query was triggered for unresolved words.
	if getWordsByVariantsCalls != 1 {
		t.Errorf("Expected 1 batch entry_forms query, got %d", getWordsByVariantsCalls)
	}

	// Verify results: 2 found (apple + book via entry_forms), 1 not found (xyz)
	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}

	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 3 {
		t.Errorf("Expected requested=3, got %d", *meta.Requested)
	}
	if *meta.Found != 2 {
		t.Errorf("Expected found=2, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 1 {
		t.Errorf("Expected 1 not found word, got %v", meta.NotFound)
	}
	if meta.NotFound[0] != "xyz" {
		t.Errorf("Expected 'xyz' in not found, got %v", meta.NotFound)
	}

	// Verify order is preserved (apple, book)
	if responses[0].Headword != "apple" {
		t.Errorf("Expected first response to be 'apple', got %s", responses[0].Headword)
	}
	if responses[1].Headword != "book" {
		t.Errorf("Expected second response to be 'book', got %s", responses[1].Headword)
	}
}

// TestGetWordsBatch_NormalizedFormMatching tests that batch query correctly uses
// normalized form for matching (e.g., "air-conditioning" matches "air conditioning")
func TestGetWordsBatch_NormalizedFormMatching(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			// Simulate database returns normalized form: "air conditioning" (with space)
			return []repository.Word{
				{ID: 1, Headword: "air conditioning"},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	// User queries with hyphen: "air-conditioning"
	req := &BatchRequest{
		Words: []string{"air-conditioning"},
	}

	responses, meta, err := service.GetWordsBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should find the word despite different spelling
	if len(responses) != 1 {
		t.Fatalf("Expected 1 response, got %d", len(responses))
	}

	if responses[0].Headword != "air conditioning" {
		t.Errorf("Expected 'air conditioning', got %s", responses[0].Headword)
	}

	if meta == nil || *meta.Found != 1 {
		t.Error("Expected to find 1 word via normalized matching")
	}
}

// TestGetWordsBatch_ApostrophePreservation tests that apostrophes are preserved
// in normalized form, distinguishing "it's" from "its"
func TestGetWordsBatch_ApostrophePreservation(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			// Return both words
			return []repository.Word{
				{ID: 1, Headword: "it's"},
				{ID: 2, Headword: "its"},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	req := &BatchRequest{
		Words: []string{"it's", "its"},
	}

	responses, meta, err := service.GetWordsBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Should find both words as distinct entries
	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}

	if responses[0].Headword != "it's" {
		t.Errorf("Expected first response 'it's', got %s", responses[0].Headword)
	}
	if responses[1].Headword != "its" {
		t.Errorf("Expected second response 'its', got %s", responses[1].Headword)
	}

	if meta == nil || *meta.Found != 2 {
		t.Error("Expected to find 2 distinct words")
	}
}

// TestSearchWords_KeywordLengthValidation tests input length validation
func TestSearchWords_KeywordLengthValidation(t *testing.T) {
	cfg := createTestConfig()
	mockRepo := &mockRepository{}
	service := NewWordService(mockRepo, cfg)

	tests := []struct {
		name        string
		keyword     string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid keyword",
			keyword:     "test",
			expectError: false,
		},
		{
			name:        "keyword too short (2 chars)",
			keyword:     "ab",
			expectError: true,
			errorMsg:    "at least 3 characters",
		},
		{
			name:        "keyword too short after normalization",
			keyword:     "a-b",
			expectError: true,
			errorMsg:    "at least 3 characters",
		},
		{
			name:        "keyword too long (>100 chars)",
			keyword:     string(make([]rune, 101)),
			expectError: true,
			errorMsg:    "not exceed 100 characters",
		},
		{
			name:        "minimum valid length (3 chars)",
			keyword:     "abc",
			expectError: false,
		},
		{
			name:        "maximum valid length (100 chars)",
			keyword:     string(make([]rune, 100)),
			expectError: false,
		},
		{
			name:        "unicode characters",
			keyword:     "����",
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := service.SearchWords(context.Background(), tt.keyword, nil, nil, nil, nil, nil, nil, 10, 0)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for keyword %q, got nil", tt.keyword)
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for keyword %q, got %v", tt.keyword, err)
				}
			}
		})
	}
}

// TestSuggestWords_PrefixLengthValidation tests prefix length validation
func TestSuggestWords_PrefixLengthValidation(t *testing.T) {
	cfg := createTestConfig()
	mockRepo := &mockRepository{}
	service := NewWordService(mockRepo, cfg)

	tests := []struct {
		name        string
		prefix      string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "valid prefix",
			prefix:      "test",
			expectError: false,
		},
		{
			name:        "prefix too short (2 chars)",
			prefix:      "ab",
			expectError: true,
			errorMsg:    "at least 3 characters",
		},
		{
			name:        "prefix too short after normalization",
			prefix:      "a_a",
			expectError: true,
			errorMsg:    "at least 3 characters",
		},
		{
			name:        "prefix too long (>50 chars)",
			prefix:      string(make([]rune, 51)),
			expectError: true,
			errorMsg:    "not exceed 50 characters",
		},
		{
			name:        "minimum valid length (3 chars)",
			prefix:      "abc",
			expectError: false,
		},
		{
			name:        "maximum valid length (50 chars)",
			prefix:      string(make([]rune, 50)),
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := service.SuggestWords(context.Background(), tt.prefix, nil, nil, nil, nil, nil, 10)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error for prefix %q, got nil", tt.prefix)
				} else if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error for prefix %q, got %v", tt.prefix, err)
				}
			}
		})
	}
}

// TestGetWordsBatch_MixedScenario tests a realistic scenario with:
// - Direct entries matches
// - entry_forms matches
// - Not found words
// - Different spellings (normalized form matching)
func TestGetWordsBatch_MixedScenario(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			// Direct entries lookup returns: "apple", "air conditioning"
			return []repository.Word{
				{ID: 1, Headword: "apple"},
				{ID: 2, Headword: "air conditioning"},
			}, nil
		},
		getWordsByVariantsFunc: func(_ context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
			expected := []string{"lit", "nonexistent"}
			if !reflect.DeepEqual(variants, expected) {
				t.Fatalf("expected batch variant query %v, got %v", expected, variants)
			}
			return []repository.BatchVariantMatch{{
				Word:    repository.Word{ID: 3, Headword: "light"},
				Variant: repository.WordVariant{WordID: 3, FormText: "lit"},
			}}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	req := &BatchRequest{
		Words: []string{"apple", "air-conditioning", "lit", "nonexistent"},
	}

	responses, meta, err := service.GetWordsBatch(context.Background(), req)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Expected results:
	// 1. "apple" - direct match
	// 2. "air-conditioning" - matched "air conditioning" via normalized form
	// 3. "lit" - found "light" via entry_forms
	// 4. "nonexistent" - not found

	if len(responses) != 3 {
		t.Fatalf("Expected 3 responses, got %d", len(responses))
	}

	expectedHeadwords := []string{"apple", "air conditioning", "light"}
	for i, expected := range expectedHeadwords {
		if responses[i].Headword != expected {
			t.Errorf("Response[%d]: expected %s, got %s", i, expected, responses[i].Headword)
		}
	}

	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 4 {
		t.Errorf("Expected requested=4, got %d", *meta.Requested)
	}
	if *meta.Found != 3 {
		t.Errorf("Expected found=3, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 1 || meta.NotFound[0] != "nonexistent" {
		t.Errorf("Expected not found=['nonexistent'], got %v", meta.NotFound)
	}
}

func TestGetWordsBatch_BatchVariantResolutionPreservesOrderAndNotFound(t *testing.T) {
	cfg := createTestConfig()

	batchVariantCalls := 0
	individualLookupCalls := 0

	mockRepo := &mockRepository{
		getWordByHeadwordFunc: func(_ context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*repository.Word, *repository.WordVariant, error) {
			individualLookupCalls++
			t.Fatalf("unexpected per-word lookup for %q", headword)
			return nil, nil, errors.New("unexpected per-word lookup")
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{{ID: 1, Headword: "apple"}}, nil
		},
		getWordsByVariantsFunc: func(_ context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
			batchVariantCalls++
			expected := []string{"learnt", "Lit", "missing"}
			if !reflect.DeepEqual(variants, expected) {
				t.Fatalf("expected batch entry_forms variants %v, got %v", expected, variants)
			}

			return []repository.BatchVariantMatch{
				{
					Word:    repository.Word{ID: 3, Headword: "Light"},
					Variant: repository.WordVariant{WordID: 3, FormText: "Lit", RelationKind: model.RelationKindAlias},
				},
				{
					Word:    repository.Word{ID: 2, Headword: "learn"},
					Variant: repository.WordVariant{WordID: 2, FormText: "learnt", RelationKind: model.RelationKindForm},
				},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"apple", "learnt", "Lit", "missing"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if batchVariantCalls != 1 {
		t.Fatalf("expected 1 batch entry_forms call, got %d", batchVariantCalls)
	}
	if individualLookupCalls != 0 {
		t.Fatalf("expected no per-word lookup calls, got %d", individualLookupCalls)
	}
	assertBatchVariantResolutionResponses(t, responses)
	assertBatchVariantResolutionMeta(t, meta)
}

func assertBatchVariantResolutionResponses(t *testing.T, responses []WordResponse) {
	t.Helper()
	if len(responses) != 3 {
		t.Fatalf("Expected 3 responses, got %d", len(responses))
	}
	expectedHeadwords := []string{"apple", "learn", "Light"}
	for i, expected := range expectedHeadwords {
		if responses[i].Headword != expected {
			t.Fatalf("response[%d] headword = %q, want %q", i, responses[i].Headword, expected)
		}
	}
	if responses[0].QueriedVariant != nil {
		t.Fatalf("expected direct match to have no queried_variant, got %#v", responses[0].QueriedVariant)
	}
	if responses[1].QueriedVariant == nil || responses[1].QueriedVariant.FormText != "learnt" {
		t.Fatalf("expected learnt queried_variant metadata, got %#v", responses[1].QueriedVariant)
	}
	if responses[1].QueriedVariant.RelationKind != model.RelationKindForm {
		t.Fatalf("expected learnt queried_variant kind=form, got %#v", responses[1].QueriedVariant)
	}
	if responses[2].QueriedVariant == nil || responses[2].QueriedVariant.FormText != "Lit" {
		t.Fatalf("expected Lit queried_variant metadata, got %#v", responses[2].QueriedVariant)
	}
}

func assertBatchVariantResolutionMeta(t *testing.T, meta *MetaInfo) {
	t.Helper()
	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Requested != 4 {
		t.Fatalf("expected requested=4, got %d", *meta.Requested)
	}
	if *meta.Found != 3 {
		t.Fatalf("expected found=3, got %d", *meta.Found)
	}
	if !reflect.DeepEqual(meta.NotFound, []string{"missing"}) {
		t.Fatalf("expected not_found [missing], got %v", meta.NotFound)
	}
}

func TestGetWordsBatch_BatchVariantResolutionPreservesCaseSelection(t *testing.T) {
	cfg := createTestConfig()

	mockRepo := &mockRepository{
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{}, nil
		},
		getWordsByVariantsFunc: func(_ context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.BatchVariantMatch, error) {
			expected := []string{"Polish", "POLISH"}
			if !reflect.DeepEqual(variants, expected) {
				t.Fatalf("expected batch entry_forms variants %v, got %v", expected, variants)
			}

			return []repository.BatchVariantMatch{
				{
					Word:    repository.Word{ID: 10, Headword: "Polish"},
					Variant: repository.WordVariant{WordID: 10, FormText: "Polish"},
				},
				{
					Word:    repository.Word{ID: 11, Headword: "polish"},
					Variant: repository.WordVariant{WordID: 11, FormText: "POLISH"},
				},
			}, nil
		},
	}

	service := NewWordService(mockRepo, cfg)

	responses, meta, err := service.GetWordsBatch(context.Background(), &BatchRequest{
		Words: []string{"Polish", "POLISH"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(responses) != 2 {
		t.Fatalf("Expected 2 responses, got %d", len(responses))
	}
	if responses[0].Headword != "Polish" {
		t.Fatalf("expected exact-case match 'Polish', got %q", responses[0].Headword)
	}
	if responses[1].Headword != "polish" {
		t.Fatalf("expected lowercase match 'polish', got %q", responses[1].Headword)
	}

	if meta == nil {
		t.Fatal("Expected meta info, got nil")
	}
	if *meta.Found != 2 {
		t.Fatalf("expected found=2, got %d", *meta.Found)
	}
	if len(meta.NotFound) != 0 {
		t.Fatalf("expected no not_found entries, got %v", meta.NotFound)
	}
}
