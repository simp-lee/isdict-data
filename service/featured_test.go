package service

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/simp-lee/isdict-data/repository"
)

func TestRandomFeaturedWords_UsesSharedContractAndMinimalBatchHydration(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return featuredCandidates("learn", "look after", "example", "turn on"), nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			if !reflect.DeepEqual(headwords, []string{"learn", "example"}) {
				t.Fatalf("headwords = %v; want %v", headwords, []string{"learn", "example"})
			}
			if includeVariants || includePronunciations || includeSenses {
				t.Fatalf("expected minimal hydration flags, got variants=%v pronunciations=%v senses=%v", includeVariants, includePronunciations, includeSenses)
			}
			return []repository.Word{
				wordWithIDSummary(1, "learn", "学习"),
				wordWithIDSummary(3, "example", "例子"),
			}, nil
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	got, err := service.RandomFeaturedWords(context.Background(), 2)
	if err != nil {
		t.Fatalf("RandomFeaturedWords() error = %v", err)
	}

	want := []SuggestResponse{
		{Headword: "learn", WordAnnotations: WordAnnotations{TranslationZH: "学习"}},
		{Headword: "example", WordAnnotations: WordAnnotations{TranslationZH: "例子"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RandomFeaturedWords() = %#v; want %#v", got, want)
	}
}

func TestRandomFeaturedPhrases_RejectsInvalidLimit(t *testing.T) {
	service := NewWordService(&mockRepository{}, createTestConfig())

	_, err := service.RandomFeaturedPhrases(context.Background(), 0)
	if !errors.Is(err, ErrFeaturedLimitInvalid) {
		t.Fatalf("RandomFeaturedPhrases() error = %v; want %v", err, ErrFeaturedLimitInvalid)
	}
}

func TestRandomFeaturedWords_RejectsLimitAboveBatchMax(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			t.Fatal("expected limit validation before loading candidates")
			return nil, nil
		},
	}, ServiceConfig{
		BatchMaxSize:    1,
		SearchMaxLimit:  100,
		SuggestMaxLimit: 50,
	})

	_, err := service.RandomFeaturedWords(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedLimitInvalid) {
		t.Fatalf("RandomFeaturedWords() error = %v; want %v", err, ErrFeaturedLimitInvalid)
	}
	if errors.Is(err, ErrFeaturedSourceUnavailable) {
		t.Fatalf("RandomFeaturedWords() error = %v; should not be wrapped as source unavailable", err)
	}
}

func TestRandomFeaturedPhrases_ReturnsTypedErrorWhenPoolIsTooSmall(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return featuredCandidates("learn", "look after", "example"), nil
		},
	}, createTestConfig())

	_, err := service.RandomFeaturedPhrases(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedCandidatesExhausted) {
		t.Fatalf("RandomFeaturedPhrases() error = %v; want %v", err, ErrFeaturedCandidatesExhausted)
	}
}

func TestRandomFeaturedWords_ReturnsTypedErrorWhenBatchIsIncomplete(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return featuredCandidates("learn", "example"), nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{wordWithIDSummary(1, "learn", "学习")}, nil
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	_, err := service.RandomFeaturedWords(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedBatchIncomplete) {
		t.Fatalf("RandomFeaturedWords() error = %v; want %v", err, ErrFeaturedBatchIncomplete)
	}
}

func TestRandomFeaturedWords_CachesFeaturedCandidatePool(t *testing.T) {
	listCalls := 0
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			listCalls++
			return featuredCandidates("learn", "look after", "example", "turn on"), nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			words := make([]repository.Word, 0, len(headwords))
			for _, headword := range headwords {
				words = append(words, wordWithIDSummary(featuredCandidateID(headword), headword, headword))
			}
			return words, nil
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	if _, err := service.RandomFeaturedWords(context.Background(), 2); err != nil {
		t.Fatalf("RandomFeaturedWords() error = %v", err)
	}
	if _, err := service.RandomFeaturedPhrases(context.Background(), 1); err != nil {
		t.Fatalf("RandomFeaturedPhrases() error = %v", err)
	}
	if listCalls != 1 {
		t.Fatalf("ListFeaturedCandidates() call count = %d; want 1", listCalls)
	}
}

func TestRandomFeaturedWords_RejectsHydrationHeadwordMismatches(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return featuredCandidates("learn", "example"), nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return []repository.Word{
				wordWithIDSummary(1, "learnt", "学习"),
				wordWithIDSummary(2, "example", "例子"),
			}, nil
		},
		getWordsByVariantsFunc: func(context.Context, []string, bool, bool, bool) ([]repository.BatchVariantMatch, error) {
			t.Fatal("expected featured hydration to avoid entry_forms lookup")
			return nil, nil
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	_, err := service.RandomFeaturedWords(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedBatchIncomplete) {
		t.Fatalf("RandomFeaturedWords() error = %v; want %v", err, ErrFeaturedBatchIncomplete)
	}
}

func TestRandomFeaturedWords_WrapsListFailureAndPreservesCause(t *testing.T) {
	rootErr := errors.New("bootstrap query failed")
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return nil, rootErr
		},
	}, createTestConfig())

	_, err := service.RandomFeaturedWords(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedSourceUnavailable) {
		t.Fatalf("RandomFeaturedWords() error = %v; want %v", err, ErrFeaturedSourceUnavailable)
	}
	if !errors.Is(err, rootErr) {
		t.Fatalf("RandomFeaturedWords() error = %v; want wrapped cause %v", err, rootErr)
	}
}

func TestRandomFeaturedWords_WrapsBatchFailureAndPreservesCause(t *testing.T) {
	rootErr := errors.New("batch hydration failed")
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return featuredCandidates("learn", "example"), nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			return nil, rootErr
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	_, err := service.RandomFeaturedWords(context.Background(), 2)
	if !errors.Is(err, ErrFeaturedSourceUnavailable) {
		t.Fatalf("RandomFeaturedWords() error = %v; want %v", err, ErrFeaturedSourceUnavailable)
	}
	if !errors.Is(err, rootErr) {
		t.Fatalf("RandomFeaturedWords() error = %v; want wrapped cause %v", err, rootErr)
	}
}

func TestRandomFeaturedWords_HydratesExactFeaturedEntryID(t *testing.T) {
	service := NewWordService(&mockRepository{
		listFeaturedCandidatesFunc: func(context.Context) ([]repository.FeaturedCandidate, error) {
			return []repository.FeaturedCandidate{{EntryID: 20, Headword: "abandon"}}, nil
		},
		getWordsByHeadwordsFunc: func(_ context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]repository.Word, error) {
			if !reflect.DeepEqual(headwords, []string{"abandon"}) {
				t.Fatalf("headwords = %v; want [abandon]", headwords)
			}
			return []repository.Word{
				{ID: 10, Headword: "abandon", LearningSignal: entryLearningSignal(0, "")},
				{ID: 20, Headword: "abandon", LearningSignal: entryLearningSignal(4, "")},
			}, nil
		},
	}, createTestConfig())
	service.shuffle = func([]string) {}

	got, err := service.RandomFeaturedWords(context.Background(), 1)
	if err != nil {
		t.Fatalf("RandomFeaturedWords() error = %v", err)
	}
	if len(got) != 1 || got[0].CEFRLevel != 4 {
		t.Fatalf("RandomFeaturedWords() = %#v, want selected entry CEFR level 4", got)
	}
}

func featuredCandidates(headwords ...string) []repository.FeaturedCandidate {
	candidates := make([]repository.FeaturedCandidate, 0, len(headwords))
	for i, headword := range headwords {
		candidates = append(candidates, repository.FeaturedCandidate{EntryID: int64(i + 1), Headword: headword})
	}
	return candidates
}

func featuredCandidateID(headword string) int64 {
	for _, candidate := range featuredCandidates("learn", "look after", "example", "turn on") {
		if candidate.Headword == headword {
			return candidate.EntryID
		}
	}
	return 0
}

func wordWithIDSummary(id int64, headword, summary string) repository.Word {
	word := wordWithSummary(headword, summary)
	word.ID = id
	return word
}
