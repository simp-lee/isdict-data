package service

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/simp-lee/isdict-commons/norm"
	"github.com/simp-lee/isdict-data/repository"
)

var (
	ErrFeaturedLimitInvalid        = errors.New("featured limit must be greater than zero")
	ErrFeaturedSourceUnavailable   = errors.New("featured source unavailable")
	ErrFeaturedCandidatesExhausted = errors.New("featured candidates exhausted")
	ErrFeaturedBatchIncomplete     = errors.New("featured batch lookup incomplete")
)

type featuredCandidateCache struct {
	loaded  bool
	words   []string
	phrases []string
}

type featuredSourceError struct {
	operation string
	cause     error
}

func (e *featuredSourceError) Error() string {
	if e == nil || e.cause == nil {
		return ErrFeaturedSourceUnavailable.Error()
	}
	if strings.TrimSpace(e.operation) == "" {
		return fmt.Sprintf("%s: %v", ErrFeaturedSourceUnavailable, e.cause)
	}
	return fmt.Sprintf("%s during %s: %v", ErrFeaturedSourceUnavailable, e.operation, e.cause)
}

func (e *featuredSourceError) Unwrap() []error {
	if e == nil || e.cause == nil {
		return []error{ErrFeaturedSourceUnavailable}
	}
	return []error{ErrFeaturedSourceUnavailable, e.cause}
}

func wrapFeaturedSourceError(operation string, err error) error {
	if err == nil || errors.Is(err, ErrFeaturedSourceUnavailable) {
		return err
	}
	return &featuredSourceError{operation: operation, cause: err}
}

// RandomFeaturedWords returns exactly limit single-word headwords for the
// homepage featured module.
//
// Error contract:
//   - ErrFeaturedLimitInvalid when limit <= 0 or exceeds the configured featured batch maximum
//   - ErrFeaturedCandidatesExhausted when the upstream pool cannot satisfy the requested count
//   - ErrFeaturedBatchIncomplete when hydration cannot return the sampled count or grouping
//   - ErrFeaturedSourceUnavailable for upstream source failures, preserving the root cause via errors.Is
func (s *WordService) RandomFeaturedWords(ctx context.Context, limit int) ([]SuggestResponse, error) {
	return s.randomFeatured(ctx, limit, false)
}

// RandomFeaturedPhrases returns exactly limit phrase headwords for the
// homepage featured module and follows the same error contract as
// RandomFeaturedWords.
func (s *WordService) RandomFeaturedPhrases(ctx context.Context, limit int) ([]SuggestResponse, error) {
	return s.randomFeatured(ctx, limit, true)
}

func (s *WordService) randomFeatured(ctx context.Context, limit int, phrases bool) ([]SuggestResponse, error) {
	if limit <= 0 {
		return nil, ErrFeaturedLimitInvalid
	}
	if s == nil || s.repo == nil {
		return nil, ErrFeaturedSourceUnavailable
	}
	if limit > normalizeServiceConfig(s.config).BatchMaxSize {
		return nil, fmt.Errorf("%w: maximum %d featured entries per request", ErrFeaturedLimitInvalid, normalizeServiceConfig(s.config).BatchMaxSize)
	}

	candidates, err := s.loadFeaturedCandidates(ctx, phrases)
	if err != nil {
		return nil, err
	}

	if len(candidates) < limit {
		return nil, fmt.Errorf("%w: requested %d %s, available %d", ErrFeaturedCandidatesExhausted, limit, featuredGroupName(phrases), len(candidates))
	}

	return s.hydrateFeaturedHeadwords(ctx, sampleFeaturedCandidates(candidates, limit, s.shuffle), phrases)
}

func (s *WordService) loadFeaturedCandidates(ctx context.Context, phrases bool) ([]string, error) {
	s.featuredCandidatesMu.RLock()
	if s.featuredCandidates.loaded {
		candidates := s.featuredCandidates.group(phrases)
		s.featuredCandidatesMu.RUnlock()
		return candidates, nil
	}
	s.featuredCandidatesMu.RUnlock()

	s.featuredCandidatesMu.Lock()
	defer s.featuredCandidatesMu.Unlock()
	if s.featuredCandidates.loaded {
		return s.featuredCandidates.group(phrases), nil
	}

	headwords, err := s.repo.ListFeaturedCandidateHeadwords(ctx)
	if err != nil {
		return nil, wrapFeaturedSourceError("list featured candidates", err)
	}

	s.featuredCandidates = buildFeaturedCandidateCache(headwords)
	return s.featuredCandidates.group(phrases), nil
}

func buildFeaturedCandidateCache(headwords []string) featuredCandidateCache {
	cache := featuredCandidateCache{
		loaded:  true,
		words:   make([]string, 0, len(headwords)),
		phrases: make([]string, 0, len(headwords)),
	}
	wordIndexes := make(map[string]int, len(headwords))
	phraseIndexes := make(map[string]int, len(headwords))
	for _, headword := range headwords {
		if isPhraseHeadword(headword) {
			appendUniqueFeaturedHeadword(&cache.phrases, phraseIndexes, headword)
			continue
		}
		appendUniqueFeaturedHeadword(&cache.words, wordIndexes, headword)
	}
	return cache
}

func (cache featuredCandidateCache) group(phrases bool) []string {
	if phrases {
		return cache.phrases
	}
	return cache.words
}

func appendUniqueFeaturedHeadword(target *[]string, indexes map[string]int, headword string) {
	trimmed := strings.TrimSpace(headword)
	if trimmed == "" {
		return
	}
	key := norm.NormalizeHeadword(trimmed)
	if key == "" {
		return
	}
	if existingIndex, ok := indexes[key]; ok {
		if isCanonicalFeaturedHeadword(trimmed) && !isCanonicalFeaturedHeadword((*target)[existingIndex]) {
			(*target)[existingIndex] = trimmed
		}
		return
	}
	indexes[key] = len(*target)
	*target = append(*target, trimmed)
}

func isCanonicalFeaturedHeadword(headword string) bool {
	return headword == strings.ToLower(headword)
}

func (s *WordService) hydrateFeaturedHeadwords(ctx context.Context, headwords []string, phrases bool) ([]SuggestResponse, error) {
	words, err := s.repo.GetWordsByHeadwords(ctx, headwords, false, false, false)
	if err != nil {
		return nil, wrapFeaturedSourceError("hydrate featured batch", err)
	}

	indexedWords := indexFeaturedWords(words)
	results := make([]SuggestResponse, 0, len(headwords))
	for _, headword := range headwords {
		word, ok := indexedWords.exact(headword)
		if !ok {
			return nil, fmt.Errorf("%w: missing featured headword %q", ErrFeaturedBatchIncomplete, headword)
		}
		if isPhraseHeadword(word.Headword) != phrases {
			return nil, fmt.Errorf("%w: %q fell outside %s grouping", ErrFeaturedBatchIncomplete, word.Headword, featuredGroupName(phrases))
		}
		response := s.convertToWordResponse(word, nil, nil, false, false, false)
		results = append(results, SuggestResponse{
			Headword:        response.Headword,
			WordAnnotations: response.WordAnnotations,
		})
	}

	return results, nil
}

type featuredWordIndex map[string][]*repository.Word

func indexFeaturedWords(words []repository.Word) featuredWordIndex {
	index := make(featuredWordIndex, len(words))
	for i := range words {
		key := norm.NormalizeHeadword(words[i].Headword)
		index[key] = append(index[key], &words[i])
	}
	return index
}

func (index featuredWordIndex) exact(headword string) (*repository.Word, bool) {
	for _, candidate := range index[norm.NormalizeHeadword(headword)] {
		if candidate.Headword == headword {
			return candidate, true
		}
	}
	return nil, false
}

func featuredGroupName(phrases bool) string {
	if phrases {
		return "phrases"
	}
	return "words"
}

func isPhraseHeadword(headword string) bool {
	return norm.IsMultiword(headword)
}

func sampleFeaturedCandidates(candidates []string, limit int, shuffle func([]string)) []string {
	if shuffle != nil {
		sampled := append([]string(nil), candidates...)
		shuffle(sampled)
		return sampled[:limit]
	}
	return defaultFeaturedSample(candidates, limit)
}

func defaultFeaturedSample(candidates []string, limit int) []string {
	if limit >= len(candidates) {
		return append([]string(nil), candidates...)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	selectedIndexes := make(map[int]struct{}, limit)
	sampled := make([]string, 0, limit)
	for cursor := len(candidates) - limit; cursor < len(candidates); cursor++ {
		index := rng.Intn(cursor + 1)
		if _, exists := selectedIndexes[index]; exists {
			index = cursor
		}
		selectedIndexes[index] = struct{}{}
		sampled = append(sampled, candidates[index])
	}
	return sampled
}
