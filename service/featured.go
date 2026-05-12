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
	words   []repository.FeaturedCandidate
	phrases []repository.FeaturedCandidate
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

	return s.hydrateFeaturedCandidates(ctx, sampleFeaturedCandidates(candidates, limit, s.shuffle), phrases)
}

func (s *WordService) loadFeaturedCandidates(ctx context.Context, phrases bool) ([]repository.FeaturedCandidate, error) {
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

	candidates, err := s.repo.ListFeaturedCandidates(ctx)
	if err != nil {
		return nil, wrapFeaturedSourceError("list featured candidates", err)
	}

	s.featuredCandidates = buildFeaturedCandidateCache(candidates)
	return s.featuredCandidates.group(phrases), nil
}

func buildFeaturedCandidateCache(candidates []repository.FeaturedCandidate) featuredCandidateCache {
	cache := featuredCandidateCache{
		loaded:  true,
		words:   make([]repository.FeaturedCandidate, 0, len(candidates)),
		phrases: make([]repository.FeaturedCandidate, 0, len(candidates)),
	}
	for _, candidate := range candidates {
		if isPhraseHeadword(candidate.Headword) {
			cache.phrases = append(cache.phrases, candidate)
			continue
		}
		cache.words = append(cache.words, candidate)
	}
	return cache
}

func (cache featuredCandidateCache) group(phrases bool) []repository.FeaturedCandidate {
	if phrases {
		return cache.phrases
	}
	return cache.words
}

func (s *WordService) hydrateFeaturedCandidates(ctx context.Context, candidates []repository.FeaturedCandidate, phrases bool) ([]SuggestResponse, error) {
	headwords := featuredCandidateHeadwords(candidates)
	words, err := s.repo.GetWordsByHeadwords(ctx, headwords, false, false, false)
	if err != nil {
		return nil, wrapFeaturedSourceError("hydrate featured batch", err)
	}

	indexedWords := indexFeaturedWordsByID(words)
	results := make([]SuggestResponse, 0, len(candidates))
	for _, candidate := range candidates {
		word, ok := indexedWords[candidate.EntryID]
		if !ok {
			return nil, fmt.Errorf("%w: missing featured entry %d (%q)", ErrFeaturedBatchIncomplete, candidate.EntryID, candidate.Headword)
		}
		if word.Headword != candidate.Headword {
			return nil, fmt.Errorf("%w: featured entry %d hydrated as %q, want %q", ErrFeaturedBatchIncomplete, candidate.EntryID, word.Headword, candidate.Headword)
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

func featuredCandidateHeadwords(candidates []repository.FeaturedCandidate) []string {
	headwords := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		headwords = append(headwords, candidate.Headword)
	}
	return headwords
}

func indexFeaturedWordsByID(words []repository.Word) map[int64]*repository.Word {
	index := make(map[int64]*repository.Word, len(words))
	for i := range words {
		index[words[i].ID] = &words[i]
	}
	return index
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

func sampleFeaturedCandidates(candidates []repository.FeaturedCandidate, limit int, shuffle func([]string)) []repository.FeaturedCandidate {
	if shuffle != nil {
		headwords := featuredCandidateHeadwords(candidates)
		byHeadword := make(map[string]repository.FeaturedCandidate, len(candidates))
		for _, candidate := range candidates {
			byHeadword[candidate.Headword] = candidate
		}
		shuffle(headwords)
		sampled := make([]repository.FeaturedCandidate, 0, limit)
		for _, headword := range headwords {
			candidate, ok := byHeadword[headword]
			if !ok {
				continue
			}
			sampled = append(sampled, candidate)
			if len(sampled) == limit {
				return sampled
			}
		}
		return sampled
	}
	return defaultFeaturedSample(candidates, limit)
}

func defaultFeaturedSample(candidates []repository.FeaturedCandidate, limit int) []repository.FeaturedCandidate {
	if limit >= len(candidates) {
		return append([]repository.FeaturedCandidate(nil), candidates...)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	selectedIndexes := make(map[int]struct{}, limit)
	sampled := make([]repository.FeaturedCandidate, 0, limit)
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
