package repository

import "context"

// BatchVariantMatch binds a matched variant row to the word it resolved to.
// Each element represents one successful requested input in batch lookup order.
type BatchVariantMatch struct {
	Word    Word
	Variant WordVariant
}

// WordRepository defines the interface for word data access.
//
// Not-found behavior is part of this contract for methods that return a single
// lookup result. Implementations and decorators must return ErrWordNotFound /
// ErrVariantNotFound, or an equivalent wrapped error that matches via
// errors.Is, so upper layers can map not-found cases consistently.
type WordRepository interface {
	// GetWordByHeadword returns ErrWordNotFound, or a wrapped equivalent matched
	// by errors.Is(err, ErrWordNotFound), when neither a headword nor a variant
	// resolves the requested input.
	GetWordByHeadword(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*Word, *WordVariant, error)
	GetWordsByHeadwords(ctx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]Word, error)
	GetWordsByVariants(ctx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]BatchVariantMatch, error)
	// GetWordsByVariant returns ErrVariantNotFound, or a wrapped equivalent
	// matched by errors.Is(err, ErrVariantNotFound), when no variant matches the
	// requested input.
	GetWordsByVariant(ctx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]Word, []WordVariant, error)
	// ListFeaturedCandidateHeadwords returns quality-filtered headwords used to
	// build the in-memory featured recommendation pool.
	ListFeaturedCandidateHeadwords(ctx context.Context) ([]string, error)
	SearchWords(ctx context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]Word, int64, error)
	SuggestWords(ctx context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]Word, error)
	SearchPhrases(ctx context.Context, keyword string, limit int) ([]Word, error)
	GetPronunciationsByWordID(ctx context.Context, wordID int64, accent *string) ([]Pronunciation, error)
	GetSensesByWordID(ctx context.Context, wordID int64, pos *string) ([]Sense, error)
}

// Ensure Repository implements WordRepository interface
var _ WordRepository = (*Repository)(nil)
