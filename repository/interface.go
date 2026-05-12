package repository

import "context"

// BatchVariantMatch binds a matched variant row to the word it resolved to.
// Each element represents one successful requested input in batch lookup order.
type BatchVariantMatch struct {
	Word    Word
	Variant WordVariant
}

// FeaturedCandidate binds an upstream featured candidate to its exact entry.
// The commons read model selects one entry per normalized headword; entry id
// keeps hydration pinned to that selected POS entry.
type FeaturedCandidate struct {
	EntryID  int64
	Headword string
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
	GetEntryGroupByHeadword(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) ([]Word, *WordVariant, error)
	GetWordsByHeadwords(ctx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]Word, error)
	GetWordsByVariants(ctx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]BatchVariantMatch, error)
	GetHeadwordRelationGroups(ctx context.Context, headword string, posCode int, opts RelationQueryOptions) ([]HeadwordRelationGroup, error)
	// GetWordsByVariant returns ErrVariantNotFound, or a wrapped equivalent
	// matched by errors.Is(err, ErrVariantNotFound), when no variant matches the
	// requested input.
	GetWordsByVariant(ctx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]Word, []WordVariant, error)
	// ListFeaturedCandidates returns the upstream canonical featured_candidates
	// rows used to build the in-memory recommendation pool. Eligibility,
	// per-headword selection, and ranking are owned by the commons read model.
	ListFeaturedCandidates(ctx context.Context) ([]FeaturedCandidate, error)
	SearchWords(ctx context.Context, keyword string, opts SearchOptions) ([]Word, int64, error)
	SuggestWords(ctx context.Context, prefix string, opts SuggestOptions) ([]Word, error)
	SearchPhrases(ctx context.Context, keyword string, limit int) ([]Word, error)
	GetPronunciationsByWordID(ctx context.Context, wordID int64, accent *string) ([]Pronunciation, error)
	GetSensesByWordID(ctx context.Context, wordID int64, pos *string) ([]Sense, error)
}

// Ensure Repository implements WordRepository interface
var _ WordRepository = (*Repository)(nil)
