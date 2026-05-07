package repository

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-commons/norm"
	"gorm.io/gorm"
)

// Word is the read model exposed by this data package. It is backed by the
// commons v1 entries schema and preloads the related v1 tables used by callers.
type Word struct {
	ID                 int64     `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	Headword           string    `gorm:"type:text;not null;uniqueIndex:idx_entries_headword_pos_etymology_index,priority:1;index:idx_entries_headword"`
	NormalizedHeadword string    `gorm:"column:normalized_headword;type:text;not null;index:idx_entries_normalized_headword"`
	Pos                string    `gorm:"column:pos;type:text;not null;uniqueIndex:idx_entries_headword_pos_etymology_index,priority:2;index:idx_entries_pos"`
	EtymologyIndex     int       `gorm:"type:integer;not null;default:0;check:etymology_index >= 0;uniqueIndex:idx_entries_headword_pos_etymology_index,priority:3"`
	IsMultiword        bool      `gorm:"type:boolean;not null;default:false"`
	SourceRunID        int64     `gorm:"type:bigint;autoIncrement:false;not null;index:idx_entries_source_run_id"`
	CreatedAt          time.Time `gorm:"type:timestamptz;not null;default:now()"`
	UpdatedAt          time.Time `gorm:"type:timestamptz;not null;default:now()"`

	SourceRun           *model.ImportRun              `gorm:"foreignKey:SourceRunID;references:ID"`
	LearningSignal      *model.EntryLearningSignal    `gorm:"foreignKey:EntryID;references:ID"`
	CEFRSourceSignals   []model.EntryCEFRSourceSignal `gorm:"foreignKey:EntryID;references:ID"`
	SummariesZH         []model.EntrySummaryZH        `gorm:"foreignKey:EntryID;references:ID"`
	Etymology           *model.EntryEtymology         `gorm:"foreignKey:EntryID;references:ID"`
	Pronunciations      []Pronunciation               `gorm:"foreignKey:WordID;references:ID"`
	PronunciationAudios []PronunciationAudio          `gorm:"foreignKey:WordID;references:ID"`
	Senses              []Sense                       `gorm:"foreignKey:WordID;references:ID"`
	WordVariants        []WordVariant                 `gorm:"foreignKey:WordID;references:ID"`
	LexicalRelations    []model.LexicalRelation       `gorm:"foreignKey:EntryID;references:ID"`
}

func (Word) TableName() string {
	return "entries"
}

// Pronunciation maps the v1 pronunciation_ipas table.
type Pronunciation struct {
	ID           int64  `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	WordID       int64  `gorm:"column:entry_id;type:bigint;autoIncrement:false;not null;index:idx_pronunciation_ipas_entry_id_accent_code_display_order,priority:1"`
	Accent       string `gorm:"column:accent_code;type:text;not null;default:'unknown';index:idx_pronunciation_ipas_entry_id_accent_code_display_order,priority:2"`
	IPA          string `gorm:"column:ipa;type:text;not null"`
	IsPrimary    bool   `gorm:"type:boolean;not null;default:false"`
	DisplayOrder int    `gorm:"type:smallint;not null;default:1;index:idx_pronunciation_ipas_entry_id_accent_code_display_order,priority:3"`
}

func (Pronunciation) TableName() string {
	return "pronunciation_ipas"
}

// PronunciationAudio maps the v1 pronunciation_audios table.
type PronunciationAudio struct {
	ID            int64  `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	WordID        int64  `gorm:"column:entry_id;type:bigint;autoIncrement:false;not null;index:idx_pronunciation_audios_entry_id_accent_code_display_order,priority:1"`
	Accent        string `gorm:"column:accent_code;type:text;not null;default:'unknown';index:idx_pronunciation_audios_entry_id_accent_code_display_order,priority:2"`
	AudioFilename string `gorm:"column:audio_filename;type:text;not null;index:idx_pronunciation_audios_audio_filename"`
	IsPrimary     bool   `gorm:"type:boolean;not null;default:false"`
	DisplayOrder  int    `gorm:"type:smallint;not null;default:1;index:idx_pronunciation_audios_entry_id_accent_code_display_order,priority:3"`
}

func (PronunciationAudio) TableName() string {
	return "pronunciation_audios"
}

// Sense maps the v1 senses table and preloaded gloss/signal rows to the
// response-friendly shape consumed by service conversions.
type Sense struct {
	ID         int64 `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	WordID     int64 `gorm:"column:entry_id;type:bigint;autoIncrement:false;not null;uniqueIndex:idx_senses_entry_id_sense_order,priority:1"`
	SenseOrder int   `gorm:"type:smallint;not null;check:sense_order >= 1;uniqueIndex:idx_senses_entry_id_sense_order,priority:2"`

	LearningSignal    *model.SenseLearningSignal    `gorm:"foreignKey:SenseID;references:ID"`
	CEFRSourceSignals []model.SenseCEFRSourceSignal `gorm:"foreignKey:SenseID;references:ID"`
	GlossesEN         []model.SenseGlossEN          `gorm:"foreignKey:SenseID;references:ID"`
	GlossesZH         []model.SenseGlossZH          `gorm:"foreignKey:SenseID;references:ID"`
	Labels            []model.SenseLabel            `gorm:"foreignKey:SenseID;references:ID"`
	Examples          []Example                     `gorm:"foreignKey:SenseID;references:ID"`
	LexicalRelations  []model.LexicalRelation       `gorm:"foreignKey:SenseID;references:ID"`
}

func (Sense) TableName() string {
	return "senses"
}

// Example maps the v1 sense_examples table.
type Example struct {
	ID           int64  `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	SenseID      int64  `gorm:"type:bigint;autoIncrement:false;not null;uniqueIndex:idx_sense_examples_sense_id_source_example_order,priority:1;index:idx_sense_examples_sense_id_example_order,priority:1"`
	Source       string `gorm:"type:text;not null;default:'wiktionary';uniqueIndex:idx_sense_examples_sense_id_source_example_order,priority:2"`
	ExampleOrder int    `gorm:"type:smallint;not null;check:example_order >= 1;uniqueIndex:idx_sense_examples_sense_id_source_example_order,priority:3;index:idx_sense_examples_sense_id_example_order,priority:2"`
	SentenceEN   string `gorm:"column:sentence_en;type:text;not null"`
}

func (Example) TableName() string {
	return "sense_examples"
}

// WordVariant maps the v1 entry_forms table.
type WordVariant struct {
	ID              int64          `gorm:"primaryKey;autoIncrement:false;default:(-);type:bigint"`
	WordID          int64          `gorm:"column:entry_id;type:bigint;autoIncrement:false;not null;index:idx_entry_forms_entry_id_relation_kind,priority:1"`
	FormText        string         `gorm:"column:form_text;type:text;not null"`
	NormalizedForm  string         `gorm:"column:normalized_form;type:text;not null;index:idx_entry_forms_normalized_form"`
	RelationKind    string         `gorm:"column:relation_kind;type:text;not null;check:relation_kind IN ('form','alias');index:idx_entry_forms_entry_id_relation_kind,priority:2"`
	FormType        *string        `gorm:"type:text"`
	SourceRelations pq.StringArray `gorm:"column:source_relations;type:text[];not null;default:'{}'"`
	DisplayOrder    int            `gorm:"type:smallint;not null;default:1"`
}

func (WordVariant) TableName() string {
	return "entry_forms"
}

// Repository provides database access methods.
type Repository struct {
	db *gorm.DB
}

type searchFilters struct {
	pos              *string
	cefrLevel        *int
	oxfordLevel      *int
	cetLevel         *int
	maxFrequencyRank *int
	minCollinsStars  *int
}

type searchResultID struct {
	ID       int64
	Priority int
	FreqRank int64
}

type suggestionResultID struct {
	ID int64
}

type preloadOptions struct {
	variants       bool
	pronunciations bool
	senses         bool
	entryDetails   bool
}

func newPreloadOptions(includeVariants, includePronunciations, includeSenses bool) preloadOptions {
	return preloadOptions{
		variants:       includeVariants,
		pronunciations: includePronunciations,
		senses:         includeSenses,
		entryDetails:   includeVariants || includePronunciations || includeSenses,
	}
}

// NewRepository creates a new repository instance.
func NewRepository(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

func (r *Repository) dbWithContext(ctx context.Context) (*gorm.DB, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("%w: gorm DB is nil", errRepositoryUninitialized)
	}
	return r.db.WithContext(ctx), nil
}

func (r *Repository) applyPreloads(query *gorm.DB, opts preloadOptions) *gorm.DB {
	query = query.
		Preload("LearningSignal").
		Preload("SummariesZH", func(db *gorm.DB) *gorm.DB {
			return db.Order("updated_at DESC, source ASC")
		})

	if opts.entryDetails {
		query = query.
			Preload("SourceRun").
			Preload("CEFRSourceSignals", func(db *gorm.DB) *gorm.DB {
				return db.Order("cefr_source ASC")
			}).
			Preload("Etymology")
	}

	if opts.pronunciations {
		query = query.
			Preload("Pronunciations", func(db *gorm.DB) *gorm.DB {
				return db.Order("accent_code ASC, is_primary DESC, display_order ASC, id ASC")
			}).
			Preload("PronunciationAudios", func(db *gorm.DB) *gorm.DB {
				return db.Order("accent_code ASC, is_primary DESC, display_order ASC, audio_filename ASC, id ASC")
			})
	}
	if opts.senses {
		query = query.
			Preload("Senses.LearningSignal").
			Preload("Senses.CEFRSourceSignals", func(db *gorm.DB) *gorm.DB {
				return db.Order("cefr_source ASC")
			}).
			Preload("Senses.GlossesEN", func(db *gorm.DB) *gorm.DB {
				return db.Order("gloss_order ASC, id ASC")
			}).
			Preload("Senses.GlossesZH", func(db *gorm.DB) *gorm.DB {
				return db.Order("is_primary DESC, gloss_order ASC, source ASC, id ASC")
			}).
			Preload("Senses.Labels", func(db *gorm.DB) *gorm.DB {
				return db.Order("label_order ASC, label_type ASC, label_code ASC, id ASC")
			}).
			Preload("Senses.Examples", func(db *gorm.DB) *gorm.DB {
				return db.Order("example_order ASC, id ASC")
			}).
			Preload("Senses.LexicalRelations", func(db *gorm.DB) *gorm.DB {
				return db.Order("relation_type ASC, display_order ASC, target_text ASC, id ASC")
			}).
			Preload("LexicalRelations", func(db *gorm.DB) *gorm.DB {
				return db.Where("sense_id IS NULL").Order("relation_type ASC, display_order ASC, target_text ASC, id ASC")
			}).
			Preload("Senses", func(db *gorm.DB) *gorm.DB {
				return db.Order("sense_order ASC, id ASC")
			})
	}
	if opts.variants {
		query = query.Preload("WordVariants", func(db *gorm.DB) *gorm.DB {
			return db.Order("relation_kind ASC, display_order ASC, form_text ASC, id ASC")
		})
	}
	return query
}

func entryQualityOrder(entryAlias, learningAlias string) string {
	return fmt.Sprintf(
		"CASE WHEN COALESCE(%[2]s.frequency_rank, 0) = 0 THEN 999999 ELSE %[2]s.frequency_rank END ASC, COALESCE(%[2]s.cefr_level, 0) DESC, %[1]s.id ASC",
		entryAlias,
		learningAlias,
	)
}

// GetWordByHeadword resolves an entry by headword or by matching entry_forms.
func (r *Repository) GetWordByHeadword(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) (*Word, *WordVariant, error) {
	normalizedHeadword := norm.NormalizeHeadword(headword)
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	var word Word
	query := db.Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entries.headword = ?", headword).
		Order(entryQualityOrder("entries", "els"))
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))

	err = query.First(&word).Error
	if err == nil {
		return &word, nil, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, err
	}

	query = db.Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entries.normalized_headword = ?", normalizedHeadword).
		Order("CASE WHEN entries.headword = LOWER(entries.headword) THEN 0 ELSE 1 END ASC").
		Order(entryQualityOrder("entries", "els"))
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))

	err = query.First(&word).Error
	if err == nil {
		return &word, nil, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, err
	}

	var variant WordVariant
	variantRanking := `CASE WHEN entry_forms.form_text = ? THEN 0 ELSE 1 END ASC,
	       entry_forms.relation_kind ASC,
	       CASE WHEN COALESCE(els.frequency_rank, 0) = 0 THEN 999999 ELSE els.frequency_rank END ASC,
	       COALESCE(els.cefr_level, 0) DESC,
	       entry_forms.entry_id ASC`
	err = db.
		Select("entry_forms.*").
		Joins("INNER JOIN entries ON entry_forms.entry_id = entries.id").
		Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entry_forms.normalized_form = ?", normalizedHeadword).
		Order(gorm.Expr(variantRanking, headword)).
		First(&variant).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil, ErrWordNotFound
		}
		return nil, nil, err
	}

	loaded, err := r.loadWordsByIDs(db, []int64{variant.WordID}, includeVariants, includePronunciations, includeSenses)
	if err != nil {
		return nil, nil, err
	}
	if len(loaded) == 0 {
		return nil, nil, ErrWordNotFound
	}

	return &loaded[0], &variant, nil
}

// GetWordsByVariant finds entries by form/alias text using normalized matching.
func (r *Repository) GetWordsByVariant(ctx context.Context, variant string, kind *string, includePronunciations, includeSenses bool) ([]Word, []WordVariant, error) {
	var variants []WordVariant
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	variantNormalized := norm.NormalizeHeadword(variant)
	query := db.Where("normalized_form = ?", variantNormalized)
	if kind != nil {
		query = query.Where("relation_kind = ?", *kind)
	}
	query = query.Order("relation_kind ASC, COALESCE(form_type, '') ASC, form_text ASC, id ASC")

	if err := query.Find(&variants).Error; err != nil {
		return nil, nil, err
	}
	if len(variants) == 0 {
		return nil, nil, ErrVariantNotFound
	}

	idSet := make(map[int64]struct{}, len(variants))
	wordIDs := make([]int64, 0, len(variants))
	for _, v := range variants {
		if _, exists := idSet[v.WordID]; exists {
			continue
		}
		idSet[v.WordID] = struct{}{}
		wordIDs = append(wordIDs, v.WordID)
	}

	words, err := r.loadWordsByIDs(db, wordIDs, false, includePronunciations, includeSenses)
	if err != nil {
		return nil, nil, err
	}

	return words, variants, nil
}

// GetWordsByVariants resolves multiple form/alias headwords in one query.
func (r *Repository) GetWordsByVariants(ctx context.Context, variants []string, includeVariants, includePronunciations, includeSenses bool) ([]BatchVariantMatch, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if len(variants) == 0 {
		return []BatchVariantMatch{}, nil
	}

	normalizedInputs, normalizedForms := normalizeUniqueInputs(variants)
	if len(normalizedForms) == 0 {
		return []BatchVariantMatch{}, nil
	}

	var rankedVariants []WordVariant
	err = db.
		Select("entry_forms.*").
		Joins("INNER JOIN entries ON entry_forms.entry_id = entries.id").
		Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entry_forms.normalized_form IN ?", normalizedForms).
		Order(`entry_forms.normalized_form ASC,
		       entry_forms.relation_kind ASC,
		       CASE WHEN COALESCE(els.frequency_rank, 0) = 0 THEN 999999 ELSE els.frequency_rank END ASC,
		       COALESCE(els.cefr_level, 0) DESC,
		       entry_forms.entry_id ASC`).
		Find(&rankedVariants).Error
	if err != nil {
		return nil, err
	}
	if len(rankedVariants) == 0 {
		return []BatchVariantMatch{}, nil
	}

	variantsByNormalized, wordIDs := groupVariantsByNormalized(rankedVariants, len(normalizedForms))
	words, err := r.loadWordsByIDs(db, wordIDs, includeVariants, includePronunciations, includeSenses)
	if err != nil {
		return nil, err
	}

	return buildBatchVariantMatches(variants, normalizedInputs, variantsByNormalized, indexWordsByID(words)), nil
}

// GetWordsByHeadwords retrieves multiple entries by normalized headword.
func (r *Repository) GetWordsByHeadwords(ctx context.Context, headwords []string, includeVariants, includePronunciations, includeSenses bool) ([]Word, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if len(headwords) == 0 {
		return []Word{}, nil
	}

	normalizedForms := make([]string, len(headwords))
	for i, hw := range headwords {
		normalizedForms[i] = norm.NormalizeHeadword(hw)
	}

	var words []Word
	query := db.Where("normalized_headword IN ?", normalizedForms)
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))
	if err := query.Find(&words).Error; err != nil {
		return nil, err
	}

	return words, nil
}

func (r *Repository) loadWordsByIDs(db *gorm.DB, wordIDs []int64, includeVariants, includePronunciations, includeSenses bool) ([]Word, error) {
	if len(wordIDs) == 0 {
		return []Word{}, nil
	}

	var words []Word
	query := db.Where("id IN ?", wordIDs)
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))
	if err := query.Find(&words).Error; err != nil {
		return nil, err
	}

	wordByID := make(map[int64]*Word, len(words))
	for i := range words {
		wordByID[words[i].ID] = &words[i]
	}

	ordered := make([]Word, 0, len(wordIDs))
	for _, id := range wordIDs {
		if word, ok := wordByID[id]; ok {
			ordered = append(ordered, *word)
		}
	}

	return ordered, nil
}

// ListFeaturedCandidateHeadwords returns headwords with learning signals for featured recommendations.
func (r *Repository) ListFeaturedCandidateHeadwords(ctx context.Context) ([]string, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}

	headwords := make([]string, 0, 70000)
	if err := db.
		Table("featured_candidates").
		Pluck("headword", &headwords).Error; err != nil {
		return nil, err
	}

	return headwords, nil
}

// escapeLikePattern escapes SQL LIKE wildcard characters to prevent user input
// from being interpreted as wildcards.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

func newSearchFilters(pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int) searchFilters {
	return searchFilters{
		pos:              pos,
		cefrLevel:        cefrLevel,
		oxfordLevel:      oxfordLevel,
		cetLevel:         cetLevel,
		maxFrequencyRank: maxFrequencyRank,
		minCollinsStars:  minCollinsStars,
	}
}

func (filters searchFilters) searchTermFuzzyClauses(alias string) []string {
	return filters.appendSearchTermFilterClauses([]string{fmt.Sprintf("%s.normalized_term LIKE ?", alias)}, alias)
}

func (filters searchFilters) searchTermPrefixRangeClauses(alias, prefix, lower, upper string) ([]string, []interface{}) {
	if lower != "" && upper != "" {
		clauses := []string{fmt.Sprintf("%[1]s.normalized_term >= ? AND %[1]s.normalized_term < ?", alias)}
		clauses = filters.appendSearchTermFilterClauses(clauses, alias)
		return clauses, filters.argsWithPrefix(lower, upper)
	}

	pattern := escapeLikePattern(prefix) + "%"
	return filters.searchTermFuzzyClauses(alias), filters.args(pattern)
}

func (filters searchFilters) matchesNothing() bool {
	return filters.maxFrequencyRank != nil && *filters.maxFrequencyRank <= 0
}

func positiveFilter(value *int) bool {
	return value != nil && *value > 0
}

func (filters searchFilters) appendSearchTermFilterClauses(clauses []string, alias string) []string {
	if filters.cefrLevel != nil {
		clauses = append(clauses, fmt.Sprintf("%s.cefr_level = ?", alias))
	}
	if filters.oxfordLevel != nil {
		clauses = append(clauses, fmt.Sprintf("%s.oxford_level = ?", alias))
	}
	if filters.cetLevel != nil {
		clauses = append(clauses, fmt.Sprintf("%s.cet_level = ?", alias))
	}
	if positiveFilter(filters.maxFrequencyRank) {
		clauses = append(clauses, fmt.Sprintf("%s.frequency_rank > 0 AND %s.frequency_rank <= ?", alias, alias))
	}
	if positiveFilter(filters.minCollinsStars) {
		clauses = append(clauses, fmt.Sprintf("%s.collins_stars >= ?", alias))
	}
	if filters.pos != nil {
		clauses = append(clauses, fmt.Sprintf("%s.pos = ?", alias))
	}
	return clauses
}

func (filters searchFilters) argsWithPrefix(lower, upper string) []interface{} {
	args := []interface{}{lower, upper}
	return filters.appendFilterArgs(args)
}

func (filters searchFilters) args(pattern string) []interface{} {
	args := []interface{}{pattern}
	return filters.appendFilterArgs(args)
}

func (filters searchFilters) appendFilterArgs(args []interface{}) []interface{} {
	if filters.cefrLevel != nil {
		args = append(args, *filters.cefrLevel)
	}
	if filters.oxfordLevel != nil {
		args = append(args, *filters.oxfordLevel)
	}
	if filters.cetLevel != nil {
		args = append(args, *filters.cetLevel)
	}
	if positiveFilter(filters.maxFrequencyRank) {
		args = append(args, *filters.maxFrequencyRank)
	}
	if positiveFilter(filters.minCollinsStars) {
		args = append(args, *filters.minCollinsStars)
	}
	if filters.pos != nil {
		args = append(args, *filters.pos)
	}
	return args
}

func normalizedPrefixRange(prefix string) (string, string) {
	if prefix == "" {
		return "", ""
	}
	for _, r := range prefix {
		if r > 0x7f {
			return "", ""
		}
	}
	bytes := []byte(prefix)
	for i := len(bytes) - 1; i >= 0; i-- {
		if bytes[i] < 0x7f {
			upper := append([]byte(nil), bytes[:i+1]...)
			upper[i]++
			return prefix, string(upper)
		}
	}
	return "", ""
}

func appendArgSets(argSets ...[]interface{}) []interface{} {
	combined := make([]interface{}, 0)
	for _, argSet := range argSets {
		combined = append(combined, argSet...)
	}
	return combined
}

func normalizeUniqueInputs(inputs []string) ([]string, []string) {
	normalizedInputs := make([]string, len(inputs))
	normalizedForms := make([]string, 0, len(inputs))
	seenNormalized := make(map[string]struct{}, len(inputs))
	for i, input := range inputs {
		normalized := norm.NormalizeHeadword(input)
		normalizedInputs[i] = normalized
		if normalized == "" {
			continue
		}
		if _, exists := seenNormalized[normalized]; exists {
			continue
		}
		seenNormalized[normalized] = struct{}{}
		normalizedForms = append(normalizedForms, normalized)
	}
	return normalizedInputs, normalizedForms
}

func groupVariantsByNormalized(rankedVariants []WordVariant, capacity int) (map[string][]WordVariant, []int64) {
	variantsByNormalized := make(map[string][]WordVariant, capacity)
	selectedWordIDs := make(map[int64]struct{}, len(rankedVariants))
	wordIDs := make([]int64, 0, len(rankedVariants))
	for _, variant := range rankedVariants {
		variantsByNormalized[variant.NormalizedForm] = append(variantsByNormalized[variant.NormalizedForm], variant)
		if _, exists := selectedWordIDs[variant.WordID]; exists {
			continue
		}
		selectedWordIDs[variant.WordID] = struct{}{}
		wordIDs = append(wordIDs, variant.WordID)
	}
	return variantsByNormalized, wordIDs
}

func indexWordsByID(words []Word) map[int64]Word {
	indexed := make(map[int64]Word, len(words))
	for _, word := range words {
		indexed[word.ID] = word
	}
	return indexed
}

func selectVariantCandidate(candidates []WordVariant, input string) (WordVariant, bool) {
	if len(candidates) == 0 {
		return WordVariant{}, false
	}
	for _, candidate := range candidates {
		if candidate.FormText == input {
			return candidate, true
		}
	}
	return candidates[0], true
}

func buildBatchVariantMatches(inputs []string, normalizedInputs []string, variantsByNormalized map[string][]WordVariant, wordsByID map[int64]Word) []BatchVariantMatch {
	matches := make([]BatchVariantMatch, 0, len(inputs))
	for i, input := range inputs {
		normalized := normalizedInputs[i]
		if normalized == "" {
			continue
		}
		selected, ok := selectVariantCandidate(variantsByNormalized[normalized], input)
		if !ok {
			continue
		}
		word, exists := wordsByID[selected.WordID]
		if !exists {
			continue
		}
		matches = append(matches, BatchVariantMatch{Word: word, Variant: selected})
	}
	return matches
}

// SearchWords performs fuzzy search through the entry_search_terms read model.
func (r *Repository) SearchWords(ctx context.Context, keyword string, pos *string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit, offset int) ([]Word, int64, error) {
	normalizedKeyword := norm.NormalizeHeadword(keyword)
	escaped := escapeLikePattern(normalizedKeyword)
	prefix := escaped + "%"
	fuzzy := "%" + escaped + "%"
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	filters := newSearchFilters(pos, cefrLevel, oxfordLevel, cetLevel, maxFrequencyRank, minCollinsStars)
	if filters.matchesNothing() {
		return []Word{}, 0, nil
	}
	fuzzyFilters := filters.searchTermFuzzyClauses("t")

	countSQL := fmt.Sprintf(`
		SELECT COUNT(DISTINCT t.entry_id)
		FROM entry_search_terms t
		WHERE %s
	`,
		strings.Join(fuzzyFilters, " AND "),
	)

	fuzzyArgs := filters.args(fuzzy)

	var total int64
	if err := db.Raw(countSQL, fuzzyArgs...).Scan(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []Word{}, 0, nil
	}

	results, err := querySearchResultIDs(db, prefix, fuzzyFilters, fuzzyArgs, limit, offset, true)
	if err != nil {
		return nil, 0, err
	}
	if len(results) < limit {
		results, err = querySearchResultIDs(db, prefix, fuzzyFilters, fuzzyArgs, limit, offset, false)
		if err != nil {
			return nil, 0, err
		}
	}
	if len(results) == 0 {
		return []Word{}, total, nil
	}

	wordIDs := make([]int64, len(results))
	for i, result := range results {
		wordIDs[i] = result.ID
	}

	words, err := r.loadWordsByIDs(db, wordIDs, false, false, false)
	if err != nil {
		return nil, 0, err
	}

	return words, total, nil
}

func querySearchResultIDs(db *gorm.DB, prefix string, fuzzyFilters []string, fuzzyArgs []interface{}, limit, offset int, positiveFrequencyOnly bool) ([]searchResultID, error) {
	clauses := append([]string(nil), fuzzyFilters...)
	if positiveFrequencyOnly {
		clauses = append(clauses, "t.frequency_rank > 0")
	}

	pageSQL := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				t.entry_id AS id,
				CASE WHEN t.normalized_term LIKE ? THEN t.term_rank ELSE t.term_rank + 2 END AS priority,
				CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END AS freq_rank,
				t.headword,
				ROW_NUMBER() OVER (
					PARTITION BY t.entry_id
					ORDER BY
						CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END ASC,
						CASE WHEN t.normalized_term LIKE ? THEN t.term_rank ELSE t.term_rank + 2 END ASC,
						t.headword ASC
				) AS rn
			FROM entry_search_terms t
			WHERE %s
		)
		SELECT id, priority, freq_rank
		FROM ranked
		WHERE rn = 1
		ORDER BY freq_rank ASC, priority ASC, headword ASC
			LIMIT ? OFFSET ?
		`,
		strings.Join(clauses, " AND "),
	)

	pageArgs := append(appendArgSets([]interface{}{prefix, prefix}, fuzzyArgs), limit, offset)
	var results []searchResultID
	if err := db.Raw(pageSQL, pageArgs...).Scan(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

// SuggestWords provides autocomplete suggestions through the entry_search_terms read model.
func (r *Repository) SuggestWords(ctx context.Context, prefix string, cefrLevel *int, oxfordLevel *int, cetLevel *int, maxFrequencyRank *int, minCollinsStars *int, limit int) ([]Word, error) {
	normalizedPrefix := norm.NormalizeHeadword(prefix)
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	filters := newSearchFilters(nil, cefrLevel, oxfordLevel, cetLevel, maxFrequencyRank, minCollinsStars)
	if filters.matchesNothing() {
		return []Word{}, nil
	}

	results, err := querySuggestionIDs(db, normalizedPrefix, filters, limit, true)
	if err != nil {
		return nil, err
	}
	if len(results) < limit {
		results, err = querySuggestionIDs(db, normalizedPrefix, filters, limit, false)
	}
	if err != nil {
		return nil, err
	}
	return r.loadSuggestionResults(db, results)
}

func querySuggestionIDs(db *gorm.DB, normalizedPrefix string, filters searchFilters, limit int, positiveFrequencyOnly bool) ([]suggestionResultID, error) {
	prefixLower, prefixUpper := normalizedPrefixRange(normalizedPrefix)
	clauses, args := filters.searchTermPrefixRangeClauses("t", normalizedPrefix, prefixLower, prefixUpper)
	if positiveFrequencyOnly {
		clauses = append(clauses, "t.frequency_rank > 0")
	}

	unionSQL := fmt.Sprintf(`
		WITH params AS (
			SELECT ?::text AS normalized_prefix
		),
		entry_ranked AS (
			SELECT
				t.entry_id AS id,
				t.headword,
				LOWER(TRIM(t.headword)) AS headword_key,
				CASE
					WHEN t.term_kind = 'headword' THEN 0
					WHEN t.term_kind = 'alias' AND t.normalized_term = p.normalized_prefix THEN 1
					WHEN t.term_kind = 'alias' THEN 2
					WHEN t.term_kind = 'form' AND t.normalized_term = p.normalized_prefix THEN 3
					WHEN t.term_kind = 'form' THEN 4
					ELSE 5
				END AS match_priority,
				CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END AS frequency_rank,
				t.term_rank,
				ROW_NUMBER() OVER (
					PARTITION BY t.entry_id
					ORDER BY
						CASE
							WHEN t.term_kind = 'headword' THEN 0
							WHEN t.term_kind = 'alias' AND t.normalized_term = p.normalized_prefix THEN 1
							WHEN t.term_kind = 'alias' THEN 2
							WHEN t.term_kind = 'form' AND t.normalized_term = p.normalized_prefix THEN 3
							WHEN t.term_kind = 'form' THEN 4
							ELSE 5
						END ASC,
						CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END ASC,
						t.entry_id ASC,
						t.term_rank ASC,
						t.headword ASC
				) AS entry_rn
			FROM entry_search_terms t
			CROSS JOIN params p
			WHERE %s
		),
		headword_ranked AS (
			SELECT
				id,
				headword,
				headword_key,
				match_priority,
				frequency_rank,
				term_rank,
				ROW_NUMBER() OVER (
					PARTITION BY headword_key
					ORDER BY
						match_priority ASC,
						frequency_rank ASC,
						id ASC,
						term_rank ASC
				) AS headword_rn
			FROM entry_ranked
			WHERE entry_rn = 1
		)
		SELECT id, frequency_rank
		FROM headword_ranked
		WHERE headword_rn = 1
		ORDER BY match_priority ASC, frequency_rank ASC, id ASC, term_rank ASC, headword ASC
		LIMIT ?
		`, strings.Join(clauses, " AND "))

	args = append([]interface{}{normalizedPrefix}, args...)
	args = append(args, limit)
	var results []suggestionResultID
	if err := db.Raw(unionSQL, args...).Scan(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func (r *Repository) loadSuggestionResults(db *gorm.DB, results []suggestionResultID) ([]Word, error) {
	if len(results) == 0 {
		return []Word{}, nil
	}
	wordIDs := make([]int64, len(results))
	for i, result := range results {
		wordIDs[i] = result.ID
	}
	return r.loadWordsByIDs(db, wordIDs, false, false, false)
}

// SearchPhrases searches for multiword entries containing the keyword.
func (r *Repository) SearchPhrases(ctx context.Context, keyword string, limit int) ([]Word, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	if limit <= 0 {
		limit = 20
	}

	lowerKeyword := strings.ToLower(strings.TrimSpace(keyword))
	escaped := escapeLikePattern(lowerKeyword)
	normalizedFuzzy := "%" + escapeLikePattern(norm.NormalizeHeadword(keyword)) + "%"
	hasSpace := strings.Contains(lowerKeyword, " ")

	patternStart := escaped + " %"
	patternMiddle := "% " + escaped + " %"
	patternEnd := "% " + escaped
	var patternPrefix string
	if hasSpace {
		patternPrefix = escaped + "%"
	} else {
		patternPrefix = escaped + " %"
	}

	unionSQL := `
	WITH ranked AS (
		SELECT
			t.entry_id AS id,
			t.entry_id AS id_ord,
			CASE
				WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank
				WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank
				WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank + 2
				WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank + 4
				ELSE t.term_rank + 6
			END AS priority,
			CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END AS freq_rank,
			t.headword,
			ROW_NUMBER() OVER (
				PARTITION BY t.entry_id
				ORDER BY
					CASE WHEN t.frequency_rank = 0 THEN 999999 ELSE t.frequency_rank END,
					CASE
						WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank
						WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank
						WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank + 2
						WHEN LOWER(t.term_text) LIKE ? THEN t.term_rank + 4
						ELSE t.term_rank + 6
					END,
					t.entry_id,
					t.headword
			) AS rn
		FROM entry_search_terms t
		WHERE t.is_multiword = true
			AND t.normalized_term LIKE ?
			AND (
				LOWER(t.term_text) LIKE ?
				OR LOWER(t.term_text) LIKE ?
				OR LOWER(t.term_text) LIKE ?
				OR LOWER(t.term_text) LIKE ?
			)
	)
	SELECT id, priority, freq_rank, id_ord, headword
	FROM ranked
	WHERE rn = 1
	ORDER BY freq_rank, priority, id_ord, headword
	LIMIT ?
	`

	args := []interface{}{
		patternStart, patternPrefix, patternMiddle, patternEnd,
		patternStart, patternPrefix, patternMiddle, patternEnd,
		normalizedFuzzy,
		patternStart, patternPrefix, patternMiddle, patternEnd,
		limit,
	}

	type phraseRow struct {
		ID       int64
		Priority int
		FreqRank int
		IDOrd    int64
		Headword string
	}

	var rankedRows []phraseRow
	if err := db.Raw(unionSQL, args...).Scan(&rankedRows).Error; err != nil {
		return nil, err
	}
	if len(rankedRows) == 0 {
		return []Word{}, nil
	}

	wordIDs := make([]int64, 0, len(rankedRows))
	for _, row := range rankedRows {
		wordIDs = append(wordIDs, row.ID)
	}

	return r.loadWordsByIDs(db, wordIDs, false, false, false)
}

// GetPronunciationsByWordID retrieves pronunciations for an entry.
func (r *Repository) GetPronunciationsByWordID(ctx context.Context, wordID int64, accent *string) ([]Pronunciation, error) {
	var pronunciations []Pronunciation
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	query := db.Where("entry_id = ?", wordID)
	if accent != nil {
		query = query.Where("accent_code = ?", *accent)
	}
	if err := query.Order("accent_code ASC, is_primary DESC, display_order ASC, id ASC").Find(&pronunciations).Error; err != nil {
		return nil, err
	}

	return pronunciations, nil
}

// GetSensesByWordID retrieves senses for an entry.
func (r *Repository) GetSensesByWordID(ctx context.Context, wordID int64, pos *string) ([]Sense, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}

	var entry Word
	if err := db.Select("id", "pos").First(&entry, "id = ?", wordID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return []Sense{}, nil
		}
		return nil, err
	}
	if pos != nil && entry.Pos != *pos {
		return []Sense{}, nil
	}

	var senses []Sense
	if err := db.Where("entry_id = ?", wordID).
		Preload("LearningSignal").
		Preload("CEFRSourceSignals", func(db *gorm.DB) *gorm.DB {
			return db.Order("cefr_source ASC")
		}).
		Preload("GlossesEN", func(db *gorm.DB) *gorm.DB {
			return db.Order("gloss_order ASC, id ASC")
		}).
		Preload("GlossesZH", func(db *gorm.DB) *gorm.DB {
			return db.Order("is_primary DESC, gloss_order ASC, source ASC, id ASC")
		}).
		Preload("Labels", func(db *gorm.DB) *gorm.DB {
			return db.Order("label_order ASC, label_type ASC, label_code ASC, id ASC")
		}).
		Preload("Examples", func(db *gorm.DB) *gorm.DB {
			return db.Order("example_order ASC, id ASC")
		}).
		Preload("LexicalRelations", func(db *gorm.DB) *gorm.DB {
			return db.Order("relation_type ASC, display_order ASC, target_text ASC, id ASC")
		}).
		Order("sense_order ASC, id ASC").
		Find(&senses).Error; err != nil {
		return nil, err
	}
	return senses, nil
}
