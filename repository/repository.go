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
	EntryDefinitions    []model.EntryDefinition       `gorm:"foreignKey:EntryID;references:ID"`
	EntryExamples       []model.EntryExample          `gorm:"foreignKey:EntryID;references:ID"`
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
	schoolLevel      *int
	maxFrequencyRank *int
	minCollinsStars  *int
}

// SearchOptions names the optional filters and paging controls for word search.
type SearchOptions struct {
	POS              *string
	CEFRLevel        *int
	OxfordLevel      *int
	CETLevel         *int
	SchoolLevel      *int
	MaxFrequencyRank *int
	MinCollinsStars  *int
	Limit            int
	Offset           int
}

// SuggestOptions names the optional filters and limit control for suggestions.
type SuggestOptions struct {
	CEFRLevel        *int
	OxfordLevel      *int
	CETLevel         *int
	SchoolLevel      *int
	MaxFrequencyRank *int
	MinCollinsStars  *int
	Limit            int
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

type learnerSignalColumns struct {
	frequencyRank string
	schoolLevel   string
	cefrLevel     string
	oxfordLevel   string
	cetLevel      string
	collinsStars  string
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
			Preload("Etymology").
			Preload("EntryDefinitions", func(db *gorm.DB) *gorm.DB {
				return db.Order("definition_order ASC, source ASC, id ASC")
			}).
			Preload("EntryExamples", func(db *gorm.DB) *gorm.DB {
				return db.Order("example_order ASC, source ASC, id ASC")
			})
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

func aliasedLearnerSignalColumns(alias string) learnerSignalColumns {
	return learnerSignalColumns{
		frequencyRank: alias + ".frequency_rank",
		schoolLevel:   alias + ".school_level",
		cefrLevel:     alias + ".cefr_level",
		oxfordLevel:   alias + ".oxford_level",
		cetLevel:      alias + ".cet_level",
		collinsStars:  alias + ".collins_stars",
	}
}

func learnerSignalOrder(alias string) string {
	return learnerSignalOrderForColumns(aliasedLearnerSignalColumns(alias))
}

func learnerSignalOrderForColumns(columns learnerSignalColumns) string {
	return fmt.Sprintf(
		`CASE WHEN COALESCE(%[1]s, 0) = 0 THEN 999999999 ELSE %[1]s END ASC,
		CASE WHEN COALESCE(%[2]s, 0) = 0 THEN 999999999 ELSE %[2]s END ASC,
		CASE WHEN COALESCE(%[3]s, 0) > 0 THEN 0 ELSE 1 END ASC,
		CASE WHEN COALESCE(%[4]s, 0) > 0 THEN 0 ELSE 1 END ASC,
		CASE WHEN COALESCE(%[5]s, 0) > 0 THEN 0 ELSE 1 END ASC,
		CASE WHEN COALESCE(%[6]s, 0) > 0 THEN 0 ELSE 1 END ASC,
		COALESCE(%[3]s, 0) DESC,
		COALESCE(%[4]s, 0) DESC,
		COALESCE(%[5]s, 0) DESC,
		COALESCE(%[6]s, 0) DESC`,
		columns.frequencyRank,
		columns.schoolLevel,
		columns.cefrLevel,
		columns.oxfordLevel,
		columns.cetLevel,
		columns.collinsStars,
	)
}

func rankedLearnerSignalColumns() learnerSignalColumns {
	return learnerSignalColumns{
		frequencyRank: "freq_rank",
		schoolLevel:   "school_rank",
		cefrLevel:     "cefr_level",
		oxfordLevel:   "oxford_level",
		cetLevel:      "cet_level",
		collinsStars:  "collins_stars",
	}
}

func entryQualityOrder(entryAlias, learningAlias string) string {
	return fmt.Sprintf(
		`%s,
		%s.etymology_index ASC,
		%s.id ASC`,
		learnerSignalOrder(learningAlias),
		entryAlias,
		entryAlias,
	)
}

func entryGroupOrder(entryAlias, learningAlias string) string {
	return fmt.Sprintf(
		`CASE WHEN %[1]s.headword = LOWER(%[1]s.headword) THEN 0 ELSE 1 END ASC,
		CASE %[1]s.pos
			WHEN 'noun' THEN 1
			WHEN 'verb' THEN 2
			WHEN 'adjective' THEN 3
			WHEN 'adverb' THEN 4
			ELSE 9
		END ASC,
		%s`,
		entryAlias,
		entryQualityOrder(entryAlias, learningAlias),
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
	       ` + learnerSignalOrder("els") + `,
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

// GetEntryGroupByHeadword resolves all entries under the same normalized
// headword. If the input is an entry form or alias, the group for its canonical
// entry headword is returned and the selected form is exposed as queriedVariant.
func (r *Repository) GetEntryGroupByHeadword(ctx context.Context, headword string, includeVariants, includePronunciations, includeSenses bool) ([]Word, *WordVariant, error) {
	normalizedHeadword := norm.NormalizeHeadword(headword)
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	words, err := r.loadWordsByNormalizedHeadword(db, normalizedHeadword, includeVariants, includePronunciations, includeSenses)
	if err != nil {
		return nil, nil, err
	}
	if len(words) > 0 {
		return words, nil, nil
	}

	variant, err := r.findBestVariant(db, normalizedHeadword, headword)
	if err != nil {
		return nil, nil, err
	}

	selected, err := r.loadWordsByIDs(db, []int64{variant.WordID}, false, false, false)
	if err != nil {
		return nil, nil, err
	}
	if len(selected) == 0 || selected[0].NormalizedHeadword == "" {
		return nil, nil, ErrWordNotFound
	}

	words, err = r.loadWordsByNormalizedHeadword(db, selected[0].NormalizedHeadword, includeVariants, includePronunciations, includeSenses)
	if err != nil {
		return nil, nil, err
	}
	if len(words) == 0 {
		return nil, nil, ErrWordNotFound
	}

	return words, &variant, nil
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
		       ` + learnerSignalOrder("els") + `,
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

	_, uniqueNormalizedForms := normalizeUniqueInputs(headwords)
	if len(uniqueNormalizedForms) == 0 {
		return []Word{}, nil
	}

	var words []Word
	query := db.Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entries.normalized_headword IN ?", uniqueNormalizedForms).
		Order("entries.normalized_headword ASC").
		Order(entryQualityOrder("entries", "els"))
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))
	if err := query.Find(&words).Error; err != nil {
		return nil, err
	}

	return words, nil
}

func (r *Repository) loadWordsByNormalizedHeadword(db *gorm.DB, normalizedHeadword string, includeVariants, includePronunciations, includeSenses bool) ([]Word, error) {
	if strings.TrimSpace(normalizedHeadword) == "" {
		return []Word{}, nil
	}

	var words []Word
	query := db.Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entries.normalized_headword = ?", normalizedHeadword).
		Order(entryGroupOrder("entries", "els"))
	query = r.applyPreloads(query, newPreloadOptions(includeVariants, includePronunciations, includeSenses))
	if err := query.Find(&words).Error; err != nil {
		return nil, err
	}
	return words, nil
}

func (r *Repository) findBestVariant(db *gorm.DB, normalizedForm, originalForm string) (WordVariant, error) {
	var variant WordVariant
	variantRanking := `CASE WHEN entry_forms.form_text = ? THEN 0 ELSE 1 END ASC,
	       entry_forms.relation_kind ASC,
	       ` + learnerSignalOrder("els") + `,
	       entry_forms.entry_id ASC`
	err := db.
		Select("entry_forms.*").
		Joins("INNER JOIN entries ON entry_forms.entry_id = entries.id").
		Joins("LEFT JOIN entry_learning_signals els ON els.entry_id = entries.id").
		Where("entry_forms.normalized_form = ?", normalizedForm).
		Order(gorm.Expr(variantRanking, originalForm)).
		First(&variant).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return WordVariant{}, ErrWordNotFound
		}
		return WordVariant{}, err
	}
	return variant, nil
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

// ListFeaturedCandidates returns upstream canonical featured_candidates in
// quality-rank order. Candidate eligibility, per-headword selection, and
// ranking are owned by the commons read model.
func (r *Repository) ListFeaturedCandidates(ctx context.Context) ([]FeaturedCandidate, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}

	candidates := make([]FeaturedCandidate, 0, 70000)
	if err := db.
		Table("featured_candidates").
		Select("entry_id, headword").
		Order("quality_rank ASC, entry_id ASC").
		Scan(&candidates).Error; err != nil {
		return nil, err
	}

	return candidates, nil
}

// escapeLikePattern escapes SQL LIKE wildcard characters to prevent user input
// from being interpreted as wildcards.
func escapeLikePattern(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, "%", "\\%")
	s = strings.ReplaceAll(s, "_", "\\_")
	return s
}

func newSearchFilters(opts SearchOptions) searchFilters {
	return searchFilters{
		pos:              opts.POS,
		cefrLevel:        opts.CEFRLevel,
		oxfordLevel:      opts.OxfordLevel,
		cetLevel:         opts.CETLevel,
		schoolLevel:      opts.SchoolLevel,
		maxFrequencyRank: opts.MaxFrequencyRank,
		minCollinsStars:  opts.MinCollinsStars,
	}
}

func searchOptionsFromSuggestOptions(opts SuggestOptions) SearchOptions {
	return SearchOptions{
		CEFRLevel:        opts.CEFRLevel,
		OxfordLevel:      opts.OxfordLevel,
		CETLevel:         opts.CETLevel,
		SchoolLevel:      opts.SchoolLevel,
		MaxFrequencyRank: opts.MaxFrequencyRank,
		MinCollinsStars:  opts.MinCollinsStars,
		Limit:            opts.Limit,
	}
}

func normalizeSearchOptions(opts SearchOptions) SearchOptions {
	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	return opts
}

func normalizeSuggestOptions(opts SuggestOptions) SuggestOptions {
	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	return opts
}

func ValidateSearchOptions(opts SearchOptions) error {
	if opts.POS != nil {
		if _, ok := model.ValidPOSCodes()[*opts.POS]; !ok {
			return fmt.Errorf("%w: pos must be a known POS code", ErrInvalidSearchFilter)
		}
	}
	if err := validateInt16CodeFilter("cefr_level", opts.CEFRLevel, model.CEFRLevelUnknown, model.CEFRLevelC2); err != nil {
		return err
	}
	if err := validateInt16CodeFilter("oxford_level", opts.OxfordLevel, model.OxfordLevelUnknown, model.OxfordLevel5000); err != nil {
		return err
	}
	if err := validateInt16CodeFilter("cet_level", opts.CETLevel, model.CETLevelUnknown, model.CETLevel6); err != nil {
		return err
	}
	if err := validateInt16CodeFilter("school_level", opts.SchoolLevel, model.SchoolLevelUnknown, model.SchoolLevelUniversity); err != nil {
		return err
	}
	if opts.MaxFrequencyRank != nil && *opts.MaxFrequencyRank <= 0 {
		return fmt.Errorf("%w: max_frequency_rank must be greater than 0", ErrInvalidSearchFilter)
	}
	if err := validateInt16CodeFilter("min_collins_stars", opts.MinCollinsStars, model.CollinsStarsUnknown, model.CollinsFiveStars); err != nil {
		return err
	}
	return nil
}

func ValidateSuggestOptions(opts SuggestOptions) error {
	return ValidateSearchOptions(searchOptionsFromSuggestOptions(opts))
}

func validateInt16CodeFilter(name string, value *int, minValue, maxValue int16) error {
	if value == nil {
		return nil
	}
	if *value < int(minValue) || *value > int(maxValue) {
		return fmt.Errorf("%w: %s must be between %d and %d", ErrInvalidSearchFilter, name, minValue, maxValue)
	}
	return nil
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
	if filters.schoolLevel != nil {
		clauses = append(clauses, fmt.Sprintf("%s.school_level = ?", alias))
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
	if filters.schoolLevel != nil {
		args = append(args, *filters.schoolLevel)
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
func (r *Repository) SearchWords(ctx context.Context, keyword string, opts SearchOptions) ([]Word, int64, error) {
	normalizedKeyword := norm.NormalizeHeadword(keyword)
	escaped := escapeLikePattern(normalizedKeyword)
	prefix := escaped + "%"
	fuzzy := "%" + escaped + "%"
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	opts = normalizeSearchOptions(opts)
	if err := ValidateSearchOptions(opts); err != nil {
		return nil, 0, err
	}
	filters := newSearchFilters(opts)
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

	results, err := querySearchResultIDs(db, prefix, fuzzyFilters, fuzzyArgs, opts.Limit, opts.Offset)
	if err != nil {
		return nil, 0, err
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

func querySearchResultIDs(db *gorm.DB, prefix string, fuzzyFilters []string, fuzzyArgs []interface{}, limit, offset int) ([]searchResultID, error) {
	clauses := append([]string(nil), fuzzyFilters...)

	pageSQL := fmt.Sprintf(`
		WITH ranked AS (
			SELECT
				t.entry_id AS id,
				CASE WHEN t.normalized_term LIKE ? THEN t.term_rank ELSE t.term_rank + 2 END AS priority,
				CASE WHEN t.frequency_rank = 0 THEN 999999999 ELSE t.frequency_rank END AS freq_rank,
				CASE WHEN t.school_level = 0 THEN 999999999 ELSE t.school_level END AS school_rank,
				t.cefr_level,
				t.oxford_level,
				t.cet_level,
				t.collins_stars,
				t.headword,
				ROW_NUMBER() OVER (
					PARTITION BY t.entry_id
					ORDER BY
						%s,
						CASE WHEN t.normalized_term LIKE ? THEN t.term_rank ELSE t.term_rank + 2 END ASC,
						t.headword ASC
				) AS rn
			FROM entry_search_terms t
			WHERE %s
		)
		SELECT id, priority, freq_rank, school_rank, cefr_level, oxford_level, cet_level, collins_stars
		FROM ranked
		WHERE rn = 1
		ORDER BY
			%s,
			priority ASC,
			headword ASC,
			id ASC
			LIMIT ? OFFSET ?
		`,
		learnerSignalOrder("t"),
		strings.Join(clauses, " AND "),
		learnerSignalOrderForColumns(rankedLearnerSignalColumns()),
	)

	pageArgs := append(appendArgSets([]interface{}{prefix, prefix}, fuzzyArgs), limit, offset)
	var results []searchResultID
	if err := db.Raw(pageSQL, pageArgs...).Scan(&results).Error; err != nil {
		return nil, err
	}
	return results, nil
}

// SuggestWords provides autocomplete suggestions through the entry_search_terms read model.
func (r *Repository) SuggestWords(ctx context.Context, prefix string, opts SuggestOptions) ([]Word, error) {
	normalizedPrefix := norm.NormalizeHeadword(prefix)
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	opts = normalizeSuggestOptions(opts)
	if err := ValidateSuggestOptions(opts); err != nil {
		return nil, err
	}
	filters := newSearchFilters(searchOptionsFromSuggestOptions(opts))

	results, err := querySuggestionIDs(db, normalizedPrefix, filters, opts.Limit)
	if err != nil {
		return nil, err
	}
	return r.loadSuggestionResults(db, results)
}

func querySuggestionIDs(db *gorm.DB, normalizedPrefix string, filters searchFilters, limit int) ([]suggestionResultID, error) {
	prefixLower, prefixUpper := normalizedPrefixRange(normalizedPrefix)
	clauses, args := filters.searchTermPrefixRangeClauses("t", normalizedPrefix, prefixLower, prefixUpper)
	rankedColumns := learnerSignalColumns{
		frequencyRank: "frequency_rank",
		schoolLevel:   "school_rank",
		cefrLevel:     "cefr_level",
		oxfordLevel:   "oxford_level",
		cetLevel:      "cet_level",
		collinsStars:  "collins_stars",
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
				CASE WHEN t.frequency_rank = 0 THEN 999999999 ELSE t.frequency_rank END AS frequency_rank,
				CASE WHEN t.school_level = 0 THEN 999999999 ELSE t.school_level END AS school_rank,
				t.cefr_level,
				t.oxford_level,
				t.cet_level,
				t.collins_stars,
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
						%s,
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
				school_rank,
				cefr_level,
				oxford_level,
				cet_level,
				collins_stars,
				term_rank,
				ROW_NUMBER() OVER (
					PARTITION BY headword_key
					ORDER BY
						match_priority ASC,
						%s,
						id ASC,
						term_rank ASC
				) AS headword_rn
			FROM entry_ranked
			WHERE entry_rn = 1
		)
		SELECT id, frequency_rank, school_rank, cefr_level, oxford_level, cet_level, collins_stars
		FROM headword_ranked
		WHERE headword_rn = 1
		ORDER BY
			match_priority ASC,
			%s,
			id ASC,
			term_rank ASC,
			headword ASC
		LIMIT ?
		`,
		learnerSignalOrder("t"),
		strings.Join(clauses, " AND "),
		learnerSignalOrderForColumns(rankedColumns),
		learnerSignalOrderForColumns(rankedColumns))

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

	unionSQL := fmt.Sprintf(`
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
			CASE WHEN t.frequency_rank = 0 THEN 999999999 ELSE t.frequency_rank END AS freq_rank,
			CASE WHEN t.school_level = 0 THEN 999999999 ELSE t.school_level END AS school_rank,
			t.cefr_level,
			t.oxford_level,
			t.cet_level,
			t.collins_stars,
			t.headword,
			ROW_NUMBER() OVER (
				PARTITION BY t.entry_id
				ORDER BY
					%s,
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
	SELECT id, priority, freq_rank, school_rank, cefr_level, oxford_level, cet_level, collins_stars, id_ord, headword
	FROM ranked
	WHERE rn = 1
	ORDER BY
		%s,
		priority,
		id_ord,
		headword
	LIMIT ?
	`,
		learnerSignalOrder("t"),
		learnerSignalOrderForColumns(rankedLearnerSignalColumns()),
	)

	args := []interface{}{
		patternStart, patternPrefix, patternMiddle, patternEnd,
		patternStart, patternPrefix, patternMiddle, patternEnd,
		normalizedFuzzy,
		patternStart, patternPrefix, patternMiddle, patternEnd,
		limit,
	}

	type phraseRow struct {
		ID           int64
		Priority     int
		FreqRank     int
		SchoolRank   int
		CEFRLevel    int
		OxfordLevel  int
		CETLevel     int
		CollinsStars int
		IDOrd        int64
		Headword     string
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
		Order("sense_order ASC, id ASC").
		Find(&senses).Error; err != nil {
		return nil, err
	}
	return senses, nil
}
