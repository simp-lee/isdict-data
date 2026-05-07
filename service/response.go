package service

import "time"

// WordAnnotations contains word-level annotation fields used by service
// responses. Response DTOs live in this downstream package since commons v1.
type WordAnnotations struct {
	CEFRLevel         int        `json:"cefr_level"`
	CEFRLevelName     string     `json:"cefr_level_name,omitempty"`
	CEFRSource        string     `json:"cefr_source,omitempty"`
	CEFRRunID         *int64     `json:"cefr_run_id,omitempty"`
	CETLevel          int        `json:"cet_level"`
	CETRunID          *int64     `json:"cet_run_id,omitempty"`
	OxfordLevel       int        `json:"oxford_level"`
	OxfordRunID       *int64     `json:"oxford_run_id,omitempty"`
	SchoolLevel       int        `json:"school_level"`
	FrequencyRank     int        `json:"frequency_rank"`
	FrequencyCount    int        `json:"frequency_count"`
	FrequencyRunID    *int64     `json:"frequency_run_id,omitempty"`
	CollinsStars      int        `json:"collins_stars"`
	CollinsRunID      *int64     `json:"collins_run_id,omitempty"`
	TranslationZH     string     `json:"translation_zh,omitempty"`
	LearningUpdatedAt *time.Time `json:"learning_updated_at,omitempty"`
}

type WordResponse struct {
	ID                  int64                        `json:"id"`
	Headword            string                       `json:"headword"`
	SourceRunID         int64                        `json:"source_run_id,omitempty"`
	SourceRun           *ImportRunResponse           `json:"source_run,omitempty"`
	WordAnnotations                                  // Embedded: all annotation fields
	CEFRSourceSignals   []CEFRSourceSignalResponse   `json:"cefr_source_signals,omitempty"`
	Etymology           *EtymologyResponse           `json:"etymology,omitempty"`
	QueriedVariant      *QueriedVariantInfo          `json:"queried_variant,omitempty"`
	Pronunciations      []PronunciationResponse      `json:"pronunciations,omitempty"`
	PronunciationAudios []PronunciationAudioResponse `json:"pronunciation_audios,omitempty"`
	Senses              []SenseResponse              `json:"senses,omitempty"`
	Variants            []VariantResponse            `json:"variants,omitempty"`
	LexicalRelations    []LexicalRelationResponse    `json:"lexical_relations,omitempty"`
}

type PronunciationResponse struct {
	Accent       string `json:"accent"`
	IPA          string `json:"ipa"`
	IsPrimary    bool   `json:"is_primary"`
	DisplayOrder int    `json:"display_order,omitempty"`
}

type PronunciationAudioResponse struct {
	Accent        string `json:"accent"`
	AudioFilename string `json:"audio_filename"`
	IsPrimary     bool   `json:"is_primary"`
	DisplayOrder  int    `json:"display_order,omitempty"`
}

type SenseResponse struct {
	SenseID           int64                      `json:"sense_id"`
	POS               string                     `json:"pos"`
	CEFRLevel         int                        `json:"cefr_level"`
	CEFRLevelName     string                     `json:"cefr_level_name,omitempty"`
	CEFRSource        string                     `json:"cefr_source,omitempty"`
	CEFRRunID         *int64                     `json:"cefr_run_id,omitempty"`
	OxfordLevel       int                        `json:"oxford_level"`
	OxfordRunID       *int64                     `json:"oxford_run_id,omitempty"`
	LearningUpdatedAt *time.Time                 `json:"learning_updated_at,omitempty"`
	CEFRSourceSignals []CEFRSourceSignalResponse `json:"cefr_source_signals,omitempty"`
	DefinitionsEN     []GlossENResponse          `json:"definitions_en,omitempty"`
	DefinitionsZH     []GlossZHResponse          `json:"definitions_zh,omitempty"`
	Labels            []SenseLabelResponse       `json:"labels,omitempty"`
	LexicalRelations  []LexicalRelationResponse  `json:"lexical_relations,omitempty"`
	SenseOrder        int                        `json:"sense_order"`
	Examples          []ExampleResponse          `json:"examples,omitempty"`
}

type ExampleResponse struct {
	ExampleID    int64  `json:"example_id"`
	Source       string `json:"source,omitempty"`
	SentenceEN   string `json:"sentence_en,omitempty"`
	ExampleOrder int    `json:"example_order"`
}

type VariantResponse struct {
	FormText        string   `json:"form_text"`
	RelationKind    string   `json:"relation_kind"`
	FormType        string   `json:"form_type,omitempty"`
	SourceRelations []string `json:"source_relations,omitempty"`
	DisplayOrder    int      `json:"display_order,omitempty"`
}

type QueriedVariantInfo struct {
	FormText        string   `json:"form_text"`
	RelationKind    string   `json:"relation_kind"`
	FormType        string   `json:"form_type,omitempty"`
	SourceRelations []string `json:"source_relations,omitempty"`
	DisplayOrder    int      `json:"display_order,omitempty"`
}

type VariantReverseResponse struct {
	ID                  int64                        `json:"id"`
	Headword            string                       `json:"headword"`
	SourceRunID         int64                        `json:"source_run_id,omitempty"`
	SourceRun           *ImportRunResponse           `json:"source_run,omitempty"`
	WordAnnotations                                  // Embedded: all annotation fields
	CEFRSourceSignals   []CEFRSourceSignalResponse   `json:"cefr_source_signals,omitempty"`
	Etymology           *EtymologyResponse           `json:"etymology,omitempty"`
	VariantInfo         []VariantResponse            `json:"variant_info"`
	Pronunciations      []PronunciationResponse      `json:"pronunciations,omitempty"`
	PronunciationAudios []PronunciationAudioResponse `json:"pronunciation_audios,omitempty"`
	Senses              []SenseResponse              `json:"senses,omitempty"`
	LexicalRelations    []LexicalRelationResponse    `json:"lexical_relations,omitempty"`
}

type SearchResultResponse struct {
	ID              int64    `json:"id"`
	Headword        string   `json:"headword"`
	POS             []string `json:"pos"`
	WordAnnotations          // Embedded: all annotation fields
}

type SuggestResponse struct {
	Headword        string `json:"headword"`
	WordAnnotations        // Embedded: all annotation fields
}

type BatchRequest struct {
	Words                 []string `json:"words" binding:"required"`
	IncludeVariants       *bool    `json:"include_variants,omitempty"`
	IncludePronunciations *bool    `json:"include_pronunciations,omitempty"`
	IncludeSenses         *bool    `json:"include_senses,omitempty"`
}

type MetaInfo struct {
	Page      *int     `json:"page,omitempty"`
	PageSize  *int     `json:"page_size,omitempty"`
	Total     *int64   `json:"total,omitempty"`
	Requested *int     `json:"requested,omitempty"`
	Found     *int     `json:"found,omitempty"`
	NotFound  []string `json:"not_found,omitempty"`
	Limit     *int     `json:"limit,omitempty"`
	Offset    *int     `json:"offset,omitempty"`
}

type ImportRunResponse struct {
	ID              int64      `json:"id"`
	SourceName      string     `json:"source_name"`
	SourcePath      string     `json:"source_path,omitempty"`
	SourceDumpID    string     `json:"source_dump_id,omitempty"`
	SourceDumpDate  *time.Time `json:"source_dump_date,omitempty"`
	RawFileSHA256   string     `json:"raw_file_sha256,omitempty"`
	ErrorCount      int64      `json:"error_count,omitempty"`
	PipelineVersion string     `json:"pipeline_version,omitempty"`
	Status          string     `json:"status,omitempty"`
	RowCount        int64      `json:"row_count,omitempty"`
	EntryCount      int64      `json:"entry_count,omitempty"`
	Note            string     `json:"note,omitempty"`
	StartedAt       *time.Time `json:"started_at,omitempty"`
	FinishedAt      *time.Time `json:"finished_at,omitempty"`
}

type CEFRSourceSignalResponse struct {
	Source    string     `json:"source"`
	Level     int        `json:"level"`
	LevelName string     `json:"level_name,omitempty"`
	RunID     *int64     `json:"run_id,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type EtymologyResponse struct {
	Source    string     `json:"source"`
	RunID     int64      `json:"run_id,omitempty"`
	TextRaw   string     `json:"text_raw"`
	TextClean *string    `json:"text_clean,omitempty"`
	UpdatedAt *time.Time `json:"updated_at,omitempty"`
}

type GlossENResponse struct {
	GlossID    int64  `json:"gloss_id"`
	GlossOrder int    `json:"gloss_order"`
	TextEN     string `json:"text_en"`
}

type GlossZHResponse struct {
	GlossID      int64   `json:"gloss_id"`
	Source       string  `json:"source,omitempty"`
	SourceRunID  int64   `json:"source_run_id,omitempty"`
	GlossOrder   int     `json:"gloss_order"`
	TextZHHans   string  `json:"text_zh_hans"`
	DialectCode  *string `json:"dialect_code,omitempty"`
	Romanization *string `json:"romanization,omitempty"`
	IsPrimary    bool    `json:"is_primary"`
}

type SenseLabelResponse struct {
	Type     string `json:"type"`
	TypeName string `json:"type_name,omitempty"`
	Code     string `json:"code"`
	Name     string `json:"name,omitempty"`
	Order    int    `json:"order"`
}

type LexicalRelationResponse struct {
	RelationType         string `json:"relation_type"`
	RelationName         string `json:"relation_name,omitempty"`
	TargetText           string `json:"target_text"`
	TargetTextNormalized string `json:"target_text_normalized,omitempty"`
	DisplayOrder         int    `json:"display_order,omitempty"`
}
