package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-commons/norm"
	"github.com/simp-lee/isdict-data/queryvalidation"
	"github.com/simp-lee/isdict-data/repository"
)

// WordService handles business logic for word operations.
type WordService struct {
	repo    repository.WordRepository
	config  ServiceConfig
	shuffle func([]string)

	featuredCandidatesMu sync.RWMutex
	featuredCandidates   featuredCandidateCache
}

var (
	ErrBatchLimitExceeded = errors.New("batch limit exceeded")
	ErrWordNotFound       = repository.ErrWordNotFound
	ErrVariantNotFound    = repository.ErrVariantNotFound
)

const (
	langBoth    = "both"
	langEnglish = "en"
	langChinese = "zh"
)

type batchCandidate struct {
	word    *repository.Word
	variant *repository.WordVariant
}

type batchCandidateIndex map[string][]batchCandidate

type batchIncludeOptions struct {
	variants       bool
	pronunciations bool
	senses         bool
}

type RelationQueryOptions = repository.RelationQueryOptions
type SearchOptions = repository.SearchOptions
type SuggestOptions = repository.SuggestOptions

type EntryGroupOptions struct {
	AccentCode            *string
	IncludeVariants       bool
	IncludePronunciations bool
	IncludeSenses         bool
	IncludeRelations      *bool
	RelationOptions       RelationQueryOptions
}

func NewWordService(repo repository.WordRepository, cfg ServiceConfig) *WordService {
	return &WordService{
		repo:   repo,
		config: normalizeServiceConfig(cfg),
	}
}

func (s *WordService) GetWordByHeadword(ctx context.Context, headword string, accentCode *string, includeVariants, includePronunciations, includeSenses bool) (*WordResponse, error) {
	word, variant, err := s.repo.GetWordByHeadword(ctx, headword, includeVariants, includePronunciations, includeSenses)
	if err != nil {
		return nil, err
	}

	return s.convertToWordResponse(word, variant, accentCode, includeVariants, includePronunciations, includeSenses), nil
}

func (s *WordService) GetEntryGroupByHeadword(ctx context.Context, headword string, opts EntryGroupOptions) (*EntryGroupResponse, error) {
	words, variant, err := s.repo.GetEntryGroupByHeadword(ctx, headword, opts.IncludeVariants, opts.IncludePronunciations, opts.IncludeSenses)
	if err != nil {
		return nil, err
	}
	if len(words) == 0 {
		return nil, ErrWordNotFound
	}

	entries := make([]WordResponse, 0, len(words))
	for i := range words {
		entries = append(entries, *s.convertToWordResponse(&words[i], nil, opts.AccentCode, opts.IncludeVariants, opts.IncludePronunciations, opts.IncludeSenses))
	}

	normalizedHeadword := words[0].NormalizedHeadword
	if normalizedHeadword == "" {
		normalizedHeadword = norm.NormalizeHeadword(words[0].Headword)
	}
	resp := &EntryGroupResponse{
		Headword:           words[0].Headword,
		HeadwordNormalized: normalizedHeadword,
		Entries:            entries,
		QueriedVariant:     queriedVariantInfo(variant),
	}

	if includeEntryGroupRelations(opts) {
		relationGroupsByPOS, err := s.relationGroupsByPOS(ctx, normalizedHeadword, words, opts.RelationOptions)
		if err != nil {
			return nil, err
		}
		resp.RelationGroupsByPOS = relationGroupsByPOS
	}

	return resp, nil
}

func (s *WordService) GetHeadwordRelationGroups(ctx context.Context, headword string, posCode int, opts RelationQueryOptions) ([]RelationGroupResponse, error) {
	groups, err := s.repo.GetHeadwordRelationGroups(ctx, headword, posCode, opts)
	if err != nil {
		return nil, err
	}
	return relationGroupResponses(groups), nil
}

func (s *WordService) GetWordsByVariant(ctx context.Context, variant string, kindStr *string, includePronunciations, includeSenses bool) ([]VariantReverseResponse, error) {
	var kind *string
	if kindStr != nil {
		switch *kindStr {
		case model.RelationKindForm:
			k := model.RelationKindForm
			kind = &k
		case model.RelationKindAlias:
			k := model.RelationKindAlias
			kind = &k
		}
	}

	words, variants, err := s.repo.GetWordsByVariant(ctx, variant, kind, includePronunciations, includeSenses)
	if err != nil {
		return nil, err
	}

	variantMap := make(map[int64][]repository.WordVariant)
	for i := range variants {
		wordID := variants[i].WordID
		variantMap[wordID] = append(variantMap[wordID], variants[i])
	}

	results := make([]VariantReverseResponse, 0, len(words))
	for _, word := range words {
		resp := VariantReverseResponse{
			ID:                word.ID,
			Headword:          word.Headword,
			SourceRunID:       word.SourceRunID,
			SourceRun:         importRunResponse(word.SourceRun),
			WordAnnotations:   wordAnnotations(word),
			CEFRSourceSignals: entryCEFRSourceSignalResponses(word.CEFRSourceSignals),
			Etymology:         etymologyResponse(word.Etymology),
		}

		if includePronunciations {
			resp.Pronunciations = s.convertPronunciations(word.Pronunciations, nil)
			resp.PronunciationAudios = s.convertPronunciationAudios(word.PronunciationAudios, nil)
		}
		if includeSenses {
			resp.Senses = s.convertSenses(word.Senses, langBoth, word.Pos)
		}
		if variants, ok := variantMap[word.ID]; ok {
			resp.VariantInfo = make([]VariantResponse, 0, len(variants))
			for _, v := range variants {
				resp.VariantInfo = append(resp.VariantInfo, *s.convertVariant(v))
			}
		}

		results = append(results, resp)
	}

	return results, nil
}

func (s *WordService) GetWordsBatch(ctx context.Context, req *BatchRequest) ([]WordResponse, *MetaInfo, error) {
	includeOptions, err := s.prepareBatchRequest(req)
	if err != nil {
		return nil, nil, err
	}
	if req == nil || len(req.Words) == 0 {
		return []WordResponse{}, nil, nil
	}

	words, err := s.repo.GetWordsByHeadwords(ctx, req.Words, includeOptions.variants, includeOptions.pronunciations, includeOptions.senses)
	if err != nil {
		return nil, nil, err
	}
	index := buildBatchCandidateIndex(words)
	if err := s.resolveBatchEntryForms(ctx, req.Words, index, includeOptions); err != nil {
		return nil, nil, err
	}

	responses, notFound := s.buildBatchResponses(req.Words, index, includeOptions)
	requested := len(req.Words)
	found := len(responses)

	meta := &MetaInfo{
		Requested: &requested,
		Found:     &found,
		NotFound:  notFound,
	}

	return responses, meta, nil
}

func (s *WordService) prepareBatchRequest(req *BatchRequest) (batchIncludeOptions, error) {
	includeOptions := resolveBatchIncludeOptions(req)
	if req == nil {
		return includeOptions, nil
	}
	if len(req.Words) == 0 {
		return includeOptions, nil
	}
	if len(req.Words) > s.config.BatchMaxSize {
		return batchIncludeOptions{}, fmt.Errorf("%w: maximum %d words per request", ErrBatchLimitExceeded, s.config.BatchMaxSize)
	}

	cleanedWords := queryvalidation.NormalizeBatchWords(req.Words)
	if len(cleanedWords) == 0 {
		req.Words = cleanedWords
		return includeOptions, nil
	}
	if len(cleanedWords) > s.config.BatchMaxSize {
		return batchIncludeOptions{}, fmt.Errorf("%w: maximum %d words per request", ErrBatchLimitExceeded, s.config.BatchMaxSize)
	}
	req.Words = cleanedWords
	return includeOptions, nil
}

func resolveBatchIncludeOptions(req *BatchRequest) batchIncludeOptions {
	options := batchIncludeOptions{
		variants:       true,
		pronunciations: true,
		senses:         true,
	}
	if req == nil {
		return options
	}
	if req.IncludeVariants != nil {
		options.variants = *req.IncludeVariants
	}
	if req.IncludePronunciations != nil {
		options.pronunciations = *req.IncludePronunciations
	}
	if req.IncludeSenses != nil {
		options.senses = *req.IncludeSenses
	}
	return options
}

func buildBatchCandidateIndex(words []repository.Word) batchCandidateIndex {
	index := make(batchCandidateIndex, len(words))
	for i := range words {
		index.addCandidate(&words[i], nil, "")
	}
	return index
}

func (index batchCandidateIndex) addCandidate(word *repository.Word, variant *repository.WordVariant, alias string) {
	if word == nil {
		return
	}
	for _, key := range batchCandidateKeys(word, alias) {
		if index.hasCandidate(key, word, variant) {
			continue
		}
		index[key] = append(index[key], batchCandidate{word: word, variant: variant})
	}
}

func batchCandidateKeys(word *repository.Word, alias string) []string {
	keys := []string{norm.NormalizeHeadword(word.Headword)}
	if trimmed := strings.TrimSpace(alias); trimmed != "" {
		aliasKey := norm.NormalizeHeadword(trimmed)
		if aliasKey != "" && aliasKey != keys[0] {
			keys = append(keys, aliasKey)
		}
	}
	return keys
}

func (index batchCandidateIndex) hasCandidate(key string, word *repository.Word, variant *repository.WordVariant) bool {
	for _, existing := range index[key] {
		if sameBatchCandidate(existing, batchCandidate{word: word, variant: variant}) {
			return true
		}
	}
	return false
}

func sameBatchCandidate(left, right batchCandidate) bool {
	if left.word == nil || right.word == nil || left.word.ID != right.word.ID {
		return false
	}
	if left.variant == nil || right.variant == nil {
		return left.variant == nil && right.variant == nil
	}
	return left.variant.FormText == right.variant.FormText
}

func (index batchCandidateIndex) selectCandidate(input string) (batchCandidate, bool) {
	key := norm.NormalizeHeadword(input)
	candidates := index[key]
	if len(candidates) == 0 {
		return batchCandidate{}, false
	}
	return preferredBatchCandidate(candidates, input), true
}

func preferredBatchCandidate(candidates []batchCandidate, input string) batchCandidate {
	for _, candidate := range candidates {
		if candidate.word.Headword == input {
			return candidate
		}
	}
	for _, candidate := range candidates {
		if candidate.variant != nil && candidate.variant.FormText == input {
			return candidate
		}
	}
	for _, candidate := range candidates {
		if candidate.word.Headword == strings.ToLower(candidate.word.Headword) {
			return candidate
		}
	}
	return candidates[0]
}

func unresolvedBatchWords(words []string, index batchCandidateIndex) []string {
	missing := make([]string, 0)
	for _, word := range words {
		if _, ok := index.selectCandidate(word); !ok {
			missing = append(missing, word)
		}
	}
	return missing
}

func (s *WordService) resolveBatchEntryForms(ctx context.Context, inputs []string, index batchCandidateIndex, options batchIncludeOptions) error {
	missing := unresolvedBatchWords(inputs, index)
	if len(missing) == 0 {
		return nil
	}

	matches, err := s.repo.GetWordsByVariants(ctx, missing, options.variants, options.pronunciations, options.senses)
	if err != nil {
		return err
	}
	for i := range matches {
		index.addCandidate(&matches[i].Word, &matches[i].Variant, matches[i].Variant.FormText)
	}
	return nil
}

func (s *WordService) buildBatchResponses(inputs []string, index batchCandidateIndex, options batchIncludeOptions) ([]WordResponse, []string) {
	responses := make([]WordResponse, 0, len(inputs))
	notFound := make([]string, 0)
	for _, input := range inputs {
		candidate, ok := index.selectCandidate(input)
		if !ok {
			notFound = append(notFound, input)
			continue
		}
		responses = append(responses, *s.convertToWordResponse(candidate.word, candidate.variant, nil, options.variants, options.pronunciations, options.senses))
	}
	return responses, notFound
}

func (s *WordService) SearchWords(ctx context.Context, keyword string, opts SearchOptions) ([]SearchResultResponse, *MetaInfo, error) {
	keywordLength := queryvalidation.NormalizedRuneCount(keyword)
	if keywordLength < queryvalidation.MinQueryLength {
		return nil, nil, fmt.Errorf("keyword must be at least %d characters", queryvalidation.MinQueryLength)
	}
	if queryvalidation.TrimmedRuneCount(keyword) > 100 {
		return nil, nil, errors.New("keyword must not exceed 100 characters")
	}

	if opts.Limit <= 0 {
		opts.Limit = 20
	}
	if opts.Limit > s.config.SearchMaxLimit {
		opts.Limit = s.config.SearchMaxLimit
	}
	if opts.Offset < 0 {
		opts.Offset = 0
	}
	if err := repository.ValidateSearchOptions(opts); err != nil {
		return nil, nil, err
	}

	words, total, err := s.repo.SearchWords(ctx, keyword, opts)
	if err != nil {
		return nil, nil, err
	}

	results := make([]SearchResultResponse, 0, len(words))
	for _, word := range words {
		results = append(results, SearchResultResponse{
			ID:              word.ID,
			Headword:        word.Headword,
			POS:             []string{posDisplayName(word.Pos)},
			WordAnnotations: wordAnnotations(word),
		})
	}

	meta := &MetaInfo{
		Total:  &total,
		Limit:  &opts.Limit,
		Offset: &opts.Offset,
	}

	return results, meta, nil
}

func (s *WordService) SuggestWords(ctx context.Context, prefix string, opts SuggestOptions) ([]SuggestResponse, error) {
	prefixLength := queryvalidation.NormalizedRuneCount(prefix)
	if prefixLength < queryvalidation.MinQueryLength {
		return nil, fmt.Errorf("prefix must be at least %d characters", queryvalidation.MinQueryLength)
	}
	if queryvalidation.TrimmedRuneCount(prefix) > 50 {
		return nil, errors.New("prefix must not exceed 50 characters")
	}

	if opts.Limit <= 0 {
		opts.Limit = 10
	}
	if opts.Limit > s.config.SuggestMaxLimit {
		opts.Limit = s.config.SuggestMaxLimit
	}
	if err := repository.ValidateSuggestOptions(opts); err != nil {
		return nil, err
	}

	words, err := s.repo.SuggestWords(ctx, prefix, opts)
	if err != nil {
		return nil, err
	}

	results := make([]SuggestResponse, 0, len(words))
	for _, word := range words {
		results = append(results, SuggestResponse{
			Headword:        word.Headword,
			WordAnnotations: wordAnnotations(word),
		})
	}

	return results, nil
}

func (s *WordService) SearchPhrases(ctx context.Context, keyword string, limit int) ([]SuggestResponse, error) {
	keywordRunes := []rune(strings.TrimSpace(keyword))
	if len(keywordRunes) < 1 {
		return nil, errors.New("keyword must be at least 1 character")
	}
	if len(keywordRunes) > 50 {
		return nil, errors.New("keyword must not exceed 50 characters")
	}

	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}

	words, err := s.repo.SearchPhrases(ctx, keyword, limit)
	if err != nil {
		return nil, err
	}

	results := make([]SuggestResponse, 0, len(words))
	for _, word := range words {
		results = append(results, SuggestResponse{
			Headword:        word.Headword,
			WordAnnotations: wordAnnotations(word),
		})
	}

	return results, nil
}

func (s *WordService) GetPronunciations(ctx context.Context, headword string, accentCode *string) ([]PronunciationResponse, error) {
	word, _, err := s.repo.GetWordByHeadword(ctx, headword, false, false, false)
	if err != nil {
		return nil, err
	}

	pronunciations, err := s.repo.GetPronunciationsByWordID(ctx, word.ID, accentCode)
	if err != nil {
		return nil, err
	}

	return s.convertPronunciations(pronunciations, accentCode), nil
}

func (s *WordService) GetSenses(ctx context.Context, headword string, posCode *string, lang string) ([]SenseResponse, error) {
	word, _, err := s.repo.GetWordByHeadword(ctx, headword, false, false, false)
	if err != nil {
		return nil, err
	}

	senses, err := s.repo.GetSensesByWordID(ctx, word.ID, posCode)
	if err != nil {
		return nil, err
	}

	return s.convertSenses(senses, lang, word.Pos), nil
}

func (s *WordService) convertToWordResponse(word *repository.Word, variant *repository.WordVariant, accentCode *string, includeVariants, includePronunciations, includeSenses bool) *WordResponse {
	resp := &WordResponse{
		ID:                word.ID,
		Headword:          word.Headword,
		SourceRunID:       word.SourceRunID,
		SourceRun:         importRunResponse(word.SourceRun),
		WordAnnotations:   wordAnnotations(*word),
		CEFRSourceSignals: entryCEFRSourceSignalResponses(word.CEFRSourceSignals),
		Etymology:         etymologyResponse(word.Etymology),
		EntryDefinitions:  entryDefinitionResponses(word.EntryDefinitions),
		EntryExamples:     entryExampleResponses(word.EntryExamples),
	}
	if strings.TrimSpace(word.Pos) != "" {
		resp.POS = word.Pos
		resp.POSName = posDisplayName(word.Pos)
	}

	if variant != nil {
		resp.QueriedVariant = queriedVariantInfo(variant)
	}

	if includePronunciations {
		resp.Pronunciations = s.convertPronunciations(word.Pronunciations, accentCode)
		resp.PronunciationAudios = s.convertPronunciationAudios(word.PronunciationAudios, accentCode)
	}
	if includeSenses {
		resp.Senses = s.convertSenses(word.Senses, langBoth, word.Pos)
	}
	if includeVariants {
		resp.Variants = s.convertVariants(word.WordVariants)
	}

	return resp
}

func includeEntryGroupRelations(opts EntryGroupOptions) bool {
	return opts.IncludeRelations == nil || *opts.IncludeRelations
}

func (s *WordService) relationGroupsByPOS(ctx context.Context, normalizedHeadword string, words []repository.Word, opts RelationQueryOptions) ([]POSRelationGroupsResponse, error) {
	results := make([]POSRelationGroupsResponse, 0, len(words))
	seenPOSCodes := make(map[int]struct{}, len(words))
	for _, word := range words {
		posCode, ok := entryPOSToHeadwordRelationPOSCode(word.Pos)
		if !ok {
			continue
		}
		if _, exists := seenPOSCodes[posCode]; exists {
			continue
		}
		seenPOSCodes[posCode] = struct{}{}

		groups, err := s.GetHeadwordRelationGroups(ctx, normalizedHeadword, posCode, opts)
		if err != nil {
			return nil, err
		}
		if len(groups) == 0 {
			continue
		}
		results = append(results, POSRelationGroupsResponse{
			POSCode: posCode,
			POSName: headwordRelationPOSDisplayName(posCode),
			Groups:  groups,
		})
	}
	return results, nil
}

func wordAnnotations(word repository.Word) WordAnnotations {
	annotations := WordAnnotations{TranslationZH: translationZH(word.SummariesZH)}
	if signal := word.LearningSignal; signal != nil {
		annotations.CEFRLevel = int(signal.CEFRLevel)
		annotations.CEFRLevelName = cefrLevelName(int(signal.CEFRLevel))
		annotations.CEFRSource = signal.CEFRSource
		annotations.CETLevel = int(signal.CETLevel)
		annotations.OxfordLevel = int(signal.OxfordLevel)
		annotations.SchoolLevel = int(signal.SchoolLevel)
		annotations.SchoolLevelName = schoolLevelName(signal.SchoolLevel)
		annotations.SchoolRunID = signal.SchoolRunID
		annotations.FrequencyRank = signal.FrequencyRank
		annotations.FrequencyCount = signal.FrequencyCount
		annotations.CollinsStars = int(signal.CollinsStars)
		annotations.CEFRRunID = signal.CEFRRunID
		annotations.CETRunID = signal.CETRunID
		annotations.OxfordRunID = signal.OxfordRunID
		annotations.FrequencyRunID = signal.FrequencyRunID
		annotations.CollinsRunID = signal.CollinsRunID
		annotations.LearningUpdatedAt = timePtr(signal.UpdatedAt)
	}
	return annotations
}

func translationZH(summaries []model.EntrySummaryZH) string {
	if len(summaries) == 0 {
		return ""
	}
	return summaries[0].SummaryText
}

func (s *WordService) convertPronunciations(pronunciations []repository.Pronunciation, accentCode *string) []PronunciationResponse {
	results := make([]PronunciationResponse, 0, len(pronunciations))
	for _, p := range pronunciations {
		if accentCode != nil && p.Accent != *accentCode {
			continue
		}
		results = append(results, PronunciationResponse{
			Accent:       accentDisplayName(p.Accent),
			IPA:          p.IPA,
			IsPrimary:    p.IsPrimary,
			DisplayOrder: p.DisplayOrder,
		})
	}
	return results
}

func (s *WordService) convertPronunciationAudios(audios []repository.PronunciationAudio, accentCode *string) []PronunciationAudioResponse {
	results := make([]PronunciationAudioResponse, 0, len(audios))
	for _, audio := range audios {
		if accentCode != nil && audio.Accent != *accentCode {
			continue
		}
		results = append(results, PronunciationAudioResponse{
			Accent:        accentDisplayName(audio.Accent),
			AudioFilename: audio.AudioFilename,
			IsPrimary:     audio.IsPrimary,
			DisplayOrder:  audio.DisplayOrder,
		})
	}
	return results
}

func (s *WordService) convertSenses(senses []repository.Sense, lang string, pos string) []SenseResponse {
	results := make([]SenseResponse, 0, len(senses))
	for _, sense := range senses {
		senseResp := SenseResponse{
			SenseID:           sense.ID,
			POS:               posDisplayName(pos),
			CEFRSourceSignals: senseCEFRSourceSignalResponses(sense.CEFRSourceSignals),
			DefinitionsEN:     glossENResponses(sense.GlossesEN),
			DefinitionsZH:     glossZHResponses(sense.GlossesZH),
			Labels:            senseLabelResponses(sense.Labels),
			SenseOrder:        sense.SenseOrder,
			Examples:          s.convertExamples(sense.Examples, lang),
		}
		if signal := sense.LearningSignal; signal != nil {
			senseResp.CEFRLevel = int(signal.CEFRLevel)
			senseResp.CEFRLevelName = cefrLevelName(int(signal.CEFRLevel))
			senseResp.CEFRSource = signal.CEFRSource
			senseResp.OxfordLevel = int(signal.OxfordLevel)
			senseResp.CEFRRunID = signal.CEFRRunID
			senseResp.OxfordRunID = signal.OxfordRunID
			senseResp.LearningUpdatedAt = timePtr(signal.UpdatedAt)
		}

		applyDefinitionLangFilter(&senseResp, lang)
		results = append(results, senseResp)
	}
	return results
}

func (s *WordService) convertExamples(examples []repository.Example, lang string) []ExampleResponse {
	results := make([]ExampleResponse, 0, len(examples))
	for _, ex := range examples {
		exampleResp := ExampleResponse{
			ExampleID:    ex.ID,
			Source:       ex.Source,
			SentenceEN:   ex.SentenceEN,
			ExampleOrder: ex.ExampleOrder,
		}

		applyExampleLangFilter(&exampleResp, lang)
		results = append(results, exampleResp)
	}
	return results
}

func applyDefinitionLangFilter(senseResp *SenseResponse, lang string) {
	switch lang {
	case langEnglish:
		senseResp.DefinitionsZH = nil
	case langChinese:
		senseResp.DefinitionsEN = nil
	}
}

func applyExampleLangFilter(exampleResp *ExampleResponse, lang string) {
	switch lang {
	case langChinese:
		exampleResp.SentenceEN = ""
	}
}

func (s *WordService) convertVariants(variants []repository.WordVariant) []VariantResponse {
	results := make([]VariantResponse, 0, len(variants))
	for _, v := range variants {
		results = append(results, *s.convertVariant(v))
	}
	return results
}

func (s *WordService) convertVariant(v repository.WordVariant) *VariantResponse {
	resp := &VariantResponse{
		FormText:        v.FormText,
		RelationKind:    v.RelationKind,
		SourceRelations: []string(v.SourceRelations),
		DisplayOrder:    v.DisplayOrder,
	}
	if v.FormType != nil {
		resp.FormType = *v.FormType
	}
	return resp
}

func queriedVariantInfo(variant *repository.WordVariant) *QueriedVariantInfo {
	if variant == nil {
		return nil
	}
	resp := &QueriedVariantInfo{
		FormText:        variant.FormText,
		RelationKind:    variant.RelationKind,
		SourceRelations: []string(variant.SourceRelations),
		DisplayOrder:    variant.DisplayOrder,
	}
	if variant.FormType != nil {
		resp.FormType = *variant.FormType
	}
	return resp
}

func importRunResponse(run *model.ImportRun) *ImportRunResponse {
	if run == nil || (run.ID == 0 && strings.TrimSpace(run.SourceName) == "") {
		return nil
	}
	return &ImportRunResponse{
		ID:              run.ID,
		SourceName:      run.SourceName,
		SourcePath:      run.SourcePath,
		SourceDumpID:    run.SourceDumpID,
		SourceDumpDate:  run.SourceDumpDate,
		RawFileSHA256:   run.RawFileSHA256,
		ErrorCount:      run.ErrorCount,
		PipelineVersion: run.PipelineVersion,
		Status:          run.Status,
		RowCount:        run.RowCount,
		EntryCount:      run.EntryCount,
		Note:            run.Note,
		StartedAt:       timePtr(run.StartedAt),
		FinishedAt:      run.FinishedAt,
	}
}

func entryCEFRSourceSignalResponses(signals []model.EntryCEFRSourceSignal) []CEFRSourceSignalResponse {
	results := make([]CEFRSourceSignalResponse, 0, len(signals))
	for _, signal := range signals {
		results = append(results, CEFRSourceSignalResponse{
			Source:    signal.CEFRSource,
			Level:     int(signal.CEFRLevel),
			LevelName: cefrLevelName(int(signal.CEFRLevel)),
			RunID:     signal.CEFRRunID,
			UpdatedAt: timePtr(signal.UpdatedAt),
		})
	}
	return results
}

func senseCEFRSourceSignalResponses(signals []model.SenseCEFRSourceSignal) []CEFRSourceSignalResponse {
	results := make([]CEFRSourceSignalResponse, 0, len(signals))
	for _, signal := range signals {
		results = append(results, CEFRSourceSignalResponse{
			Source:    signal.CEFRSource,
			Level:     int(signal.CEFRLevel),
			LevelName: cefrLevelName(int(signal.CEFRLevel)),
			RunID:     signal.CEFRRunID,
			UpdatedAt: timePtr(signal.UpdatedAt),
		})
	}
	return results
}

func etymologyResponse(etymology *model.EntryEtymology) *EtymologyResponse {
	if etymology == nil || (etymology.EntryID == 0 && strings.TrimSpace(etymology.EtymologyTextRaw) == "") {
		return nil
	}
	return &EtymologyResponse{
		Source:    etymology.Source,
		RunID:     etymology.SourceRunID,
		TextRaw:   etymology.EtymologyTextRaw,
		TextClean: etymology.EtymologyTextClean,
		UpdatedAt: timePtr(etymology.UpdatedAt),
	}
}

func entryDefinitionResponses(definitions []model.EntryDefinition) []EntryDefinitionResponse {
	results := make([]EntryDefinitionResponse, 0, len(definitions))
	for _, definition := range definitions {
		results = append(results, EntryDefinitionResponse{
			DefinitionID:    definition.ID,
			Source:          definition.Source,
			SourceRunID:     definition.SourceRunID,
			SenseID:         definition.SenseID,
			POS:             definition.POS,
			DefinitionOrder: int(definition.DefinitionOrder),
			TextZHHans:      definition.TextZHHans,
			TextEN:          definition.TextEN,
		})
	}
	return results
}

func entryExampleResponses(examples []model.EntryExample) []EntryExampleResponse {
	results := make([]EntryExampleResponse, 0, len(examples))
	for _, example := range examples {
		results = append(results, EntryExampleResponse{
			ExampleID:      example.ID,
			Source:         example.Source,
			SourceRunID:    example.SourceRunID,
			SenseID:        example.SenseID,
			ExampleOrder:   int(example.ExampleOrder),
			SentenceEN:     example.SentenceEN,
			SentenceZHHans: example.SentenceZHHans,
		})
	}
	return results
}

func glossENResponses(glosses []model.SenseGlossEN) []GlossENResponse {
	results := make([]GlossENResponse, 0, len(glosses))
	for _, gloss := range glosses {
		results = append(results, GlossENResponse{
			GlossID:    gloss.ID,
			GlossOrder: int(gloss.GlossOrder),
			TextEN:     gloss.TextEN,
		})
	}
	return results
}

func glossZHResponses(glosses []model.SenseGlossZH) []GlossZHResponse {
	results := make([]GlossZHResponse, 0, len(glosses))
	for _, gloss := range glosses {
		results = append(results, GlossZHResponse{
			GlossID:      gloss.ID,
			Source:       gloss.Source,
			SourceRunID:  gloss.SourceRunID,
			GlossOrder:   int(gloss.GlossOrder),
			TextZHHans:   gloss.TextZHHans,
			DialectCode:  gloss.DialectCode,
			Romanization: gloss.Romanization,
			IsPrimary:    gloss.IsPrimary,
		})
	}
	return results
}

func senseLabelResponses(labels []model.SenseLabel) []SenseLabelResponse {
	results := make([]SenseLabelResponse, 0, len(labels))
	for _, label := range labels {
		results = append(results, SenseLabelResponse{
			Type:     label.LabelType,
			TypeName: labelTypeDisplayName(label.LabelType),
			Code:     label.LabelCode,
			Name:     labelDisplayName(label.LabelType, label.LabelCode),
			Order:    int(label.LabelOrder),
		})
	}
	return results
}

func labelTypeDisplayName(code string) string {
	if name, ok := model.LabelTypeCodeToName()[code]; ok {
		return name
	}
	return code
}

func labelDisplayName(labelType, code string) string {
	if byType, ok := model.LabelCodeToNameByType()[labelType]; ok {
		if name, ok := byType[code]; ok {
			return name
		}
	}
	return code
}

func relationDisplayName(code string) string {
	if name, ok := model.RelationTypeCodeToName()[code]; ok {
		return name
	}
	return code
}

func headwordRelationPOSDisplayName(code int) string {
	if name, ok := model.HeadwordRelationPOSCodeToName()[code]; ok {
		return name
	}
	return ""
}

func entryPOSToHeadwordRelationPOSCode(pos string) (int, bool) {
	switch pos {
	case model.POSNoun:
		return model.HeadwordRelationPOSCodeNoun, true
	case model.POSVerb:
		return model.HeadwordRelationPOSCodeVerb, true
	case model.POSAdjective:
		return model.HeadwordRelationPOSCodeAdjective, true
	case model.POSAdverb:
		return model.HeadwordRelationPOSCodeAdverb, true
	default:
		return 0, false
	}
}

func relationGroupResponses(groups []repository.HeadwordRelationGroup) []RelationGroupResponse {
	results := make([]RelationGroupResponse, 0, len(groups))
	for _, group := range groups {
		results = append(results, RelationGroupResponse{
			RelationType: group.RelationType,
			RelationName: relationDisplayName(group.RelationType),
			Items:        relationItemResponses(group.Items),
		})
	}
	return results
}

func relationItemResponses(items []repository.HeadwordRelationItem) []RelationItemResponse {
	results := make([]RelationItemResponse, 0, len(items))
	for _, item := range items {
		results = append(results, RelationItemResponse{
			TargetHeadword:           item.TargetHeadword,
			TargetHeadwordNormalized: item.TargetHeadwordNormalized,
			TargetPOSCode:            item.TargetPOSCode,
			TargetPOSName:            headwordRelationPOSDisplayName(item.TargetPOSCode),
			SourceRelationType:       item.SourceRelationType,
			SourceSynsetID:           item.SourceSynsetID,
			TargetSynsetID:           item.TargetSynsetID,
			SourceSenseID:            item.SourceSenseID,
			TargetSenseID:            item.TargetSenseID,
			EvidenceCount:            item.EvidenceCount,
			HasTargetEntry:           item.HasTargetEntry,
		})
	}
	return results
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	return &value
}

func cefrLevelName(level int) string {
	if level <= 0 {
		return ""
	}
	if name, ok := model.CEFRLevelCodeToName()[int16(level)]; ok && name != "unknown" {
		return name
	}
	return ""
}

func schoolLevelName(level int16) string {
	if level <= 0 {
		return ""
	}
	if name, ok := model.SchoolLevelCodeToName()[level]; ok && name != "unknown" {
		return name
	}
	return ""
}

func posDisplayName(code string) string {
	if name, ok := model.POSCodeToName()[code]; ok {
		return name
	}
	if strings.TrimSpace(code) == "" {
		return "unknown"
	}
	return code
}

func accentDisplayName(code string) string {
	if name, ok := model.AccentCodeToName()[code]; ok {
		return name
	}
	if strings.TrimSpace(code) == "" {
		return "unknown"
	}
	return code
}
