//go:build readonlydb

package repository_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	commonmodel "github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-data/repository"
	"github.com/simp-lee/isdict-data/service"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestReadOnlyProductionDatabaseSmoke(t *testing.T) {
	dsn := os.Getenv("READONLY_SMOKE_DSN")
	if dsn == "" {
		t.Fatal("READONLY_SMOKE_DSN is required")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatalf("db handle: %v", err)
	}
	defer sqlDB.Close()

	tx := db.Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		t.Fatalf("begin read-only transaction: %v", tx.Error)
	}
	defer tx.Rollback()

	assertReadOnlySchemaCoverage(t, tx)

	repo := repository.NewRepository(tx)
	svc := service.NewWordService(repo, service.ServiceConfig{
		BatchMaxSize:    100,
		SearchMaxLimit:  20,
		SuggestMaxLimit: 20,
	})
	ctx := context.Background()

	word, err := svc.GetWordByHeadword(ctx, "learn", nil, true, true, true)
	if err != nil {
		t.Fatalf("GetWordByHeadword(learn): %v", err)
	}
	if word.Headword != "learn" {
		t.Fatalf("learn headword = %q", word.Headword)
	}
	if word.SourceRunID == 0 || word.SourceRun == nil {
		t.Fatalf("source run not hydrated: %#v", word)
	}
	if word.TranslationZH == "" {
		t.Fatalf("translation_zh is empty for learn")
	}
	if len(word.Pronunciations) == 0 || len(word.PronunciationAudios) == 0 {
		t.Fatalf("pronunciation data missing: ipas=%d audios=%d", len(word.Pronunciations), len(word.PronunciationAudios))
	}
	if len(word.Senses) == 0 || len(word.Senses[0].DefinitionsEN) == 0 {
		t.Fatalf("sense definitions missing: %#v", word.Senses)
	}
	if len(word.Variants) == 0 {
		t.Fatalf("entry_forms data missing")
	}
	if len(word.EntryExamples) == 0 {
		t.Fatalf("school entry_examples missing for learn")
	}

	headGroup, err := svc.GetEntryGroupByHeadword(ctx, "head", service.EntryGroupOptions{})
	if err != nil {
		t.Fatalf("GetEntryGroupByHeadword(head): %v", err)
	}
	if len(headGroup.Entries) < 2 {
		t.Fatalf("head entry group returned %d entries, want multiple", len(headGroup.Entries))
	}
	if !hasRelationPOS(headGroup.RelationGroupsByPOS, commonmodel.HeadwordRelationPOSCodeNoun) ||
		!hasRelationPOS(headGroup.RelationGroupsByPOS, commonmodel.HeadwordRelationPOSCodeVerb) {
		t.Fatalf("head relation groups missing noun/verb POS: %#v", headGroup.RelationGroupsByPOS)
	}

	disableRelations := false
	cloudGroup, err := svc.GetEntryGroupByHeadword(ctx, "cloud", service.EntryGroupOptions{
		IncludeSenses:    true,
		IncludeRelations: &disableRelations,
	})
	if err != nil {
		t.Fatalf("GetEntryGroupByHeadword(cloud): %v", err)
	}
	if !entryGroupHasDefinitions(cloudGroup) {
		t.Fatalf("cloud entry group missing school entry_definitions: %#v", cloudGroup.Entries)
	}

	var variantSample struct {
		FormText     string
		RelationKind string
	}
	if err := tx.Raw(`
		SELECT f.form_text, f.relation_kind
		FROM entry_forms f
		WHERE f.normalized_form <> ''
			AND NOT EXISTS (
				SELECT 1
				FROM entries e
				WHERE e.normalized_headword = f.normalized_form
			)
		ORDER BY f.id
		LIMIT 1
	`).Scan(&variantSample).Error; err != nil {
		t.Fatalf("select variant-only sample: %v", err)
	}
	if variantSample.FormText == "" || variantSample.RelationKind == "" {
		t.Fatalf("no entry_forms-only sample found")
	}

	variantWord, err := svc.GetWordByHeadword(ctx, variantSample.FormText, nil, true, true, true)
	if err != nil {
		t.Fatalf("GetWordByHeadword(%q): %v", variantSample.FormText, err)
	}
	if variantWord.QueriedVariant == nil || variantWord.QueriedVariant.FormText != variantSample.FormText {
		t.Fatalf("queried variant not exposed: %#v", variantWord.QueriedVariant)
	}
	if variantWord.QueriedVariant.RelationKind != variantSample.RelationKind {
		t.Fatalf("queried variant relation kind = %q, want %q", variantWord.QueriedVariant.RelationKind, variantSample.RelationKind)
	}

	reverse, err := svc.GetWordsByVariant(ctx, variantSample.FormText, &variantSample.RelationKind, true, true)
	if err != nil {
		t.Fatalf("GetWordsByVariant(%q): %v", variantSample.FormText, err)
	}
	if len(reverse) == 0 || len(reverse[0].VariantInfo) == 0 {
		t.Fatalf("variant reverse lookup returned no variant info: %#v", reverse)
	}

	search, meta, err := svc.SearchWords(ctx, "lear", service.SearchOptions{Limit: 5})
	if err != nil {
		t.Fatalf("SearchWords(lear): %v", err)
	}
	if len(search) == 0 || meta == nil || meta.Total == nil || *meta.Total == 0 {
		t.Fatalf("search returned no results: results=%#v meta=%#v", search, meta)
	}

	suggest, err := svc.SuggestWords(ctx, "lea", service.SuggestOptions{Limit: 5})
	if err != nil {
		t.Fatalf("SuggestWords(lea): %v", err)
	}
	if len(suggest) == 0 {
		t.Fatalf("suggest returned no results")
	}

	phrases, err := svc.SearchPhrases(ctx, "look", 5)
	if err != nil {
		t.Fatalf("SearchPhrases(look): %v", err)
	}
	if len(phrases) == 0 {
		t.Fatalf("phrase search returned no results")
	}

	pronunciations, err := svc.GetPronunciations(ctx, "learn", nil)
	if err != nil {
		t.Fatalf("GetPronunciations(learn): %v", err)
	}
	if len(pronunciations) == 0 {
		t.Fatalf("pronunciations returned no results")
	}

	senses, err := svc.GetSenses(ctx, "learn", nil, "both")
	if err != nil {
		t.Fatalf("GetSenses(learn): %v", err)
	}
	if len(senses) == 0 || len(senses[0].DefinitionsEN) == 0 {
		t.Fatalf("senses returned no definitions: %#v", senses)
	}
}

func assertReadOnlySchemaCoverage(t *testing.T, db *gorm.DB) {
	t.Helper()

	for _, table := range []string{
		"import_runs",
		"entries",
		"senses",
		"sense_glosses_en",
		"sense_glosses_zh",
		"sense_labels",
		"sense_examples",
		"entry_definitions",
		"entry_examples",
		"pronunciation_ipas",
		"pronunciation_audios",
		"entry_forms",
		"headword_relation_edges",
		"entry_summaries_zh",
		"entry_learning_signals",
		"entry_cefr_source_signals",
		"sense_learning_signals",
		"sense_cefr_source_signals",
		"entry_etymologies",
		"entry_search_terms",
		"featured_candidates",
	} {
		exists, err := tableExists(db, table)
		if err != nil {
			t.Fatalf("check table %s: %v", table, err)
		}
		if !exists {
			t.Fatalf("required table %s missing", table)
		}
	}
	if exists, err := tableExists(db, "lexical_relations"); err != nil {
		t.Fatalf("check lexical_relations absence: %v", err)
	} else if exists {
		t.Fatalf("legacy lexical_relations table should not exist")
	}

	var counts struct {
		EntryCount               int64
		EntryFormCount           int64
		SearchTermCount          int64
		HeadwordSearchTermCount  int64
		FormAliasSearchTermCount int64
		FeaturedCandidateCount   int64
		FeaturedEligibleCount    int64
		FeaturedDuplicateCount   int64
		FeaturedRankMismatch     int64
		FeaturedUniqueIndex      bool
		SchoolDefinitionCount    int64
		SchoolExampleCount       int64
		OEWNCompletedCount       int64
		SchoolCompletedCount     int64
	}
	if err := db.Raw(`
		SELECT
			(SELECT COUNT(*) FROM entries) AS entry_count,
			(SELECT COUNT(*) FROM entry_forms) AS entry_form_count,
			(SELECT COUNT(*) FROM entry_search_terms) AS search_term_count,
			(SELECT COUNT(*) FROM entry_search_terms WHERE term_kind = 'headword') AS headword_search_term_count,
			(SELECT COUNT(*) FROM entry_search_terms WHERE term_kind IN ('form', 'alias')) AS form_alias_search_term_count,
			(SELECT COUNT(*) FROM featured_candidates) AS featured_candidate_count,
			(
				SELECT COUNT(DISTINCT e.normalized_headword)
				FROM entries e
				JOIN entry_learning_signals ls ON ls.entry_id = e.id
				WHERE ls.frequency_rank > 0 OR ls.cefr_level > 0 OR ls.school_level > 0
			) AS featured_eligible_count,
			(
				SELECT COUNT(*)
				FROM (
					SELECT normalized_headword
					FROM featured_candidates
					GROUP BY normalized_headword
					HAVING COUNT(*) > 1
				) duplicate_featured_headwords
			) AS featured_duplicate_count,
			(
				SELECT COUNT(*)
				FROM (
					SELECT
						quality_rank,
						ROW_NUMBER() OVER (ORDER BY quality_rank, entry_id)::integer AS expected_rank
					FROM featured_candidates
				) ranked
				WHERE quality_rank <> expected_rank
			) AS featured_rank_mismatch,
			(
				SELECT EXISTS (
					SELECT 1
					FROM pg_indexes
					WHERE schemaname = current_schema()
						AND tablename = 'featured_candidates'
						AND indexname = 'idx_featured_candidates_normalized_headword'
						AND indexdef ILIKE '%UNIQUE%'
				)
			) AS featured_unique_index,
			(SELECT COUNT(*) FROM entry_definitions WHERE source = 'school') AS school_definition_count,
			(SELECT COUNT(*) FROM entry_examples WHERE source = 'school') AS school_example_count,
			(SELECT COUNT(*) FROM import_runs WHERE source_name = 'oewn' AND status = 'completed') AS oewn_completed_count,
			(SELECT COUNT(*) FROM import_runs WHERE source_name = 'school' AND status = 'completed') AS school_completed_count
	`).Scan(&counts).Error; err != nil {
		t.Fatalf("load read model counts: %v", err)
	}
	if counts.EntryCount == 0 {
		t.Fatal("entries table is empty")
	}
	if counts.SearchTermCount != counts.EntryCount+counts.EntryFormCount {
		t.Fatalf("entry_search_terms count = %d; want entries + entry_forms = %d", counts.SearchTermCount, counts.EntryCount+counts.EntryFormCount)
	}
	if counts.HeadwordSearchTermCount != counts.EntryCount {
		t.Fatalf("entry_search_terms headword count = %d; want entries count %d", counts.HeadwordSearchTermCount, counts.EntryCount)
	}
	if counts.FormAliasSearchTermCount != counts.EntryFormCount {
		t.Fatalf("entry_search_terms form/alias count = %d; want entry_forms count %d", counts.FormAliasSearchTermCount, counts.EntryFormCount)
	}
	if counts.FeaturedCandidateCount == 0 {
		t.Fatal("featured_candidates table is empty")
	}
	if counts.FeaturedCandidateCount != counts.FeaturedEligibleCount {
		t.Fatalf("featured_candidates count = %d; want eligible distinct normalized headwords %d", counts.FeaturedCandidateCount, counts.FeaturedEligibleCount)
	}
	if counts.FeaturedDuplicateCount != 0 {
		t.Fatalf("featured_candidates duplicate normalized_headword count = %d; want 0", counts.FeaturedDuplicateCount)
	}
	if counts.FeaturedRankMismatch != 0 {
		t.Fatalf("featured_candidates quality_rank mismatch count = %d; want 0", counts.FeaturedRankMismatch)
	}
	if !counts.FeaturedUniqueIndex {
		t.Fatal("featured_candidates normalized_headword unique index is missing")
	}
	if counts.SchoolDefinitionCount == 0 {
		t.Fatal("entry_definitions(source='school') is empty")
	}
	if counts.SchoolExampleCount == 0 {
		t.Fatal("entry_examples(source='school') is empty")
	}
	if counts.OEWNCompletedCount == 0 || counts.SchoolCompletedCount == 0 {
		t.Fatalf("completed import_runs missing: oewn=%d school=%d", counts.OEWNCompletedCount, counts.SchoolCompletedCount)
	}
}

func tableExists(db *gorm.DB, table string) (bool, error) {
	var exists bool
	err := db.Raw(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
				AND table_name = ?
		)
	`, table).Scan(&exists).Error
	return exists, err
}

func hasRelationPOS(groups []service.POSRelationGroupsResponse, posCode int) bool {
	for _, group := range groups {
		if group.POSCode == posCode && len(group.Groups) > 0 {
			return true
		}
	}
	return false
}

func entryGroupHasDefinitions(group *service.EntryGroupResponse) bool {
	if group == nil {
		return false
	}
	for _, entry := range group.Entries {
		if len(entry.EntryDefinitions) > 0 {
			return true
		}
	}
	return false
}
