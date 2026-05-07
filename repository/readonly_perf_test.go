//go:build perfdb

package repository_test

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"

	"github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-data/repository"
	"github.com/simp-lee/isdict-data/service"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type readOnlyPerfDB struct {
	db     *gorm.DB
	repo   *repository.Repository
	svc    *service.WordService
	sample readOnlyPerfSample
}

type readOnlyPerfSample struct {
	EntryID           int64
	Headword          string
	NormalizedInput   string
	POS               string
	Accent            string
	VariantForm       string
	VariantKind       string
	AliasForm         string
	AliasKind         string
	BatchHeadwords    []string
	BatchForms        []string
	SearchKeyword     string
	SuggestPrefix     string
	FormSearchKeyword string
	FormSuggestPrefix string
	PhraseKeyword     string
	PhraseFormKeyword string
	LearningCEFRLevel int
	OxfordKeyword     string
	OxfordLevel       int
	CETKeyword        string
	CETLevel          int
	MaxFrequencyRank  int
	MinCollinsStars   int
	CollinsKeyword    string
	CollinsStars      int
	NoMatchKeyword    string
}

func BenchmarkReadOnlyRepositoryQueries(b *testing.B) {
	perfDB := openReadOnlyPerfDB(b)
	ctx := context.Background()
	sample := perfDB.sample

	b.Run("GetWordByHeadword_Exact_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordByHeadword(ctx, sample.Headword, false, false, false)
			return err
		})
	})
	b.Run("GetWordByHeadword_Exact_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordByHeadword(ctx, sample.Headword, true, true, true)
			return err
		})
	})
	b.Run("GetWordByHeadword_Normalized_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordByHeadword(ctx, sample.NormalizedInput, false, false, false)
			return err
		})
	})
	b.Run("GetWordByHeadword_EntryForms_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordByHeadword(ctx, sample.VariantForm, true, true, true)
			return err
		})
	})
	b.Run("GetWordByHeadword_EntryForms_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordByHeadword(ctx, sample.VariantForm, false, false, false)
			return err
		})
	})
	b.Run("GetWordsByHeadwords_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetWordsByHeadwords(ctx, sample.BatchHeadwords, false, false, false)
			return err
		})
	})
	b.Run("GetWordsByHeadwords_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetWordsByHeadwords(ctx, sample.BatchHeadwords, true, true, true)
			return err
		})
	})
	b.Run("GetWordsByVariant_Form_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordsByVariant(ctx, sample.VariantForm, &sample.VariantKind, false, false)
			return err
		})
	})
	b.Run("GetWordsByVariant_Form_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordsByVariant(ctx, sample.VariantForm, &sample.VariantKind, true, true)
			return err
		})
	})
	b.Run("GetWordsByVariant_NoKind", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.GetWordsByVariant(ctx, sample.VariantForm, nil, true, true)
			return err
		})
	})
	if sample.AliasForm != "" {
		b.Run("GetWordsByVariant_Alias", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, _, err := perfDB.repo.GetWordsByVariant(ctx, sample.AliasForm, &sample.AliasKind, true, true)
				return err
			})
		})
	}
	b.Run("GetWordsByVariants_Batch", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetWordsByVariants(ctx, sample.BatchForms, true, true, true)
			return err
		})
	})
	b.Run("GetWordsByVariants_Batch_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetWordsByVariants(ctx, sample.BatchForms, false, false, false)
			return err
		})
	})
	b.Run("ListFeaturedCandidateHeadwords", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.ListFeaturedCandidateHeadwords(ctx)
			return err
		})
	})
	b.Run("SearchWords_Basic", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.SearchKeyword, nil, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_WithPOS", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.SearchKeyword, &sample.POS, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_WithLearningFilters", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.SearchKeyword, nil, &sample.LearningCEFRLevel, nil, nil, &sample.MaxFrequencyRank, &sample.MinCollinsStars, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_MaxFrequencyRank", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.SearchKeyword, nil, nil, nil, nil, &sample.MaxFrequencyRank, nil, 20, 0)
			return err
		})
	})
	if sample.OxfordLevel > 0 {
		b.Run("SearchWords_OxfordLevel", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, _, err := perfDB.repo.SearchWords(ctx, sample.OxfordKeyword, nil, nil, &sample.OxfordLevel, nil, nil, nil, 20, 0)
				return err
			})
		})
	}
	if sample.CETLevel > 0 {
		b.Run("SearchWords_CETLevel", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, _, err := perfDB.repo.SearchWords(ctx, sample.CETKeyword, nil, nil, nil, &sample.CETLevel, nil, nil, 20, 0)
				return err
			})
		})
	}
	if sample.CollinsStars > 0 {
		b.Run("SearchWords_CollinsStars", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, _, err := perfDB.repo.SearchWords(ctx, sample.CollinsKeyword, nil, nil, nil, nil, nil, &sample.CollinsStars, 20, 0)
				return err
			})
		})
	}
	b.Run("SearchWords_FormOnlyMatch", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.FormSearchKeyword, nil, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_Offset", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.SearchKeyword, nil, nil, nil, nil, nil, nil, 20, 20)
			return err
		})
	})
	b.Run("SearchWords_NoResult", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.repo.SearchWords(ctx, sample.NoMatchKeyword, nil, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SuggestWords_Basic", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.SuggestWords(ctx, sample.SuggestPrefix, nil, nil, nil, nil, nil, 20)
			return err
		})
	})
	b.Run("SuggestWords_WithLearningFilters", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.SuggestWords(ctx, sample.SuggestPrefix, &sample.LearningCEFRLevel, nil, nil, &sample.MaxFrequencyRank, &sample.MinCollinsStars, 20)
			return err
		})
	})
	b.Run("SuggestWords_FormOnlyMatch", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.SuggestWords(ctx, sample.FormSuggestPrefix, nil, nil, nil, nil, nil, 20)
			return err
		})
	})
	if sample.OxfordLevel > 0 {
		b.Run("SuggestWords_OxfordLevel", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, err := perfDB.repo.SuggestWords(ctx, firstRunes(sample.OxfordKeyword, 3), nil, &sample.OxfordLevel, nil, nil, nil, 20)
				return err
			})
		})
	}
	if sample.CETLevel > 0 {
		b.Run("SuggestWords_CETLevel", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, err := perfDB.repo.SuggestWords(ctx, firstRunes(sample.CETKeyword, 3), nil, nil, &sample.CETLevel, nil, nil, 20)
				return err
			})
		})
	}
	if sample.CollinsStars > 0 {
		b.Run("SuggestWords_CollinsStars", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, err := perfDB.repo.SuggestWords(ctx, firstRunes(sample.CollinsKeyword, 3), nil, nil, nil, nil, &sample.CollinsStars, 20)
				return err
			})
		})
	}
	b.Run("SearchPhrases", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.SearchPhrases(ctx, sample.PhraseKeyword, 20)
			return err
		})
	})
	if sample.PhraseFormKeyword != "" {
		b.Run("SearchPhrases_FormMatch", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, err := perfDB.repo.SearchPhrases(ctx, sample.PhraseFormKeyword, 20)
				return err
			})
		})
	}
	b.Run("GetPronunciationsByWordID_All", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetPronunciationsByWordID(ctx, sample.EntryID, nil)
			return err
		})
	})
	b.Run("GetPronunciationsByWordID_Accent", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetPronunciationsByWordID(ctx, sample.EntryID, &sample.Accent)
			return err
		})
	})
	b.Run("GetSensesByWordID_All", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetSensesByWordID(ctx, sample.EntryID, nil)
			return err
		})
	})
	b.Run("GetSensesByWordID_POS", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.repo.GetSensesByWordID(ctx, sample.EntryID, &sample.POS)
			return err
		})
	})
}

func BenchmarkReadOnlyServiceQueries(b *testing.B) {
	perfDB := openReadOnlyPerfDB(b)
	ctx := context.Background()
	sample := perfDB.sample

	b.Run("GetWordByHeadword_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordByHeadword(ctx, sample.Headword, nil, false, false, false)
			return err
		})
	})
	b.Run("GetWordByHeadword_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordByHeadword(ctx, sample.Headword, nil, true, true, true)
			return err
		})
	})
	b.Run("GetWordByHeadword_Full_AccentFilter", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordByHeadword(ctx, sample.Headword, &sample.Accent, true, true, true)
			return err
		})
	})
	b.Run("GetWordByHeadword_EntryForms_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordByHeadword(ctx, sample.VariantForm, nil, true, true, true)
			return err
		})
	})
	b.Run("GetWordsByVariant_Minimal", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordsByVariant(ctx, sample.VariantForm, &sample.VariantKind, false, false)
			return err
		})
	})
	b.Run("GetWordsByVariant_Full", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetWordsByVariant(ctx, sample.VariantForm, &sample.VariantKind, true, true)
			return err
		})
	})
	b.Run("GetWordsBatch_Minimal", func(b *testing.B) {
		req := service.BatchRequest{
			Words:                 append([]string(nil), sample.BatchHeadwords...),
			IncludeVariants:       boolPtr(false),
			IncludePronunciations: boolPtr(false),
			IncludeSenses:         boolPtr(false),
		}
		benchmarkReadOnlyQuery(b, func() error {
			copyReq := req
			copyReq.Words = append([]string(nil), req.Words...)
			_, _, err := perfDB.svc.GetWordsBatch(ctx, &copyReq)
			return err
		})
	})
	b.Run("GetWordsBatch_Full_MixedHeadwordsAndForms", func(b *testing.B) {
		words := append([]string(nil), sample.BatchHeadwords...)
		words = append(words, sample.BatchForms...)
		req := service.BatchRequest{Words: words}
		benchmarkReadOnlyQuery(b, func() error {
			copyReq := req
			copyReq.Words = append([]string(nil), req.Words...)
			_, _, err := perfDB.svc.GetWordsBatch(ctx, &copyReq)
			return err
		})
	})
	b.Run("SearchWords", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.svc.SearchWords(ctx, sample.SearchKeyword, nil, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_WithLearningFilters", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.svc.SearchWords(ctx, sample.SearchKeyword, nil, &sample.LearningCEFRLevel, nil, nil, &sample.MaxFrequencyRank, &sample.MinCollinsStars, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_FormOnlyMatch", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.svc.SearchWords(ctx, sample.FormSearchKeyword, nil, nil, nil, nil, nil, nil, 20, 0)
			return err
		})
	})
	b.Run("SearchWords_Offset", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, _, err := perfDB.svc.SearchWords(ctx, sample.SearchKeyword, nil, nil, nil, nil, nil, nil, 20, 20)
			return err
		})
	})
	b.Run("SuggestWords", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.SuggestWords(ctx, sample.SuggestPrefix, nil, nil, nil, nil, nil, 20)
			return err
		})
	})
	b.Run("SuggestWords_WithLearningFilters", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.SuggestWords(ctx, sample.SuggestPrefix, &sample.LearningCEFRLevel, nil, nil, &sample.MaxFrequencyRank, &sample.MinCollinsStars, 20)
			return err
		})
	})
	b.Run("SuggestWords_FormOnlyMatch", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.SuggestWords(ctx, sample.FormSuggestPrefix, nil, nil, nil, nil, nil, 20)
			return err
		})
	})
	b.Run("SearchPhrases", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.SearchPhrases(ctx, sample.PhraseKeyword, 20)
			return err
		})
	})
	if sample.PhraseFormKeyword != "" {
		b.Run("SearchPhrases_FormMatch", func(b *testing.B) {
			benchmarkReadOnlyQuery(b, func() error {
				_, err := perfDB.svc.SearchPhrases(ctx, sample.PhraseFormKeyword, 20)
				return err
			})
		})
	}
	b.Run("GetPronunciations", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetPronunciations(ctx, sample.Headword, nil)
			return err
		})
	})
	b.Run("GetPronunciations_Accent", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetPronunciations(ctx, sample.Headword, &sample.Accent)
			return err
		})
	})
	b.Run("GetSenses", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetSenses(ctx, sample.Headword, nil, "both")
			return err
		})
	})
	b.Run("GetSenses_POS", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetSenses(ctx, sample.Headword, &sample.POS, "both")
			return err
		})
	})
	b.Run("GetSenses_EN", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetSenses(ctx, sample.Headword, nil, "en")
			return err
		})
	})
	b.Run("GetSenses_ZH", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.GetSenses(ctx, sample.Headword, nil, "zh")
			return err
		})
	})
	b.Run("RandomFeaturedWords_WarmCache", func(b *testing.B) {
		if _, err := perfDB.svc.RandomFeaturedWords(ctx, 10); err != nil {
			b.Fatalf("warm featured words cache: %v", err)
		}
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.RandomFeaturedWords(ctx, 10)
			return err
		})
	})
	b.Run("RandomFeaturedWords_ColdCache", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			svc := newReadOnlyPerfService(perfDB.repo)
			_, err := svc.RandomFeaturedWords(ctx, 10)
			return err
		})
	})
	b.Run("RandomFeaturedPhrases_WarmCache", func(b *testing.B) {
		if _, err := perfDB.svc.RandomFeaturedPhrases(ctx, 10); err != nil {
			b.Fatalf("warm featured phrases cache: %v", err)
		}
		benchmarkReadOnlyQuery(b, func() error {
			_, err := perfDB.svc.RandomFeaturedPhrases(ctx, 10)
			return err
		})
	})
	b.Run("RandomFeaturedPhrases_ColdCache", func(b *testing.B) {
		benchmarkReadOnlyQuery(b, func() error {
			svc := newReadOnlyPerfService(perfDB.repo)
			_, err := svc.RandomFeaturedPhrases(ctx, 10)
			return err
		})
	})
}

func openReadOnlyPerfDB(tb testing.TB) *readOnlyPerfDB {
	tb.Helper()

	dsn := os.Getenv("READONLY_PERF_DSN")
	if dsn == "" {
		dsn = os.Getenv("READONLY_SMOKE_DSN")
	}
	if dsn == "" {
		tb.Skip("READONLY_PERF_DSN or READONLY_SMOKE_DSN is required")
	}

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		tb.Fatalf("open database: %v", err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		tb.Fatalf("db handle: %v", err)
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(4)
	tb.Cleanup(func() {
		_ = sqlDB.Close()
	})

	tx := db.Begin(&sql.TxOptions{ReadOnly: true})
	if tx.Error != nil {
		tb.Fatalf("begin read-only transaction: %v", tx.Error)
	}
	tb.Cleanup(func() {
		_ = tx.Rollback().Error
	})
	if err := tx.Exec("SET LOCAL statement_timeout = '120s'").Error; err != nil {
		tb.Fatalf("set statement timeout: %v", err)
	}

	repo := repository.NewRepository(tx)
	return &readOnlyPerfDB{
		db:     tx,
		repo:   repo,
		svc:    newReadOnlyPerfService(repo),
		sample: loadReadOnlyPerfSample(tb, tx),
	}
}

func newReadOnlyPerfService(repo repository.WordRepository) *service.WordService {
	return service.NewWordService(repo, service.ServiceConfig{
		BatchMaxSize:    100,
		SearchMaxLimit:  100,
		SuggestMaxLimit: 50,
	})
}

func loadReadOnlyPerfSample(tb testing.TB, db *gorm.DB) readOnlyPerfSample {
	tb.Helper()

	var sample readOnlyPerfSample
	var entrySample struct {
		EntryID  int64
		Headword string
		POS      string
	}
	if err := db.Raw(`
		SELECT e.id AS entry_id, e.headword, e.pos
		FROM entries e
		WHERE e.normalized_headword = 'learn'
		ORDER BY CASE WHEN e.headword = 'learn' THEN 0 ELSE 1 END, e.id
		LIMIT 1
	`).Scan(&entrySample).Error; err != nil {
		tb.Fatalf("select canonical entry sample: %v", err)
	}
	if entrySample.EntryID == 0 {
		tb.Fatalf("no canonical entry sample found")
	}
	sample.EntryID = entrySample.EntryID
	sample.Headword = entrySample.Headword
	sample.POS = entrySample.POS
	sample.NormalizedInput = strings.ToUpper(sample.Headword)
	sample.SearchKeyword = firstRunes(sample.Headword, 4)
	sample.SuggestPrefix = firstRunes(sample.Headword, 3)
	sample.LearningCEFRLevel = int(model.CEFRLevelA1)
	sample.MaxFrequencyRank = 50000
	sample.MinCollinsStars = 1
	sample.NoMatchKeyword = "zzzzzznonexistent"

	var accentSample struct {
		Accent string
	}
	if err := db.Raw(`
		SELECT accent_code AS accent
		FROM pronunciation_ipas
		WHERE entry_id = ?
		ORDER BY is_primary DESC, display_order ASC, id ASC
		LIMIT 1
	`, sample.EntryID).Scan(&accentSample).Error; err != nil {
		tb.Fatalf("select accent sample: %v", err)
	}
	sample.Accent = accentSample.Accent
	if sample.Accent == "" {
		sample.Accent = model.AccentUnknown
	}

	var variantSample struct {
		VariantForm string
		VariantKind string
	}
	if err := db.Raw(`
		SELECT f.form_text AS variant_form, f.relation_kind AS variant_kind
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
		tb.Fatalf("select entry_forms-only sample: %v", err)
	}
	sample.VariantForm = variantSample.VariantForm
	sample.VariantKind = variantSample.VariantKind
	if sample.VariantForm == "" || sample.VariantKind == "" {
		tb.Fatalf("no entry_forms-only sample found")
	}
	sample.FormSearchKeyword = firstRunes(sample.VariantForm, 4)
	sample.FormSuggestPrefix = firstRunes(sample.VariantForm, 3)

	var aliasSample struct {
		AliasForm string
		AliasKind string
	}
	if err := db.Raw(`
		SELECT f.form_text AS alias_form, f.relation_kind AS alias_kind
		FROM entry_forms f
		WHERE f.relation_kind = ?
			AND f.normalized_form <> ''
		ORDER BY f.id
		LIMIT 1
	`, model.RelationKindAlias).Scan(&aliasSample).Error; err != nil {
		tb.Fatalf("select alias sample: %v", err)
	}
	sample.AliasForm = aliasSample.AliasForm
	sample.AliasKind = aliasSample.AliasKind

	sample.BatchHeadwords = pluckStrings(tb, db, `
		SELECT headword
		FROM (
			SELECT headword, MIN(id) AS first_id
			FROM entries
			WHERE normalized_headword <> ''
			GROUP BY headword
			ORDER BY MIN(id)
			LIMIT 20
		) AS sampled
		ORDER BY first_id
	`)
	if len(sample.BatchHeadwords) == 0 {
		tb.Fatalf("no batch headword samples found")
	}

	sample.BatchForms = pluckStrings(tb, db, `
		SELECT form_text
		FROM entry_forms
		WHERE normalized_form <> ''
		ORDER BY id
		LIMIT 20
	`)
	if len(sample.BatchForms) == 0 {
		tb.Fatalf("no batch form samples found")
	}

	var phrase struct {
		Keyword string
	}
	if err := db.Raw(`
		SELECT split_part(headword, ' ', 1) AS keyword
		FROM entries
		WHERE is_multiword = true
			AND position(' ' IN headword) > 0
			AND length(split_part(headword, ' ', 1)) >= 3
		ORDER BY id
		LIMIT 1
	`).Scan(&phrase).Error; err != nil {
		tb.Fatalf("select phrase keyword sample: %v", err)
	}
	if phrase.Keyword == "" {
		sample.PhraseKeyword = "look"
	} else {
		sample.PhraseKeyword = phrase.Keyword
	}

	var phraseForm struct {
		Keyword string
	}
	if err := db.Raw(`
		SELECT split_part(f.form_text, ' ', 1) AS keyword
		FROM entry_forms f
		INNER JOIN entries e ON e.id = f.entry_id
		WHERE e.is_multiword = true
			AND f.normalized_form <> ''
			AND position(' ' IN f.form_text) > 0
			AND length(split_part(f.form_text, ' ', 1)) >= 3
		ORDER BY f.id
		LIMIT 1
	`).Scan(&phraseForm).Error; err != nil {
		tb.Fatalf("select phrase form keyword sample: %v", err)
	}
	sample.PhraseFormKeyword = phraseForm.Keyword

	sample.OxfordKeyword, sample.OxfordLevel = loadReadOnlyLearningFilterSample(tb, db, `
		SELECT e.headword, els.oxford_level AS value
		FROM entry_learning_signals els
		INNER JOIN entries e ON e.id = els.entry_id
		WHERE els.oxford_level > 0
			AND length(e.normalized_headword) >= 3
		ORDER BY CASE WHEN COALESCE(els.frequency_rank, 0) = 0 THEN 999999 ELSE els.frequency_rank END ASC, e.id ASC
		LIMIT 1
	`)
	sample.CETKeyword, sample.CETLevel = loadReadOnlyLearningFilterSample(tb, db, `
		SELECT e.headword, els.cet_level AS value
		FROM entry_learning_signals els
		INNER JOIN entries e ON e.id = els.entry_id
		WHERE els.cet_level > 0
			AND length(e.normalized_headword) >= 3
		ORDER BY CASE WHEN COALESCE(els.frequency_rank, 0) = 0 THEN 999999 ELSE els.frequency_rank END ASC, e.id ASC
		LIMIT 1
	`)
	sample.CollinsKeyword, sample.CollinsStars = loadReadOnlyLearningFilterSample(tb, db, `
		SELECT e.headword, els.collins_stars AS value
		FROM entry_learning_signals els
		INNER JOIN entries e ON e.id = els.entry_id
		WHERE els.collins_stars > 0
			AND length(e.normalized_headword) >= 3
		ORDER BY els.collins_stars DESC, CASE WHEN COALESCE(els.frequency_rank, 0) = 0 THEN 999999 ELSE els.frequency_rank END ASC, e.id ASC
		LIMIT 1
	`)

	return sample
}

func loadReadOnlyLearningFilterSample(tb testing.TB, db *gorm.DB, query string) (string, int) {
	tb.Helper()

	var sample struct {
		Headword string
		Value    int
	}
	if err := db.Raw(query).Scan(&sample).Error; err != nil {
		tb.Fatalf("select learning filter sample: %v", err)
	}
	return firstRunes(sample.Headword, 4), sample.Value
}

func pluckStrings(tb testing.TB, db *gorm.DB, query string, args ...any) []string {
	tb.Helper()

	var values []string
	if err := db.Raw(query, args...).Scan(&values).Error; err != nil {
		tb.Fatalf("pluck strings: %v", err)
	}
	return values
}

func benchmarkReadOnlyQuery(b *testing.B, fn func() error) {
	b.Helper()
	b.ReportAllocs()
	if err := fn(); err != nil {
		b.Fatalf("warm query failed: %v", err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := fn(); err != nil {
			b.Fatalf("query failed: %v", err)
		}
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func firstRunes(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	runes := []rune(strings.TrimSpace(value))
	if len(runes) <= limit {
		return string(runes)
	}
	return string(runes[:limit])
}
