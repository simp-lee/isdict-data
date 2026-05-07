package repository

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/simp-lee/isdict-commons/migration"
	commonmodel "github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-commons/norm"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	gormlogger "gorm.io/gorm/logger"
)

var (
	postgresIntegrationDSNEnv     = "TEST_POSTGRES_DSN"
	allowNonLocalPostgresTestsEnv = "ISDICT_ALLOW_NONLOCAL_TEST_POSTGRES"
	externalPostgresTestMu        sync.Mutex
)

type commonsV1Fixture struct {
	word       Word
	form       WordVariant
	ipaBritish Pronunciation
	ipaUS      Pronunciation
	audio      PronunciationAudio
	sense      Sense
}

func TestValidatePostgresIntegrationDSNForDestructiveSetup(t *testing.T) {
	tests := []struct {
		name          string
		dsn           string
		allowNonLocal bool
		wantErr       bool
	}{
		{
			name:    "keyword localhost host",
			dsn:     "host=localhost user=test password=test dbname=isdict sslmode=disable",
			wantErr: false,
		},
		{
			name:    "url loopback host",
			dsn:     "postgres://test:test@127.0.0.1:5432/isdict?sslmode=disable",
			wantErr: false,
		},
		{
			name:    "keyword unix socket host",
			dsn:     "host=/var/run/postgresql user=test password=test dbname=isdict sslmode=disable",
			wantErr: false,
		},
		{
			name:    "keyword remote host",
			dsn:     "host=103.197.25.147 user=test password=test dbname=isdict sslmode=disable",
			wantErr: true,
		},
		{
			name:    "url remote host",
			dsn:     "postgres://test:test@db.example.com:5432/isdict?sslmode=disable",
			wantErr: true,
		},
		{
			name:          "keyword remote host with explicit override",
			dsn:           "host=103.197.25.147 user=test password=test dbname=isdict sslmode=disable",
			allowNonLocal: true,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validatePostgresIntegrationDSNForDestructiveSetupForHostPolicy(tt.dsn, tt.allowNonLocal)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validatePostgresIntegrationDSNForDestructiveSetupForHostPolicy() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsTransientExternalPostgresResetError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "wrapped connection reset by peer",
			err: &net.OpError{
				Op:  "read",
				Net: "tcp",
				Err: &os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET},
			},
			want: true,
		},
		{
			name: "plain sql error",
			err:  errors.New("ERROR: relation does not exist"),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isTransientExternalPostgresResetError(tt.err); got != tt.want {
				t.Fatalf("isTransientExternalPostgresResetError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRepository_PostgresCommonsV1FullHydration(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	fixture := seedCommonsV1Fixture(t, db)

	word, variant, err := repo.GetWordByHeadword(context.Background(), "learnt", true, true, true)
	if err != nil {
		t.Fatalf("GetWordByHeadword() error = %v", err)
	}
	if word == nil || word.ID != fixture.word.ID {
		t.Fatalf("word = %#v, want fixture word", word)
	}
	if variant == nil || variant.ID != fixture.form.ID {
		t.Fatalf("variant = %#v, want fixture form", variant)
	}

	if word.SourceRun == nil || word.SourceRun.SourceName != "wiktionary" {
		t.Fatalf("SourceRun not hydrated: %#v", word.SourceRun)
	}
	if word.LearningSignal == nil || word.LearningSignal.FrequencyRank != 123 {
		t.Fatalf("LearningSignal not hydrated: %#v", word.LearningSignal)
	}
	if len(word.CEFRSourceSignals) != 1 || word.CEFRSourceSignals[0].CEFRSource != commonmodel.CEFRSourceCEFRJ {
		t.Fatalf("CEFRSourceSignals not hydrated: %#v", word.CEFRSourceSignals)
	}
	if len(word.SummariesZH) != 1 || word.SummariesZH[0].SummaryText != "学习" {
		t.Fatalf("SummariesZH not hydrated: summaries=%#v", word.SummariesZH)
	}
	if word.Etymology == nil || word.Etymology.EtymologyTextRaw == "" {
		t.Fatalf("Etymology not hydrated: %#v", word.Etymology)
	}
	if len(word.Pronunciations) != 2 || len(word.PronunciationAudios) != 1 {
		t.Fatalf("pronunciation data not hydrated: ipas=%#v audios=%#v", word.Pronunciations, word.PronunciationAudios)
	}
	if len(word.WordVariants) != 1 || word.WordVariants[0].FormText != "learnt" {
		t.Fatalf("entry forms not hydrated: %#v", word.WordVariants)
	}
	if len(word.LexicalRelations) != 1 || word.LexicalRelations[0].TargetText != "learner" {
		t.Fatalf("entry lexical relations not hydrated: %#v", word.LexicalRelations)
	}
	if len(word.Senses) != 1 {
		t.Fatalf("senses len = %d, want 1", len(word.Senses))
	}

	sense := word.Senses[0]
	if sense.LearningSignal == nil || sense.LearningSignal.OxfordLevel != 1 {
		t.Fatalf("sense learning signal not hydrated: %#v", sense.LearningSignal)
	}
	if len(sense.CEFRSourceSignals) != 1 || sense.CEFRSourceSignals[0].CEFRSource != commonmodel.CEFRSourceOctanove {
		t.Fatalf("sense CEFR source signals not hydrated: %#v", sense.CEFRSourceSignals)
	}
	if len(sense.GlossesEN) != 1 || sense.GlossesEN[0].TextEN != "to gain knowledge" {
		t.Fatalf("English glosses not hydrated: %#v", sense.GlossesEN)
	}
	if len(sense.GlossesZH) != 1 || sense.GlossesZH[0].TextZHHans != "学习知识" {
		t.Fatalf("Chinese glosses not hydrated: %#v", sense.GlossesZH)
	}
	if len(sense.Labels) != 1 || len(sense.Examples) != 1 || len(sense.LexicalRelations) != 1 {
		t.Fatalf("sense related data not hydrated: labels=%#v examples=%#v relations=%#v", sense.Labels, sense.Examples, sense.LexicalRelations)
	}
}

func TestRepository_PostgresCoreQueriesUseCommonsV1Schema(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	fixture := seedCommonsV1Fixture(t, db)

	headwords, err := repo.ListFeaturedCandidateHeadwords(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidateHeadwords() error = %v", err)
	}
	if len(headwords) != 1 || headwords[0] != "learn" {
		t.Fatalf("headwords = %#v, want [learn]", headwords)
	}

	pos := commonmodel.POSVerb
	cefr := 2
	words, total, err := repo.SearchWords(context.Background(), "lear", &pos, &cefr, nil, nil, nil, nil, 10, 0)
	if err != nil {
		t.Fatalf("SearchWords() error = %v", err)
	}
	if total != 1 || len(words) != 1 || words[0].ID != fixture.word.ID {
		t.Fatalf("SearchWords() = words:%#v total:%d, want fixture", words, total)
	}

	formWords, formTotal, err := repo.SearchWords(context.Background(), "learnt", nil, nil, nil, nil, nil, nil, 10, 0)
	if err != nil {
		t.Fatalf("SearchWords(form) error = %v", err)
	}
	if formTotal != 1 || len(formWords) != 1 || formWords[0].ID != fixture.word.ID {
		t.Fatalf("SearchWords(form) = words:%#v total:%d, want fixture", formWords, formTotal)
	}

	suggestions, err := repo.SuggestWords(context.Background(), "lea", nil, nil, nil, nil, nil, 5)
	if err != nil {
		t.Fatalf("SuggestWords() error = %v", err)
	}
	if len(suggestions) != 1 || suggestions[0].Headword != "learn" {
		t.Fatalf("SuggestWords() = %#v, want learn", suggestions)
	}

	formSuggestions, err := repo.SuggestWords(context.Background(), "learnt", nil, nil, nil, nil, nil, 5)
	if err != nil {
		t.Fatalf("SuggestWords(form) error = %v", err)
	}
	if len(formSuggestions) != 1 || formSuggestions[0].Headword != "learn" {
		t.Fatalf("SuggestWords(form) = %#v, want learn", formSuggestions)
	}

	accent := commonmodel.AccentBritish
	pronunciations, err := repo.GetPronunciationsByWordID(context.Background(), fixture.word.ID, &accent)
	if err != nil {
		t.Fatalf("GetPronunciationsByWordID() error = %v", err)
	}
	if len(pronunciations) != 1 || pronunciations[0].ID != fixture.ipaBritish.ID {
		t.Fatalf("GetPronunciationsByWordID() = %#v, want British IPA", pronunciations)
	}

	senses, err := repo.GetSensesByWordID(context.Background(), fixture.word.ID, &pos)
	if err != nil {
		t.Fatalf("GetSensesByWordID() error = %v", err)
	}
	if len(senses) != 1 || senses[0].ID != fixture.sense.ID || len(senses[0].Labels) != 1 {
		t.Fatalf("GetSensesByWordID() = %#v, want hydrated sense", senses)
	}
}

func newPostgresIntegrationRepository(t *testing.T) (*Repository, *gorm.DB) {
	t.Helper()

	dsn := requirePostgresIntegrationDSN(t)
	db, err := gorm.Open(pgdriver.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		t.Fatalf("gorm.Open() error = %v", err)
	}
	t.Cleanup(func() {
		sqlDB, _ := db.DB()
		if sqlDB != nil {
			_ = sqlDB.Close()
		}
	})

	externalPostgresTestMu.Lock()
	t.Cleanup(externalPostgresTestMu.Unlock)
	if err := migration.RunMigration(db, migration.MigrateOptions{DropTables: true}); err != nil {
		t.Fatalf("migrate commons v1 schema: %v", err)
	}

	return &Repository{db: db}, db
}

func seedCommonsV1Fixture(t *testing.T, db *gorm.DB) commonsV1Fixture {
	t.Helper()

	now := time.Date(2026, 5, 5, 12, 0, 0, 0, time.UTC)
	cleanEtymology := "from Old English leornian"
	romanization := "xue2 xi2"

	importRun := commonmodel.ImportRun{
		ID:              1,
		SourceName:      "wiktionary",
		SourcePath:      "/data/enwiktionary.xml",
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	if err := db.Omit(clause.Associations).Create(&importRun).Error; err != nil {
		t.Fatalf("create import run: %v", err)
	}

	word := Word{
		ID:                 10,
		Headword:           "learn",
		NormalizedHeadword: norm.NormalizeHeadword("learn"),
		Pos:                commonmodel.POSVerb,
		EtymologyIndex:     0,
		IsMultiword:        false,
		SourceRunID:        importRun.ID,
	}
	if err := db.Omit(clause.Associations).Create(&word).Error; err != nil {
		t.Fatalf("create entry: %v", err)
	}

	createRows(t, db,
		&commonmodel.EntryLearningSignal{
			EntryID:        word.ID,
			CEFRLevel:      2,
			CEFRSource:     commonmodel.CEFRSourceOxford,
			CEFRRunID:      int64Ptr(importRun.ID),
			OxfordLevel:    1,
			OxfordRunID:    int64Ptr(importRun.ID),
			CETLevel:       1,
			CETRunID:       int64Ptr(importRun.ID),
			SchoolLevel:    2,
			FrequencyRank:  123,
			FrequencyCount: 456,
			FrequencyRunID: int64Ptr(importRun.ID),
			CollinsStars:   3,
			CollinsRunID:   int64Ptr(importRun.ID),
			UpdatedAt:      now,
		},
		&commonmodel.EntryCEFRSourceSignal{
			EntryID:    word.ID,
			CEFRSource: commonmodel.CEFRSourceCEFRJ,
			CEFRLevel:  3,
			CEFRRunID:  int64Ptr(importRun.ID),
			UpdatedAt:  now,
		},
		&commonmodel.EntrySummaryZH{
			ID:          11,
			EntryID:     word.ID,
			Source:      "manual",
			SourceRunID: importRun.ID,
			SummaryText: "学习",
			UpdatedAt:   now,
		},
		&commonmodel.EntryEtymology{
			EntryID:            word.ID,
			Source:             "wiktionary",
			SourceRunID:        importRun.ID,
			EtymologyTextRaw:   "raw etymology",
			EtymologyTextClean: &cleanEtymology,
			UpdatedAt:          now,
		},
	)

	ipaBritish := Pronunciation{
		ID:           20,
		WordID:       word.ID,
		Accent:       commonmodel.AccentBritish,
		IPA:          "lɜːn",
		IsPrimary:    true,
		DisplayOrder: 1,
	}
	ipaUS := Pronunciation{
		ID:           21,
		WordID:       word.ID,
		Accent:       commonmodel.AccentAmerican,
		IPA:          "lɝn",
		IsPrimary:    true,
		DisplayOrder: 1,
	}
	audio := PronunciationAudio{
		ID:            22,
		WordID:        word.ID,
		Accent:        commonmodel.AccentBritish,
		AudioFilename: "LL-Q1860 (eng)-Vealhurl-learn.wav",
		IsPrimary:     true,
		DisplayOrder:  1,
	}
	form := WordVariant{
		ID:              40,
		WordID:          word.ID,
		FormText:        "learnt",
		NormalizedForm:  norm.NormalizeHeadword("learnt"),
		RelationKind:    commonmodel.RelationKindForm,
		FormType:        stringPtr("past"),
		SourceRelations: pq.StringArray{"form_of"},
		DisplayOrder:    1,
	}
	createRows(t, db, &ipaBritish, &ipaUS, &audio, &form)

	sense := Sense{
		ID:         30,
		WordID:     word.ID,
		SenseOrder: 1,
	}
	if err := db.Omit(clause.Associations).Create(&sense).Error; err != nil {
		t.Fatalf("create sense: %v", err)
	}

	createRows(t, db,
		&commonmodel.SenseLearningSignal{
			SenseID:     sense.ID,
			CEFRLevel:   2,
			CEFRSource:  commonmodel.CEFRSourceOxford,
			CEFRRunID:   int64Ptr(importRun.ID),
			OxfordLevel: 1,
			OxfordRunID: int64Ptr(importRun.ID),
			UpdatedAt:   now,
		},
		&commonmodel.SenseCEFRSourceSignal{
			SenseID:    sense.ID,
			CEFRSource: commonmodel.CEFRSourceOctanove,
			CEFRLevel:  4,
			CEFRRunID:  int64Ptr(importRun.ID),
			UpdatedAt:  now,
		},
		&commonmodel.SenseGlossEN{
			ID:         31,
			SenseID:    sense.ID,
			GlossOrder: 1,
			TextEN:     "to gain knowledge",
		},
		&commonmodel.SenseGlossZH{
			ID:           32,
			SenseID:      sense.ID,
			Source:       "manual",
			SourceRunID:  importRun.ID,
			GlossOrder:   1,
			TextZHHans:   "学习知识",
			Romanization: &romanization,
			IsPrimary:    true,
		},
		&commonmodel.SenseLabel{
			ID:         33,
			SenseID:    sense.ID,
			LabelType:  commonmodel.LabelTypeRegister,
			LabelCode:  commonmodel.RegisterLabelFormal,
			LabelOrder: 1,
		},
		&Example{
			ID:           34,
			SenseID:      sense.ID,
			Source:       "wiktionary",
			ExampleOrder: 1,
			SentenceEN:   "I learn quickly.",
		},
		&commonmodel.LexicalRelation{
			ID:                   35,
			EntryID:              word.ID,
			SenseID:              int64Ptr(sense.ID),
			RelationType:         commonmodel.RelationTypeSynonym,
			TargetText:           "study",
			TargetTextNormalized: "study",
			DisplayOrder:         1,
		},
		&commonmodel.LexicalRelation{
			ID:                   36,
			EntryID:              word.ID,
			RelationType:         commonmodel.RelationTypeDerived,
			TargetText:           "learner",
			TargetTextNormalized: "learner",
			DisplayOrder:         1,
		},
	)

	if err := migration.RefreshReadModels(db); err != nil {
		t.Fatalf("refresh commons read models: %v", err)
	}

	return commonsV1Fixture{
		word:       word,
		form:       form,
		ipaBritish: ipaBritish,
		ipaUS:      ipaUS,
		audio:      audio,
		sense:      sense,
	}
}

func createRows(t *testing.T, db *gorm.DB, rows ...any) {
	t.Helper()

	for _, row := range rows {
		if err := db.Omit(clause.Associations).Create(row).Error; err != nil {
			t.Fatalf("create %T: %v", row, err)
		}
	}
}

func stringPtr(value string) *string {
	return &value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func requirePostgresIntegrationDSN(t *testing.T) string {
	t.Helper()

	dsn := strings.TrimSpace(os.Getenv(postgresIntegrationDSNEnv))
	if dsn == "" {
		t.Skip("repository PostgreSQL integration tests require TEST_POSTGRES_DSN pointing to a disposable PostgreSQL database")
	}
	if err := validatePostgresIntegrationDSNForDestructiveSetup(dsn); err != nil {
		t.Fatalf("unsafe TEST_POSTGRES_DSN for destructive repository test setup: %v", err)
	}
	return dsn
}

func validatePostgresIntegrationDSNForDestructiveSetup(dsn string) error {
	return validatePostgresIntegrationDSNForDestructiveSetupForHostPolicy(dsn, allowNonLocalPostgresTestHosts())
}

func validatePostgresIntegrationDSNForDestructiveSetupForHostPolicy(dsn string, allowNonLocal bool) error {
	host, err := postgresDSNHost(dsn)
	if err != nil {
		return err
	}
	if !allowNonLocal && !isLocalPostgresTestHost(host) {
		return fmt.Errorf("destructive PostgreSQL tests only run against localhost, loopback IPs, or unix sockets by default; got host %q (set %s=true to opt in to a disposable non-local PostgreSQL instance)", host, allowNonLocalPostgresTestsEnv)
	}
	return nil
}

func allowNonLocalPostgresTestHosts() bool {
	value := strings.TrimSpace(os.Getenv(allowNonLocalPostgresTestsEnv))
	return strings.EqualFold(value, "1") || strings.EqualFold(value, "true") || strings.EqualFold(value, "yes")
}

func postgresDSNHost(dsn string) (string, error) {
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsedURL, err := url.Parse(dsn)
		if err != nil {
			return "", fmt.Errorf("parse PostgreSQL DSN: %w", err)
		}
		if socketHost := parsedURL.Query().Get("host"); socketHost != "" {
			return socketHost, nil
		}
		return parsedURL.Hostname(), nil
	}

	for _, field := range strings.Fields(dsn) {
		parts := strings.SplitN(field, "=", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(parts[0]), "host") {
			return strings.Trim(strings.TrimSpace(parts[1]), "'\""), nil
		}
	}
	return "", nil
}

func isLocalPostgresTestHost(host string) bool {
	if host == "" {
		return true
	}
	for _, candidate := range strings.Split(host, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" || strings.HasPrefix(candidate, "/") || strings.EqualFold(candidate, "localhost") {
			continue
		}
		if ip := net.ParseIP(strings.Trim(candidate, "[]")); ip != nil && ip.IsLoopback() {
			continue
		}
		return false
	}
	return true
}

func isTransientExternalPostgresResetError(err error) bool {
	if err == nil {
		return false
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return isTransientExternalPostgresResetError(opErr.Err)
	}
	var syscallErr *os.SyscallError
	if errors.As(err, &syscallErr) {
		return errors.Is(syscallErr.Err, syscall.ECONNRESET) ||
			errors.Is(syscallErr.Err, syscall.EPIPE) ||
			errors.Is(syscallErr.Err, syscall.ECONNABORTED)
	}
	return strings.Contains(strings.ToLower(err.Error()), "connection reset by peer") ||
		strings.Contains(strings.ToLower(err.Error()), "broken pipe")
}
