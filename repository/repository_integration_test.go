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
	if len(sense.Labels) != 1 || len(sense.Examples) != 1 {
		t.Fatalf("sense related data not hydrated: labels=%#v examples=%#v", sense.Labels, sense.Examples)
	}
	if len(word.EntryDefinitions) != 1 || word.EntryDefinitions[0].Source != "school" || word.EntryDefinitions[0].TextZHHans != "学习" {
		t.Fatalf("entry definitions not hydrated: %#v", word.EntryDefinitions)
	}
	if len(word.EntryExamples) != 1 || word.EntryExamples[0].Source != "school" || word.EntryExamples[0].SentenceEN != "I learn at school." {
		t.Fatalf("entry examples not hydrated: %#v", word.EntryExamples)
	}
}

func TestRepository_PostgresCoreQueriesUseCommonsV1Schema(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	fixture := seedCommonsV1Fixture(t, db)

	candidates, err := repo.ListFeaturedCandidates(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidates() error = %v", err)
	}
	if len(candidates) != 1 || candidates[0].EntryID != fixture.word.ID || candidates[0].Headword != "learn" {
		t.Fatalf("candidates = %#v, want fixture learn entry", candidates)
	}

	pos := commonmodel.POSVerb
	cefr := 2
	words, total, err := repo.SearchWords(context.Background(), "lear", SearchOptions{POS: &pos, CEFRLevel: &cefr, Limit: 10})
	if err != nil {
		t.Fatalf("SearchWords() error = %v", err)
	}
	if total != 1 || len(words) != 1 || words[0].ID != fixture.word.ID {
		t.Fatalf("SearchWords() = words:%#v total:%d, want fixture", words, total)
	}

	formWords, formTotal, err := repo.SearchWords(context.Background(), "learnt", SearchOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SearchWords(form) error = %v", err)
	}
	if formTotal != 1 || len(formWords) != 1 || formWords[0].ID != fixture.word.ID {
		t.Fatalf("SearchWords(form) = words:%#v total:%d, want fixture", formWords, formTotal)
	}

	suggestions, err := repo.SuggestWords(context.Background(), "lea", SuggestOptions{Limit: 5})
	if err != nil {
		t.Fatalf("SuggestWords() error = %v", err)
	}
	if len(suggestions) != 1 || suggestions[0].Headword != "learn" {
		t.Fatalf("SuggestWords() = %#v, want learn", suggestions)
	}

	formSuggestions, err := repo.SuggestWords(context.Background(), "learnt", SuggestOptions{Limit: 5})
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

func TestRepository_PostgresHeadwordRelationGroupsUseOEWNEdges(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedHeadwordRelationGroupFixture(t, db)

	groups, err := repo.GetHeadwordRelationGroups(context.Background(), "Head", commonmodel.HeadwordRelationPOSCodeNoun, RelationQueryOptions{})
	if err != nil {
		t.Fatalf("GetHeadwordRelationGroups() error = %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("groups len = %d, want 2: %#v", len(groups), groups)
	}
	if groups[0].RelationType != commonmodel.RelationTypeSynonym {
		t.Fatalf("first relation type = %q, want synonym", groups[0].RelationType)
	}
	if groups[1].RelationType != commonmodel.RelationTypeAntonym {
		t.Fatalf("second relation type = %q, want antonym", groups[1].RelationType)
	}

	synonyms := groups[0].Items
	if got, want := relationItemTargets(synonyms), []string{"chief", "leader", "top", "captain", "director"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("synonym targets = %v, want %v", got, want)
	}
	if synonyms[0].TargetHeadword != "Chief" {
		t.Fatalf("local target headword display = %q, want Chief", synonyms[0].TargetHeadword)
	}
	if synonyms[2].EvidenceCount != 2 {
		t.Fatalf("top evidence_count = %d, want 2", synonyms[2].EvidenceCount)
	}
	for _, item := range synonyms {
		if !item.HasTargetEntry {
			t.Fatalf("default relation query returned missing target entry: %#v", item)
		}
		if item.TargetPOSCode != commonmodel.HeadwordRelationPOSCodeNoun {
			t.Fatalf("target POS code = %d, want noun", item.TargetPOSCode)
		}
	}
	assertRelationTargetAbsent(t, groups, "caput")
	assertRelationTargetAbsent(t, groups, "head")
	assertRelationTypeAbsent(t, groups, commonmodel.RelationTypeDerivation)
	assertRelationTypeAbsent(t, groups, commonmodel.RelationTypeEvent)
	assertRelationTypeAbsent(t, groups, commonmodel.RelationTypeAgent)

	limited, err := repo.GetHeadwordRelationGroups(context.Background(), norm.NormalizeHeadword("head"), commonmodel.HeadwordRelationPOSCodeNoun, RelationQueryOptions{LimitPerRelationType: 2})
	if err != nil {
		t.Fatalf("GetHeadwordRelationGroups(limit) error = %v", err)
	}
	if len(limited) == 0 || len(limited[0].Items) != 2 {
		t.Fatalf("limited synonym item count = %#v, want 2 items", limited)
	}

	withMissing, err := repo.GetHeadwordRelationGroups(context.Background(), norm.NormalizeHeadword("head"), commonmodel.HeadwordRelationPOSCodeNoun, RelationQueryOptions{IncludeMissingTargets: true})
	if err != nil {
		t.Fatalf("GetHeadwordRelationGroups(include missing) error = %v", err)
	}
	missing := findRelationTarget(withMissing, "caput")
	if missing == nil || missing.HasTargetEntry {
		t.Fatalf("expected caput missing target when IncludeMissingTargets=true, got %#v", missing)
	}

	withSelf, err := repo.GetHeadwordRelationGroups(context.Background(), norm.NormalizeHeadword("head"), commonmodel.HeadwordRelationPOSCodeNoun, RelationQueryOptions{IncludeSelfTargets: true})
	if err != nil {
		t.Fatalf("GetHeadwordRelationGroups(include self) error = %v", err)
	}
	self := findRelationTarget(withSelf, "head")
	if self == nil || !self.HasTargetEntry {
		t.Fatalf("expected self target when IncludeSelfTargets=true, got %#v", self)
	}
}

func TestRepository_PostgresEntryGroupByHeadwordReturnsAllPOS(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedHeadwordRelationGroupFixture(t, db)

	words, variant, err := repo.GetEntryGroupByHeadword(context.Background(), "head", false, false, false)
	if err != nil {
		t.Fatalf("GetEntryGroupByHeadword(head) error = %v", err)
	}
	if variant != nil {
		t.Fatalf("direct headword group returned queried variant: %#v", variant)
	}
	if got, want := entryGroupPOS(words), []string{commonmodel.POSNoun, commonmodel.POSVerb, commonmodel.POSAdjective}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("entry group POS = %v, want %v", got, want)
	}

	words, variant, err = repo.GetEntryGroupByHeadword(context.Background(), "headed", false, false, false)
	if err != nil {
		t.Fatalf("GetEntryGroupByHeadword(variant) error = %v", err)
	}
	if variant == nil || variant.FormText != "headed" {
		t.Fatalf("variant metadata = %#v, want headed", variant)
	}
	if got, want := entryGroupPOS(words), []string{commonmodel.POSNoun, commonmodel.POSVerb, commonmodel.POSAdjective}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("variant entry group POS = %v, want %v", got, want)
	}
}

func TestRepository_PostgresGetWordsByHeadwordsUsesStableEntryOrder(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedHeadwordRelationGroupFixture(t, db)

	words, err := repo.GetWordsByHeadwords(context.Background(), []string{"Head"}, false, false, false)
	if err != nil {
		t.Fatalf("GetWordsByHeadwords() error = %v", err)
	}
	if got, want := entryGroupPOS(words), []string{commonmodel.POSNoun, commonmodel.POSVerb, commonmodel.POSAdjective}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("GetWordsByHeadwords POS = %v, want %v", got, want)
	}
}

func TestRepository_PostgresGetWordsByHeadwordsRanksQualityBeforePOS(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsDuplicateHeadwordFixture(t, db)

	words, err := repo.GetWordsByHeadwords(context.Background(), []string{"move"}, false, false, false)
	if err != nil {
		t.Fatalf("GetWordsByHeadwords(move) error = %v", err)
	}
	if len(words) < 2 {
		t.Fatalf("GetWordsByHeadwords(move) returned %d words, want multiple", len(words))
	}
	if words[0].ID != 101 || words[0].Pos != commonmodel.POSVerb {
		t.Fatalf("first move entry = id:%d pos:%s, want quality-ranked verb id 101", words[0].ID, words[0].Pos)
	}
}

func TestRepository_PostgresSuggestWordsDeduplicatesCanonicalHeadwords(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsDuplicateHeadwordFixture(t, db)

	suggestions, err := repo.SuggestWords(context.Background(), "mov", SuggestOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(mov) error = %v", err)
	}
	assertUniqueSuggestionHeadwords(t, suggestions)
	assertSuggestionHeadwordCount(t, suggestions, "move", 1)
	assertSuggestionHeadwordCount(t, suggestions, "moving", 1)
	assertSuggestionHeadwordCount(t, suggestions, "moved", 1)
	assertSuggestionRepresentative(t, suggestions, "move", 101)
	assertSuggestionRepresentative(t, suggestions, "moving", 103)
	assertSuggestionRepresentative(t, suggestions, "moved", 106)

	limited, err := repo.SuggestWords(context.Background(), "mov", SuggestOptions{Limit: 3})
	if err != nil {
		t.Fatalf("SuggestWords(mov, limit 3) error = %v", err)
	}
	if got, want := strings.Join(suggestionHeadwords(limited), ","), "move,moving,moved"; got != want {
		t.Fatalf("SuggestWords(mov, limit 3) headwords = %s, want %s", got, want)
	}

	fullLimit, err := repo.SuggestWords(context.Background(), "mov", SuggestOptions{Limit: 5})
	if err != nil {
		t.Fatalf("SuggestWords(mov, limit 5) error = %v", err)
	}
	if got, want := strings.Join(suggestionHeadwords(fullLimit), ","), "move,moving,moved,mover,motion"; got != want {
		t.Fatalf("SuggestWords(mov, limit 5) headwords = %s, want %s", got, want)
	}

	aliasSuggestions, err := repo.SuggestWords(context.Background(), "movt", SuggestOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(alias) error = %v", err)
	}
	if len(aliasSuggestions) != 1 || aliasSuggestions[0].Headword != "motion" {
		t.Fatalf("SuggestWords(alias) = %#v, want canonical headword motion", aliasSuggestions)
	}
}

func TestRepository_PostgresSuggestWordsRanksHeadwordPrefixesBeforeFormPrefixes(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsFixture(t, db, 200, []suggestWordFixtureEntry{
		{ID: 201, Headword: "when", Pos: commonmodel.POSAdverb, FrequencyRank: 78},
		{ID: 202, Headword: "die", Pos: commonmodel.POSVerb, FrequencyRank: 358},
		{ID: 203, Headword: "dick", Pos: commonmodel.POSNoun, FrequencyRank: 1000},
		{ID: 204, Headword: "dice", Pos: commonmodel.POSNoun, FrequencyRank: 2000},
		{ID: 205, Headword: "dickhead", Pos: commonmodel.POSNoun, FrequencyRank: 3000},
		{ID: 206, Headword: "dicks", Pos: commonmodel.POSNoun, FrequencyRank: 4000},
		{ID: 207, Headword: "dictionary", Pos: commonmodel.POSNoun, FrequencyRank: 5000},
		{ID: 208, Headword: "dictate", Pos: commonmodel.POSVerb, FrequencyRank: 6000},
	}, []suggestWordFixtureForm{
		{ID: 2001, WordID: 201, FormText: "dices", RelationKind: commonmodel.RelationKindForm},
		{ID: 2002, WordID: 202, FormText: "dice", RelationKind: commonmodel.RelationKindForm},
	})

	suggestions, err := repo.SuggestWords(context.Background(), "dic", SuggestOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(dic) error = %v", err)
	}

	got := suggestionHeadwords(suggestions)
	wantPrefix := []string{"dick", "dice", "dickhead", "dicks", "dictionary", "dictate"}
	if len(got) < len(wantPrefix) {
		t.Fatalf("SuggestWords(dic) headwords = %s, want at least prefix %s", strings.Join(got, ","), strings.Join(wantPrefix, ","))
	}
	for i, want := range wantPrefix {
		if got[i] != want {
			t.Fatalf("SuggestWords(dic) headwords = %s, want prefix %s", strings.Join(got, ","), strings.Join(wantPrefix, ","))
		}
	}
	if got[0] == "when" || got[0] == "die" {
		t.Fatalf("SuggestWords(dic) started with form-only match %q in %s", got[0], strings.Join(got, ","))
	}
}

func TestRepository_PostgresSuggestWordsKeepsExactFormMatches(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsFixture(t, db, 300, []suggestWordFixtureEntry{
		{ID: 301, Headword: "learn", Pos: commonmodel.POSVerb, FrequencyRank: 700},
	}, []suggestWordFixtureForm{
		{ID: 3001, WordID: 301, FormText: "learnt", RelationKind: commonmodel.RelationKindForm},
	})

	suggestions, err := repo.SuggestWords(context.Background(), "learnt", SuggestOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(learnt) error = %v", err)
	}
	if len(suggestions) != 1 || suggestions[0].Headword != "learn" {
		t.Fatalf("SuggestWords(learnt) = %#v, want learn", suggestions)
	}
}

func TestRepository_PostgresSuggestWordsRanksExactHeadwordBeforeExactForm(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsFixture(t, db, 400, []suggestWordFixtureEntry{
		{ID: 401, Headword: "die", Pos: commonmodel.POSVerb, FrequencyRank: 358},
		{ID: 402, Headword: "dice", Pos: commonmodel.POSNoun, FrequencyRank: 5000},
	}, []suggestWordFixtureForm{
		{ID: 4001, WordID: 401, FormText: "dice", RelationKind: commonmodel.RelationKindForm},
	})

	suggestions, err := repo.SuggestWords(context.Background(), "dice", SuggestOptions{Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(dice) error = %v", err)
	}
	if got, want := strings.Join(suggestionHeadwords(suggestions), ","), "dice,die"; got != want {
		t.Fatalf("SuggestWords(dice) headwords = %s, want %s", got, want)
	}
}

func TestRepository_PostgresSearchAndSuggestUseSchoolLevel(t *testing.T) {
	repo, db := newPostgresIntegrationRepository(t)
	seedSuggestWordsFixture(t, db, 500, []suggestWordFixtureEntry{
		{ID: 501, Headword: "scan", Pos: commonmodel.POSVerb, FrequencyRank: 100, SchoolLevel: commonmodel.SchoolLevelUniversity},
		{ID: 502, Headword: "scaffold", Pos: commonmodel.POSNoun, SchoolLevel: commonmodel.SchoolLevelMiddleSchool},
		{ID: 503, Headword: "scala", Pos: commonmodel.POSNoun, SchoolLevel: commonmodel.SchoolLevelHighSchool},
		{ID: 504, Headword: "scarce", Pos: commonmodel.POSAdjective},
	}, nil)

	schoolLevel := int(commonmodel.SchoolLevelMiddleSchool)
	filtered, total, err := repo.SearchWords(context.Background(), "sca", SearchOptions{SchoolLevel: &schoolLevel, Limit: 10})
	if err != nil {
		t.Fatalf("SearchWords(school filter) error = %v", err)
	}
	if total != 1 || len(filtered) != 1 || filtered[0].Headword != "scaffold" {
		t.Fatalf("SearchWords(school filter) = words:%#v total:%d, want scaffold", filtered, total)
	}

	words, _, err := repo.SearchWords(context.Background(), "sca", SearchOptions{Limit: 3})
	if err != nil {
		t.Fatalf("SearchWords(school order) error = %v", err)
	}
	if got, want := strings.Join(suggestionHeadwords(words), ","), "scan,scaffold,scala"; got != want {
		t.Fatalf("SearchWords(school order) headwords = %s, want %s", got, want)
	}

	suggestions, err := repo.SuggestWords(context.Background(), "sca", SuggestOptions{Limit: 3})
	if err != nil {
		t.Fatalf("SuggestWords(school order) error = %v", err)
	}
	if got, want := strings.Join(suggestionHeadwords(suggestions), ","), "scan,scaffold,scala"; got != want {
		t.Fatalf("SuggestWords(school order) headwords = %s, want %s", got, want)
	}

	filteredSuggestions, err := repo.SuggestWords(context.Background(), "sca", SuggestOptions{SchoolLevel: &schoolLevel, Limit: 10})
	if err != nil {
		t.Fatalf("SuggestWords(school filter) error = %v", err)
	}
	if len(filteredSuggestions) != 1 || filteredSuggestions[0].Headword != "scaffold" {
		t.Fatalf("SuggestWords(school filter) = %#v, want scaffold", filteredSuggestions)
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
	allowFixtureIdentityOverrides(t, db)

	return &Repository{db: db}, db
}

func allowFixtureIdentityOverrides(t *testing.T, db *gorm.DB) {
	t.Helper()

	tables := []string{
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
		"entry_search_terms",
	}
	for _, table := range tables {
		if err := db.Exec(fmt.Sprintf("ALTER TABLE %s ALTER COLUMN id SET GENERATED BY DEFAULT", table)).Error; err != nil {
			t.Fatalf("allow fixture identity overrides for %s: %v", table, err)
		}
	}
}

func seedSuggestWordsDuplicateHeadwordFixture(t *testing.T, db *gorm.DB) {
	t.Helper()

	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	importRun := commonmodel.ImportRun{
		ID:              100,
		SourceName:      "wiktionary",
		SourcePath:      "/data/suggest.xml",
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	createRows(t, db, &importRun)

	entries := []Word{
		{ID: 101, Headword: "move", NormalizedHeadword: norm.NormalizeHeadword("move"), Pos: commonmodel.POSVerb, SourceRunID: importRun.ID},
		{ID: 102, Headword: "move", NormalizedHeadword: norm.NormalizeHeadword("move"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 103, Headword: "moving", NormalizedHeadword: norm.NormalizeHeadword("moving"), Pos: commonmodel.POSAdjective, SourceRunID: importRun.ID},
		{ID: 104, Headword: "moving", NormalizedHeadword: norm.NormalizeHeadword("moving"), Pos: commonmodel.POSVerb, SourceRunID: importRun.ID},
		{ID: 105, Headword: "moving", NormalizedHeadword: norm.NormalizeHeadword("moving"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 106, Headword: "moved", NormalizedHeadword: norm.NormalizeHeadword("moved"), Pos: commonmodel.POSAdjective, SourceRunID: importRun.ID},
		{ID: 107, Headword: "moved", NormalizedHeadword: norm.NormalizeHeadword("moved"), Pos: commonmodel.POSVerb, SourceRunID: importRun.ID},
		{ID: 108, Headword: "motion", NormalizedHeadword: norm.NormalizeHeadword("motion"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 109, Headword: "mover", NormalizedHeadword: norm.NormalizeHeadword("mover"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
	}
	for i := range entries {
		createRows(t, db, &entries[i])
	}

	signals := []commonmodel.EntryLearningSignal{
		{EntryID: 101, FrequencyRank: 10, UpdatedAt: now},
		{EntryID: 102, FrequencyRank: 20, UpdatedAt: now},
		{EntryID: 103, FrequencyRank: 30, UpdatedAt: now},
		{EntryID: 104, FrequencyRank: 31, UpdatedAt: now},
		{EntryID: 105, FrequencyRank: 32, UpdatedAt: now},
		{EntryID: 106, FrequencyRank: 40, UpdatedAt: now},
		{EntryID: 107, FrequencyRank: 41, UpdatedAt: now},
		{EntryID: 108, FrequencyRank: 60, UpdatedAt: now},
	}
	for i := range signals {
		createRows(t, db, &signals[i])
	}

	alias := WordVariant{
		ID:              1000,
		WordID:          108,
		FormText:        "movt",
		NormalizedForm:  norm.NormalizeHeadword("movt"),
		RelationKind:    commonmodel.RelationKindAlias,
		SourceRelations: pq.StringArray{"alias"},
		DisplayOrder:    1,
	}
	createRows(t, db, &alias)

	if err := migration.RefreshReadModels(db); err != nil {
		t.Fatalf("refresh suggest read models: %v", err)
	}
}

type suggestWordFixtureEntry struct {
	ID            int64
	Headword      string
	Pos           string
	FrequencyRank int
	CEFRLevel     int16
	OxfordLevel   int16
	CETLevel      int16
	CollinsStars  int16
	SchoolLevel   int16
}

type suggestWordFixtureForm struct {
	ID           int64
	WordID       int64
	FormText     string
	RelationKind string
}

func seedSuggestWordsFixture(t *testing.T, db *gorm.DB, runID int64, entries []suggestWordFixtureEntry, forms []suggestWordFixtureForm) {
	t.Helper()

	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	importRun := commonmodel.ImportRun{
		ID:              runID,
		SourceName:      "wiktionary",
		SourcePath:      fmt.Sprintf("/data/suggest-%d.xml", runID),
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	createRows(t, db, &importRun)

	for _, spec := range entries {
		entry := Word{
			ID:                 spec.ID,
			Headword:           spec.Headword,
			NormalizedHeadword: norm.NormalizeHeadword(spec.Headword),
			Pos:                spec.Pos,
			SourceRunID:        importRun.ID,
		}
		createRows(t, db, &entry)
		createRows(t, db, &commonmodel.EntryLearningSignal{
			EntryID:       spec.ID,
			FrequencyRank: spec.FrequencyRank,
			CEFRLevel:     spec.CEFRLevel,
			OxfordLevel:   spec.OxfordLevel,
			CETLevel:      spec.CETLevel,
			CollinsStars:  spec.CollinsStars,
			SchoolLevel:   spec.SchoolLevel,
			UpdatedAt:     now,
		})
	}

	for _, spec := range forms {
		sourceRelations := pq.StringArray{"form_of"}
		if spec.RelationKind == commonmodel.RelationKindAlias {
			sourceRelations = pq.StringArray{"alias"}
		}
		form := WordVariant{
			ID:              spec.ID,
			WordID:          spec.WordID,
			FormText:        spec.FormText,
			NormalizedForm:  norm.NormalizeHeadword(spec.FormText),
			RelationKind:    spec.RelationKind,
			SourceRelations: sourceRelations,
			DisplayOrder:    1,
		}
		createRows(t, db, &form)
	}

	if err := migration.RefreshReadModels(db); err != nil {
		t.Fatalf("refresh suggest read models: %v", err)
	}
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
	oewnRun := commonmodel.ImportRun{
		ID:              2,
		SourceName:      "oewn",
		SourcePath:      "/data/english-wordnet-2025-json",
		SourceDumpID:    "2025",
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	if err := db.Omit(clause.Associations).Create(&oewnRun).Error; err != nil {
		t.Fatalf("create OEWN import run: %v", err)
	}
	schoolRun := commonmodel.ImportRun{
		ID:              3,
		SourceName:      "school",
		SourcePath:      "/data/school.json",
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	if err := db.Omit(clause.Associations).Create(&schoolRun).Error; err != nil {
		t.Fatalf("create school import run: %v", err)
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
			SchoolRunID:    int64Ptr(schoolRun.ID),
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
		&commonmodel.HeadwordRelationEdge{
			ID:                       35,
			SourceHeadword:           "learn",
			SourceHeadwordNormalized: norm.NormalizeHeadword("learn"),
			SourcePOSCode:            commonmodel.HeadwordRelationPOSCodeVerb,
			RelationType:             commonmodel.RelationTypeSynonym,
			TargetHeadword:           "study",
			TargetHeadwordNormalized: norm.NormalizeHeadword("study"),
			TargetPOSCode:            commonmodel.HeadwordRelationPOSCodeVerb,
			SourceRelationType:       commonmodel.OEWNSourceRelationMembers,
			SourceSynsetID:           "oewn-001-v",
			TargetSynsetID:           "oewn-001-v",
			ImportRunID:              oewnRun.ID,
		},
	)

	senseID := sense.ID
	definitionEN := "to gain knowledge"
	exampleZH := "我在学校学习。"
	createRows(t, db,
		&commonmodel.EntryDefinition{
			ID:                  36,
			EntryID:             word.ID,
			SenseID:             &senseID,
			POS:                 commonmodel.POSVerb,
			Source:              "school",
			SourceRunID:         schoolRun.ID,
			DefinitionOrder:     1,
			TextZHHans:          "学习",
			TextEN:              &definitionEN,
			NormalizedZHHansKey: "学习",
			NormalizedENKey:     "to gain knowledge",
			UpdatedAt:           now,
		},
		&commonmodel.EntryExample{
			ID:                      37,
			EntryID:                 word.ID,
			SenseID:                 &senseID,
			Source:                  "school",
			SourceRunID:             schoolRun.ID,
			ExampleOrder:            1,
			SentenceEN:              "I learn at school.",
			SentenceZHHans:          &exampleZH,
			NormalizedSentenceENKey: "i learn at school",
			NormalizedSentenceZHKey: "我在学校学习",
			UpdatedAt:               now,
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

func seedHeadwordRelationGroupFixture(t *testing.T, db *gorm.DB) {
	t.Helper()

	now := time.Date(2026, 5, 6, 12, 0, 0, 0, time.UTC)
	importRun := commonmodel.ImportRun{
		ID:              900,
		SourceName:      "wiktionary",
		SourcePath:      "/data/head.xml",
		PipelineVersion: "v1",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	oewnRun := commonmodel.ImportRun{
		ID:              901,
		SourceName:      "oewn",
		SourcePath:      "/data/english-wordnet-2025-json",
		SourceDumpID:    "2025",
		PipelineVersion: "v1.0.0",
		Status:          commonmodel.ImportRunStatusCompleted,
		StartedAt:       now,
		FinishedAt:      &now,
	}
	createRows(t, db, &importRun, &oewnRun)

	entries := []Word{
		{ID: 1001, Headword: "head", NormalizedHeadword: norm.NormalizeHeadword("head"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1002, Headword: "head", NormalizedHeadword: norm.NormalizeHeadword("head"), Pos: commonmodel.POSVerb, SourceRunID: importRun.ID},
		{ID: 1003, Headword: "head", NormalizedHeadword: norm.NormalizeHeadword("head"), Pos: commonmodel.POSAdjective, SourceRunID: importRun.ID},
		{ID: 1010, Headword: "Chief", NormalizedHeadword: norm.NormalizeHeadword("chief"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1011, Headword: "leader", NormalizedHeadword: norm.NormalizeHeadword("leader"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1012, Headword: "top", NormalizedHeadword: norm.NormalizeHeadword("top"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1013, Headword: "tail", NormalizedHeadword: norm.NormalizeHeadword("tail"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1014, Headword: "heading", NormalizedHeadword: norm.NormalizeHeadword("heading"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1015, Headword: "ceremony", NormalizedHeadword: norm.NormalizeHeadword("ceremony"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1016, Headword: "chairperson", NormalizedHeadword: norm.NormalizeHeadword("chairperson"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1017, Headword: "captain", NormalizedHeadword: norm.NormalizeHeadword("captain"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
		{ID: 1018, Headword: "director", NormalizedHeadword: norm.NormalizeHeadword("director"), Pos: commonmodel.POSNoun, SourceRunID: importRun.ID},
	}
	for i := range entries {
		createRows(t, db, &entries[i])
	}

	signals := []commonmodel.EntryLearningSignal{
		{EntryID: 1001, FrequencyRank: 10, CEFRLevel: 2, UpdatedAt: now},
		{EntryID: 1002, FrequencyRank: 20, CEFRLevel: 1, UpdatedAt: now},
		{EntryID: 1003, FrequencyRank: 30, UpdatedAt: now},
		{EntryID: 1010, FrequencyRank: 5, OxfordLevel: 1, UpdatedAt: now},
		{EntryID: 1011, FrequencyRank: 50, CETLevel: 1, UpdatedAt: now},
		{EntryID: 1012, FrequencyRank: 100, CEFRLevel: 4, CollinsStars: 4, UpdatedAt: now},
		{EntryID: 1013, FrequencyRank: 30, UpdatedAt: now},
		{EntryID: 1014, FrequencyRank: 200, UpdatedAt: now},
		{EntryID: 1015, FrequencyRank: 300, UpdatedAt: now},
		{EntryID: 1016, FrequencyRank: 400, UpdatedAt: now},
		{EntryID: 1017, SchoolLevel: commonmodel.SchoolLevelMiddleSchool, UpdatedAt: now},
		{EntryID: 1018, SchoolLevel: commonmodel.SchoolLevelUniversity, UpdatedAt: now},
	}
	for i := range signals {
		createRows(t, db, &signals[i])
	}

	createRows(t, db, &WordVariant{
		ID:              1020,
		WordID:          1002,
		FormText:        "heads",
		NormalizedForm:  norm.NormalizeHeadword("heads"),
		RelationKind:    commonmodel.RelationKindForm,
		FormType:        stringPtr("third_person_singular"),
		SourceRelations: pq.StringArray{"form_of"},
		DisplayOrder:    1,
	})

	edges := []commonmodel.HeadwordRelationEdge{
		relationEdge(1100, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "chief", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-1", "oewn-chief-n-1"),
		relationEdge(1101, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "leader", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-2", "oewn-leader-n-1"),
		relationEdge(1102, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "top", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-3", "oewn-top-n-1"),
		relationEdge(1103, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "top", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-4", "oewn-top-n-2"),
		relationEdge(1104, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "caput", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-5", "oewn-caput-n-1"),
		relationEdge(1105, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeAntonym, "tail", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationAntonym, "oewn-head-n-6", "oewn-tail-n-1"),
		relationEdge(1106, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeDerivation, "heading", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationDerivation, "oewn-head-n-7", "oewn-heading-n-1"),
		relationEdge(1107, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeEvent, "ceremony", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationEvent, "oewn-head-n-8", "oewn-ceremony-n-1"),
		relationEdge(1108, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeAgent, "chairperson", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationAgent, "oewn-head-n-9", "oewn-chairperson-n-1"),
		relationEdge(1109, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "captain", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-10", "oewn-captain-n-1"),
		relationEdge(1110, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "director", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-11", "oewn-director-n-1"),
		relationEdge(1111, oewnRun.ID, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.RelationTypeSynonym, "head", commonmodel.HeadwordRelationPOSCodeNoun, commonmodel.OEWNSourceRelationMembers, "oewn-head-n-12", "oewn-head-n-13"),
	}
	for i := range edges {
		createRows(t, db, &edges[i])
	}
}

func relationEdge(id, importRunID int64, sourceHeadword string, sourcePOSCode int, relationType, targetHeadword string, targetPOSCode int, sourceRelationType, sourceSynsetID, targetSynsetID string) commonmodel.HeadwordRelationEdge {
	return commonmodel.HeadwordRelationEdge{
		ID:                       id,
		SourceHeadword:           sourceHeadword,
		SourceHeadwordNormalized: norm.NormalizeHeadword(sourceHeadword),
		SourcePOSCode:            sourcePOSCode,
		RelationType:             relationType,
		TargetHeadword:           targetHeadword,
		TargetHeadwordNormalized: norm.NormalizeHeadword(targetHeadword),
		TargetPOSCode:            targetPOSCode,
		SourceRelationType:       sourceRelationType,
		SourceSynsetID:           sourceSynsetID,
		TargetSynsetID:           targetSynsetID,
		ImportRunID:              importRunID,
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

func relationItemTargets(items []HeadwordRelationItem) []string {
	targets := make([]string, len(items))
	for i, item := range items {
		targets[i] = item.TargetHeadwordNormalized
	}
	return targets
}

func assertRelationTypeAbsent(t *testing.T, groups []HeadwordRelationGroup, relationType string) {
	t.Helper()

	for _, group := range groups {
		if group.RelationType == relationType {
			t.Fatalf("relation type %q unexpectedly present in %#v", relationType, groups)
		}
	}
}

func assertRelationTargetAbsent(t *testing.T, groups []HeadwordRelationGroup, target string) {
	t.Helper()

	if item := findRelationTarget(groups, target); item != nil {
		t.Fatalf("relation target %q unexpectedly present: %#v", target, item)
	}
}

func findRelationTarget(groups []HeadwordRelationGroup, target string) *HeadwordRelationItem {
	normalizedTarget := norm.NormalizeHeadword(target)
	for _, group := range groups {
		for _, item := range group.Items {
			if item.TargetHeadwordNormalized == normalizedTarget {
				found := item
				return &found
			}
		}
	}
	return nil
}

func entryGroupPOS(words []Word) []string {
	pos := make([]string, len(words))
	for i, word := range words {
		pos[i] = word.Pos
	}
	return pos
}

func suggestionHeadwords(words []Word) []string {
	headwords := make([]string, len(words))
	for i, word := range words {
		headwords[i] = word.Headword
	}
	return headwords
}

func assertUniqueSuggestionHeadwords(t *testing.T, words []Word) {
	t.Helper()

	seen := make(map[string]struct{}, len(words))
	for _, word := range words {
		key := strings.ToLower(strings.TrimSpace(word.Headword))
		if _, exists := seen[key]; exists {
			t.Fatalf("SuggestWords returned duplicate headword %q in %#v", word.Headword, words)
		}
		seen[key] = struct{}{}
	}
}

func assertSuggestionHeadwordCount(t *testing.T, words []Word, headword string, want int) {
	t.Helper()

	got := 0
	for _, word := range words {
		if word.Headword == headword {
			got++
		}
	}
	if got != want {
		t.Fatalf("headword %q count = %d in %#v, want %d", headword, got, words, want)
	}
}

func assertSuggestionRepresentative(t *testing.T, words []Word, headword string, wantID int64) {
	t.Helper()

	for _, word := range words {
		if word.Headword == headword {
			if word.ID != wantID {
				t.Fatalf("headword %q representative ID = %d, want %d", headword, word.ID, wantID)
			}
			return
		}
	}
	t.Fatalf("headword %q missing from %#v", headword, words)
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
