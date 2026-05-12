package repository

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newMockRepository(t *testing.T) (*Repository, sqlmock.Sqlmock) {
	t.Helper()

	sqlDB, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{Conn: sqlDB}), &gorm.Config{
		DisableAutomaticPing: true,
		Logger:               logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("open gorm db: %v", err)
	}

	t.Cleanup(func() {
		_ = sqlDB.Close()
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("sql expectations: %v", err)
		}
	})

	return &Repository{db: gormDB}, mock
}

func TestGetWordsByHeadwords_PropagatesDeadlineToDB(t *testing.T) {
	repo, mock := newMockRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	mock.ExpectQuery(`SELECT .* FROM "entries".*normalized_headword IN \(.*\)`).
		WithArgs("hello", "world").
		WillDelayFor(2 * time.Second).
		WillReturnRows(sqlmock.NewRows([]string{"id", "headword", "normalized_headword"}))

	_, err := repo.GetWordsByHeadwords(ctx, []string{"Hello", "World"}, false, false, false)
	if !errors.Is(err, sqlmock.ErrCancelled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected canceled or deadline-exceeded query error, got %v", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", ctx.Err())
	}
}

func TestListFeaturedCandidates_ReturnsQualityFilteredCandidates(t *testing.T) {
	repo, mock := newMockRepository(t)

	mock.ExpectQuery(`SELECT entry_id, headword FROM "featured_candidates"`).
		WillReturnRows(sqlmock.NewRows([]string{"entry_id", "headword"}).
			AddRow(10, "alpha").
			AddRow(20, "go after"))

	candidates, err := repo.ListFeaturedCandidates(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidates() error = %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("len(candidates) = %d, want %d", len(candidates), 2)
	}
	if candidates[0].EntryID != 10 || candidates[0].Headword != "alpha" || candidates[1].EntryID != 20 || candidates[1].Headword != "go after" {
		t.Fatalf("candidates = %#v, want exact entry ids and headwords", candidates)
	}
}

func TestListFeaturedCandidates_ReturnsEmptySliceWhenNoRows(t *testing.T) {
	repo, mock := newMockRepository(t)

	mock.ExpectQuery(`SELECT entry_id, headword FROM "featured_candidates"`).
		WillReturnRows(sqlmock.NewRows([]string{"entry_id", "headword"}))

	candidates, err := repo.ListFeaturedCandidates(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidates() error = %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("len(candidates) = %d, want %d", len(candidates), 0)
	}
	if candidates == nil {
		t.Fatal("expected empty slice, got nil")
	}
}

func TestListFeaturedCandidates_PropagatesDatabaseError(t *testing.T) {
	repo, mock := newMockRepository(t)
	wantErr := errors.New("db unavailable")

	mock.ExpectQuery(`SELECT entry_id, headword FROM "featured_candidates"`).
		WillReturnError(wantErr)

	_, err := repo.ListFeaturedCandidates(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("ListFeaturedCandidates() error = %v, want %v", err, wantErr)
	}
}

func TestSuggestWords_PropagatesDeadlineToRawQuery(t *testing.T) {
	repo, mock := newMockRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	mock.ExpectQuery(`(?s)WITH params AS .*entry_ranked AS .*FROM entry_search_terms t.*SELECT id, frequency_rank.*LIMIT \$4`).
		WithArgs("air", "air", "ais", 5).
		WillDelayFor(50 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "frequency_rank"}))

	_, err := repo.SuggestWords(ctx, "Air", SuggestOptions{Limit: 5})
	if !errors.Is(err, sqlmock.ErrCancelled) {
		t.Fatalf("expected canceled raw query error, got %v", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", ctx.Err())
	}
}
