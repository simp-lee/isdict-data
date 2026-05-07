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

	mock.ExpectQuery(`SELECT \* FROM "entries" WHERE normalized_headword IN \(.*\)`).
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

func TestListFeaturedCandidateHeadwords_ReturnsQualityFilteredHeadwords(t *testing.T) {
	repo, mock := newMockRepository(t)

	mock.ExpectQuery(`SELECT .*headword.* FROM "featured_candidates"`).
		WillReturnRows(sqlmock.NewRows([]string{"headword"}).
			AddRow("alpha").
			AddRow("go after"))

	headwords, err := repo.ListFeaturedCandidateHeadwords(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidateHeadwords() error = %v", err)
	}
	if len(headwords) != 2 {
		t.Fatalf("len(headwords) = %d, want %d", len(headwords), 2)
	}
	if headwords[0] != "alpha" || headwords[1] != "go after" {
		t.Fatalf("headwords = %#v, want %#v", headwords, []string{"alpha", "go after"})
	}
}

func TestListFeaturedCandidateHeadwords_ReturnsEmptySliceWhenNoRows(t *testing.T) {
	repo, mock := newMockRepository(t)

	mock.ExpectQuery(`SELECT .*headword.* FROM "featured_candidates"`).
		WillReturnRows(sqlmock.NewRows([]string{"headword"}))

	headwords, err := repo.ListFeaturedCandidateHeadwords(context.Background())
	if err != nil {
		t.Fatalf("ListFeaturedCandidateHeadwords() error = %v", err)
	}
	if len(headwords) != 0 {
		t.Fatalf("len(headwords) = %d, want %d", len(headwords), 0)
	}
	if headwords == nil {
		t.Fatal("expected empty slice, got nil")
	}
}

func TestListFeaturedCandidateHeadwords_PropagatesDatabaseError(t *testing.T) {
	repo, mock := newMockRepository(t)
	wantErr := errors.New("db unavailable")

	mock.ExpectQuery(`SELECT .*headword.* FROM "featured_candidates"`).
		WillReturnError(wantErr)

	_, err := repo.ListFeaturedCandidateHeadwords(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf("ListFeaturedCandidateHeadwords() error = %v, want %v", err, wantErr)
	}
}

func TestSuggestWords_PropagatesDeadlineToRawQuery(t *testing.T) {
	repo, mock := newMockRepository(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	mock.ExpectQuery(`(?s)WITH entry_ranked AS .*FROM entry_search_terms t.*SELECT id, frequency_rank.*LIMIT \$3`).
		WithArgs("air", "ais", 5).
		WillDelayFor(50 * time.Millisecond).
		WillReturnRows(sqlmock.NewRows([]string{"id", "frequency_rank"}))

	_, err := repo.SuggestWords(ctx, "Air", nil, nil, nil, nil, nil, 5)
	if !errors.Is(err, sqlmock.ErrCancelled) {
		t.Fatalf("expected canceled raw query error, got %v", err)
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("expected context deadline exceeded, got %v", ctx.Err())
	}
}
