package repository

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestRepository_NilDBGuard(t *testing.T) {
	repo := NewRepository(nil)
	ctx := context.Background()

	tests := []struct {
		name string
		run  func() error
	}{
		{
			name: "GetWordByHeadword",
			run: func() error {
				_, _, err := repo.GetWordByHeadword(ctx, "word", false, false, false)
				return err
			},
		},
		{
			name: "GetWordsByVariant",
			run: func() error {
				_, _, err := repo.GetWordsByVariant(ctx, "word", nil, false, false)
				return err
			},
		},
		{
			name: "GetWordsByVariants",
			run: func() error {
				_, err := repo.GetWordsByVariants(ctx, []string{"word"}, false, false, false)
				return err
			},
		},
		{
			name: "GetWordsByHeadwords",
			run: func() error {
				_, err := repo.GetWordsByHeadwords(ctx, []string{"word"}, false, false, false)
				return err
			},
		},
		{
			name: "ListFeaturedCandidateHeadwords",
			run: func() error {
				_, err := repo.ListFeaturedCandidateHeadwords(ctx)
				return err
			},
		},
		{
			name: "SearchWords",
			run: func() error {
				_, _, err := repo.SearchWords(ctx, "word", nil, nil, nil, nil, nil, nil, 10, 0)
				return err
			},
		},
		{
			name: "SuggestWords",
			run: func() error {
				_, err := repo.SuggestWords(ctx, "wor", nil, nil, nil, nil, nil, 10)
				return err
			},
		},
		{
			name: "SearchPhrases",
			run: func() error {
				_, err := repo.SearchPhrases(ctx, "word", 10)
				return err
			},
		},
		{
			name: "GetPronunciationsByWordID",
			run: func() error {
				_, err := repo.GetPronunciationsByWordID(ctx, 1, nil)
				return err
			},
		},
		{
			name: "GetSensesByWordID",
			run: func() error {
				_, err := repo.GetSensesByWordID(ctx, 1, nil)
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if !errors.Is(err, errRepositoryUninitialized) {
				t.Fatalf("expected errRepositoryUninitialized, got %v", err)
			}
			if !strings.Contains(err.Error(), "gorm DB is nil") {
				t.Fatalf("expected descriptive nil DB error, got %v", err)
			}
		})
	}
}

func TestEscapeLikePattern(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no special characters",
			input:    "hello",
			expected: "hello",
		},
		{
			name:     "percent sign",
			input:    "50%",
			expected: "50\\%",
		},
		{
			name:     "underscore",
			input:    "C++_test",
			expected: "C++\\_test",
		},
		{
			name:     "backslash",
			input:    "path\\to\\file",
			expected: "path\\\\to\\\\file",
		},
		{
			name:     "multiple wildcards",
			input:    "%_test_%",
			expected: "\\%\\_test\\_\\%",
		},
		{
			name:     "backslash before percent",
			input:    "\\%test",
			expected: "\\\\\\%test",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "real example: C++",
			input:    "C++",
			expected: "C++",
		},
		{
			name:     "real example: 100%",
			input:    "100%",
			expected: "100\\%",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeLikePattern(tt.input)
			if result != tt.expected {
				t.Errorf("escapeLikePattern(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}
