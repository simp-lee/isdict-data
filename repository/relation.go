package repository

import (
	"context"
	"strings"

	"github.com/lib/pq"
	"github.com/simp-lee/isdict-commons/model"
	"github.com/simp-lee/isdict-commons/norm"
)

const defaultRelationLimitPerType = 10

var defaultHeadwordRelationTypes = []string{
	model.RelationTypeSynonym,
	model.RelationTypeAntonym,
	model.RelationTypeHypernym,
	model.RelationTypeHyponym,
	model.RelationTypeMeronym,
	model.RelationTypeHolonym,
	model.RelationTypeSimilarTo,
	model.RelationTypeAlsoSee,
}

// RelationQueryOptions controls headword relation group hydration.
type RelationQueryOptions struct {
	RelationTypes         []string
	LimitPerRelationType  int
	IncludeMissingTargets bool
	IncludeSelfTargets    bool
}

// HeadwordRelationGroup is the repository representation of one relation type.
type HeadwordRelationGroup struct {
	RelationType string
	Items        []HeadwordRelationItem
}

// HeadwordRelationItem is a deduplicated target headword with OEWN evidence.
type HeadwordRelationItem struct {
	TargetHeadword           string
	TargetHeadwordNormalized string
	TargetPOSCode            int
	SourceRelationType       string
	SourceSynsetID           string
	TargetSynsetID           string
	SourceSenseID            string
	TargetSenseID            string
	EvidenceCount            int
	HasTargetEntry           bool
}

type headwordRelationGroupRow struct {
	RelationType             string
	RelationOrder            int
	TargetHeadword           string
	TargetHeadwordNormalized string
	TargetPOSCode            int
	SourceRelationType       string
	SourceSynsetID           string
	TargetSynsetID           string
	SourceSenseID            string
	TargetSenseID            string
	EvidenceCount            int
	HasTargetEntry           bool
}

// DefaultHeadwordRelationTypes returns the default word-page relation allowlist
// in display order.
func DefaultHeadwordRelationTypes() []string {
	return append([]string(nil), defaultHeadwordRelationTypes...)
}

// GetHeadwordRelationGroups reads OEWN-derived headword/POS relation groups.
func (r *Repository) GetHeadwordRelationGroups(ctx context.Context, headword string, posCode int, opts RelationQueryOptions) ([]HeadwordRelationGroup, error) {
	db, err := r.dbWithContext(ctx)
	if err != nil {
		return nil, err
	}
	normalizedHeadword := norm.NormalizeHeadword(headword)
	if normalizedHeadword == "" || !isSupportedHeadwordRelationPOSCode(posCode) {
		return []HeadwordRelationGroup{}, nil
	}

	opts = normalizeRelationQueryOptions(opts)
	if len(opts.RelationTypes) == 0 {
		return []HeadwordRelationGroup{}, nil
	}

	var rows []headwordRelationGroupRow
	if err := db.Raw(headwordRelationGroupsSQL, pq.Array(opts.RelationTypes), normalizedHeadword, posCode, opts.IncludeSelfTargets, opts.IncludeMissingTargets, opts.LimitPerRelationType).Scan(&rows).Error; err != nil {
		return nil, err
	}

	return buildHeadwordRelationGroups(rows), nil
}

func normalizeRelationQueryOptions(opts RelationQueryOptions) RelationQueryOptions {
	if len(opts.RelationTypes) == 0 {
		opts.RelationTypes = DefaultHeadwordRelationTypes()
	} else {
		opts.RelationTypes = uniqueNonEmptyStrings(opts.RelationTypes)
	}
	if opts.LimitPerRelationType <= 0 {
		opts.LimitPerRelationType = defaultRelationLimitPerType
	}
	return opts
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	results := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		results = append(results, value)
	}
	return results
}

func buildHeadwordRelationGroups(rows []headwordRelationGroupRow) []HeadwordRelationGroup {
	groups := make([]HeadwordRelationGroup, 0)
	groupIndexes := make(map[string]int)
	for _, row := range rows {
		index, exists := groupIndexes[row.RelationType]
		if !exists {
			index = len(groups)
			groupIndexes[row.RelationType] = index
			groups = append(groups, HeadwordRelationGroup{RelationType: row.RelationType})
		}
		groups[index].Items = append(groups[index].Items, HeadwordRelationItem{
			TargetHeadword:           row.TargetHeadword,
			TargetHeadwordNormalized: row.TargetHeadwordNormalized,
			TargetPOSCode:            row.TargetPOSCode,
			SourceRelationType:       row.SourceRelationType,
			SourceSynsetID:           row.SourceSynsetID,
			TargetSynsetID:           row.TargetSynsetID,
			SourceSenseID:            row.SourceSenseID,
			TargetSenseID:            row.TargetSenseID,
			EvidenceCount:            row.EvidenceCount,
			HasTargetEntry:           row.HasTargetEntry,
		})
	}
	return groups
}

func isSupportedHeadwordRelationPOSCode(posCode int) bool {
	switch posCode {
	case model.HeadwordRelationPOSCodeNoun,
		model.HeadwordRelationPOSCodeVerb,
		model.HeadwordRelationPOSCodeAdjective,
		model.HeadwordRelationPOSCodeAdverb:
		return true
	default:
		return false
	}
}

const headwordRelationGroupsSQL = `
WITH allowed_relation_types AS (
	SELECT relation_type, relation_order
	FROM unnest(?::text[]) WITH ORDINALITY AS allowed(relation_type, relation_order)
),
edge_candidates AS (
	SELECT edges.*, allowed.relation_order
	FROM headword_relation_edges edges
	INNER JOIN allowed_relation_types allowed ON allowed.relation_type = edges.relation_type
	WHERE edges.source_headword_normalized = ?
		AND edges.source_pos_code = ?
		AND (? OR NOT (
			edges.target_headword_normalized = edges.source_headword_normalized
			AND edges.target_pos_code = edges.source_pos_code
		))
),
target_keys AS (
	SELECT DISTINCT target_headword_normalized, target_pos_code
	FROM edge_candidates
),
ranked_target_entries AS (
	SELECT
		entries.normalized_headword,
		entries.pos,
		CASE entries.pos
			WHEN 'noun' THEN 1
			WHEN 'verb' THEN 2
			WHEN 'adjective' THEN 3
			WHEN 'adverb' THEN 4
			ELSE 0
		END AS pos_code,
		entries.headword,
		entries.id,
		COALESCE(signals.frequency_rank, 0) AS frequency_rank,
		COALESCE(signals.oxford_level, 0) AS oxford_level,
		COALESCE(signals.cet_level, 0) AS cet_level,
		COALESCE(signals.cefr_level, 0) AS cefr_level,
		COALESCE(signals.collins_stars, 0) AS collins_stars,
		COALESCE(signals.school_level, 0) AS school_level,
		ROW_NUMBER() OVER (
			PARTITION BY entries.normalized_headword, CASE entries.pos
				WHEN 'noun' THEN 1
				WHEN 'verb' THEN 2
				WHEN 'adjective' THEN 3
				WHEN 'adverb' THEN 4
				ELSE 0
			END
			ORDER BY
				CASE WHEN COALESCE(signals.frequency_rank, 0) = 0 THEN 999999999 ELSE signals.frequency_rank END ASC,
				CASE WHEN COALESCE(signals.school_level, 0) = 0 THEN 999999999 ELSE signals.school_level END ASC,
				CASE WHEN COALESCE(signals.cefr_level, 0) > 0 THEN 0 ELSE 1 END ASC,
				CASE WHEN COALESCE(signals.oxford_level, 0) > 0 THEN 0 ELSE 1 END ASC,
				CASE WHEN COALESCE(signals.cet_level, 0) > 0 THEN 0 ELSE 1 END ASC,
				CASE WHEN COALESCE(signals.collins_stars, 0) > 0 THEN 0 ELSE 1 END ASC,
				COALESCE(signals.cefr_level, 0) DESC,
				COALESCE(signals.oxford_level, 0) DESC,
				COALESCE(signals.cet_level, 0) DESC,
				COALESCE(signals.collins_stars, 0) DESC,
				entries.id ASC
		) AS entry_rank
	FROM entries
	LEFT JOIN entry_learning_signals signals ON signals.entry_id = entries.id
	INNER JOIN target_keys keys ON keys.target_headword_normalized = entries.normalized_headword
		AND keys.target_pos_code = CASE entries.pos
			WHEN 'noun' THEN 1
			WHEN 'verb' THEN 2
			WHEN 'adjective' THEN 3
			WHEN 'adverb' THEN 4
			ELSE 0
		END
),
target_entries AS (
	SELECT *
	FROM ranked_target_entries
	WHERE entry_rank = 1
),
deduplicated_edges AS (
	SELECT
		edges.relation_type,
		MIN(edges.relation_order)::int AS relation_order,
		edges.target_headword_normalized,
		edges.target_pos_code,
		COUNT(*)::int AS evidence_count,
		(ARRAY_AGG(edges.target_headword ORDER BY edges.id ASC))[1] AS edge_target_headword,
		(ARRAY_AGG(edges.source_relation_type ORDER BY edges.id ASC))[1] AS source_relation_type,
		(ARRAY_AGG(edges.source_synset_id ORDER BY edges.id ASC))[1] AS source_synset_id,
		(ARRAY_AGG(edges.target_synset_id ORDER BY edges.id ASC))[1] AS target_synset_id,
		(ARRAY_AGG(edges.source_sense_id ORDER BY edges.id ASC))[1] AS source_sense_id,
		(ARRAY_AGG(edges.target_sense_id ORDER BY edges.id ASC))[1] AS target_sense_id
	FROM edge_candidates edges
	GROUP BY edges.relation_type, edges.target_headword_normalized, edges.target_pos_code
),
joined_targets AS (
	SELECT
		edges.relation_type,
		edges.relation_order,
		COALESCE(targets.headword, edges.edge_target_headword) AS target_headword,
		edges.target_headword_normalized,
		edges.target_pos_code,
		edges.source_relation_type,
		edges.source_synset_id,
		edges.target_synset_id,
		edges.source_sense_id,
		edges.target_sense_id,
		edges.evidence_count,
		(targets.normalized_headword IS NOT NULL) AS has_target_entry,
		COALESCE(targets.frequency_rank, 0) AS frequency_rank,
		COALESCE(targets.oxford_level, 0) AS oxford_level,
		COALESCE(targets.cet_level, 0) AS cet_level,
		COALESCE(targets.cefr_level, 0) AS cefr_level,
		COALESCE(targets.collins_stars, 0) AS collins_stars,
		COALESCE(targets.school_level, 0) AS school_level
	FROM deduplicated_edges edges
	LEFT JOIN target_entries targets ON targets.normalized_headword = edges.target_headword_normalized
		AND targets.pos_code = edges.target_pos_code
	WHERE (? OR targets.normalized_headword IS NOT NULL)
),
ranked_items AS (
	SELECT
		joined_targets.*,
		ROW_NUMBER() OVER (
			PARTITION BY joined_targets.relation_type
			ORDER BY
				CASE WHEN joined_targets.frequency_rank = 0 THEN 999999999 ELSE joined_targets.frequency_rank END ASC,
				CASE WHEN joined_targets.school_level = 0 THEN 999999999 ELSE joined_targets.school_level END ASC,
				CASE WHEN joined_targets.cefr_level > 0 THEN 1 ELSE 0 END DESC,
				CASE WHEN joined_targets.oxford_level > 0 THEN 1 ELSE 0 END DESC,
				CASE WHEN joined_targets.cet_level > 0 THEN 1 ELSE 0 END DESC,
				CASE WHEN joined_targets.collins_stars > 0 THEN 1 ELSE 0 END DESC,
				joined_targets.cefr_level DESC,
				joined_targets.oxford_level DESC,
				joined_targets.cet_level DESC,
				joined_targets.collins_stars DESC,
				joined_targets.evidence_count DESC,
				joined_targets.target_headword_normalized ASC
		) AS item_rank
	FROM joined_targets
)
SELECT
	relation_type,
	relation_order,
	target_headword,
	target_headword_normalized,
	target_pos_code,
	source_relation_type,
	source_synset_id,
	target_synset_id,
	source_sense_id,
	target_sense_id,
	evidence_count,
	has_target_entry
FROM ranked_items
WHERE item_rank <= ?
ORDER BY relation_order ASC, item_rank ASC;
`
