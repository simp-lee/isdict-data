package service

const (
	defaultBatchMaxSize    = 100
	defaultSearchMaxLimit  = 100
	defaultSuggestMaxLimit = 50
)

// ServiceConfig holds the minimal configuration needed by WordService.
type ServiceConfig struct {
	BatchMaxSize    int
	SearchMaxLimit  int
	SuggestMaxLimit int
}

func normalizeServiceConfig(cfg ServiceConfig) ServiceConfig {
	if cfg.BatchMaxSize <= 0 {
		cfg.BatchMaxSize = defaultBatchMaxSize
	}
	if cfg.SearchMaxLimit <= 0 {
		cfg.SearchMaxLimit = defaultSearchMaxLimit
	}
	if cfg.SuggestMaxLimit <= 0 {
		cfg.SuggestMaxLimit = defaultSuggestMaxLimit
	}
	return cfg
}
