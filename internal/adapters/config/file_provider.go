package config

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
	"payment-gateway-router/internal/domain"
)

type FileProvider struct {
	path    string
	logger  *slog.Logger
	mu      sync.RWMutex
	current domain.AppConfig
	modTime time.Time
}

func NewFileProvider(path string, logger *slog.Logger) (*FileProvider, error) {
	provider := &FileProvider{
		path:   path,
		logger: logger,
	}
	if err := provider.reload(); err != nil {
		return nil, err
	}
	return provider, nil
}

func (p *FileProvider) Current(ctx context.Context) domain.AppConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.current
}

func (p *FileProvider) Watch(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.reloadIfChanged(); err != nil {
				p.logger.Error("config reload failed; keeping last valid config", "path", p.path, "error", err)
			}
		}
	}
}

func (p *FileProvider) reloadIfChanged() error {
	info, err := os.Stat(p.path)
	if err != nil {
		return err
	}

	p.mu.RLock()
	unchanged := info.ModTime().Equal(p.modTime)
	p.mu.RUnlock()
	if unchanged {
		return nil
	}

	return p.reload()
}

func (p *FileProvider) reload() error {
	data, err := os.ReadFile(p.path)
	if err != nil {
		return err
	}

	cfg, err := Parse(data, filepath.Ext(p.path))
	if err != nil {
		return err
	}
	cfg = cfg.WithDefaults()
	if err := cfg.Validate(); err != nil {
		return err
	}

	info, err := os.Stat(p.path)
	if err != nil {
		return err
	}

	p.mu.Lock()
	p.current = cfg
	p.modTime = info.ModTime()
	p.mu.Unlock()

	p.logger.Info("config loaded", "path", p.path, "gateways", len(cfg.Gateways))
	return nil
}

func Parse(data []byte, extension string) (domain.AppConfig, error) {
	extension = strings.ToLower(extension)
	var cfg domain.AppConfig
	if extension == ".json" {
		if err := json.Unmarshal(data, &cfg); err != nil {
			return domain.AppConfig{}, err
		}
		return cfg, nil
	}
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return domain.AppConfig{}, err
	}
	return cfg, nil
}
