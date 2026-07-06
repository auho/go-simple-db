package testutil

import (
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
)

const envFile = ".env.test"

// LoadEnvFile searches for .env.test starting from the current working directory
// up to the project root (the directory containing go.mod) and loads it into the
// environment. If the file does not exist, it is a no-op; externally set
// environment variables still apply.
func LoadEnvFile() error {
	path, err := findEnvFile()
	if err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	return godotenv.Load(path)
}

// findEnvFile walks up from cwd looking for .env.test, stopping at the project
// root (directory containing go.mod). It returns "" when the file is not found.
func findEnvFile() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	for {
		candidate := filepath.Join(cwd, envFile)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}

		if _, err := os.Stat(filepath.Join(cwd, "go.mod")); err == nil {
			return "", nil
		}

		parent := filepath.Dir(cwd)
		if parent == cwd {
			return "", nil
		}
		cwd = parent
	}
}
