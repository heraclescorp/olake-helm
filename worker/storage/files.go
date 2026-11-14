package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/datazip-inc/olake-helm/worker/constants"
	"github.com/datazip-inc/olake-helm/worker/types"
)

// WriteFiles atomically replaces each config under workDir in the active storage
// mode. Publication is per file, not an atomic transaction across all configs.
func WriteFiles(ctx context.Context, workDir string, configs []types.JobConfig) error {
	if len(configs) == 0 {
		return nil
	}

	if Mode() == constants.StorageModeS3 {
		return writeFilesS3(ctx, workDir, configs)
	}

	for _, jobConfig := range configs {
		if err := WriteFileAtomic(ctx, workDir, jobConfig.Name, jobConfig.Data); err != nil {
			return fmt.Errorf("failed to write %s: %w", jobConfig.Name, err)
		}
	}
	return nil
}

// CreateFileIfAbsent publishes a complete file only if the destination is absent.
// A false, nil result means a file already exists; callers must read and validate it.
// Errors can occur after publication, so an error never proves the file is absent.
// Local storage requires same-directory hard links and directory fsync support;
// S3-compatible storage must implement atomic If-None-Match conditional writes.
func CreateFileIfAbsent(ctx context.Context, dir, filename, data string) (bool, error) {
	if Mode() == constants.StorageModeS3 {
		return writeFileAtomicS3(ctx, dir, filename, data, true)
	}
	return writeFileAtomicLocal(ctx, dir, filename, data, true)
}

// WriteFileAtomic replaces a file with complete contents and waits for durable
// local publication or the S3 PutObject acknowledgement. Readers see either the
// old complete file or the new complete file. An error may follow publication.
// This is unconditional replacement, not compare-and-swap: a runtime fence may
// only use this to close, never to reopen a previously used execution identity.
func WriteFileAtomic(ctx context.Context, dir, filename, data string) error {
	if Mode() == constants.StorageModeS3 {
		_, err := writeFileAtomicS3(ctx, dir, filename, data, false)
		return err
	}
	_, err := writeFileAtomicLocal(ctx, dir, filename, data, false)
	return err
}

func writeFileAtomicLocal(ctx context.Context, dir, filename, data string, createOnly bool) (bool, error) {
	filePath := filepath.Join(dir, filename)
	tempPath, err := prepareLocalFile(ctx, filePath, data)
	if err != nil {
		return false, err
	}
	defer os.Remove(tempPath)
	return publishLocalFile(ctx, tempPath, filePath, createOnly)
}

// prepareLocalFile never exposes the destination before the entire temporary
// file has been written, synced and closed. Keep the temporary file beside the
// destination so link and rename cannot cross filesystem boundaries.
func prepareLocalFile(ctx context.Context, filePath, data string) (tempPath string, err error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	parent := filepath.Dir(filePath)
	if err := os.MkdirAll(parent, constants.DefaultDirPermissions); err != nil {
		return "", fmt.Errorf("failed to create directory %s: %w", parent, err)
	}
	temp, err := os.CreateTemp(parent, "."+filepath.Base(filePath)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("failed to prepare %s: %w", filePath, err)
	}
	tempPath = temp.Name()
	defer func() {
		_ = temp.Close()
		if err != nil {
			_ = os.Remove(temp.Name())
		}
	}()
	if err := temp.Chmod(constants.DefaultFilePermissions); err != nil {
		return "", fmt.Errorf("failed to set permissions for %s: %w", filePath, err)
	}
	if _, err := temp.WriteString(data); err != nil {
		return "", fmt.Errorf("failed to write temporary file for %s: %w", filePath, err)
	}
	if err := temp.Sync(); err != nil {
		return "", fmt.Errorf("failed to sync temporary file for %s: %w", filePath, err)
	}
	if err := temp.Close(); err != nil {
		return "", fmt.Errorf("failed to close temporary file for %s: %w", filePath, err)
	}
	return tempPath, nil
}

// publishLocalFile is the linearization point: no writer modifies a published
// inode. In particular, an OPEN prepared before CLOSED cannot overwrite CLOSED
// when its delayed hard link is finally attempted.
func publishLocalFile(ctx context.Context, tempPath, filePath string, createOnly bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	var err error
	if createOnly {
		err = os.Link(tempPath, filePath)
		if errors.Is(err, fs.ErrExist) {
			return false, nil
		}
	} else {
		err = os.Rename(tempPath, filePath)
	}
	if err != nil {
		return false, fmt.Errorf("failed to publish %s: %w", filePath, err)
	}
	parent, err := os.Open(filepath.Dir(filePath))
	if err != nil {
		return true, fmt.Errorf("failed to open parent directory for %s: %w", filePath, err)
	}
	defer parent.Close()
	if err := parent.Sync(); err != nil {
		return true, fmt.Errorf("failed to sync parent directory for %s: %w", filePath, err)
	}
	return true, nil
}

// ReadFile reads relativePath under workDir (a path under ConfigDir) in the active storage mode.
// A missing file returns an error that matches fs.ErrNotExist in both modes. validateJSON also
// requires the content to parse as a JSON object.
func ReadFile(ctx context.Context, workDir, relativePath string, validateJSON bool) (string, error) {
	if Mode() == constants.StorageModeS3 {
		return readFileS3(ctx, workDir, relativePath, validateJSON)
	}

	filePath := filepath.Join(workDir, relativePath)
	data, err := os.ReadFile(filePath)
	if err != nil {
		return "", fmt.Errorf("failed to read file %s: %w", filePath, err)
	}
	if validateJSON {
		var result map[string]interface{}
		if err := json.Unmarshal(data, &result); err != nil {
			return "", fmt.Errorf("failed to parse JSON from file %s: %s", filePath, err)
		}
	}
	return string(data), nil
}
