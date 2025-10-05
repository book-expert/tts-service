package tts

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Report section headers and messages.
const (
	reportHeaderAnalysis  = "=== OuteTTS Cleanup Analysis ==="
	reportHeaderSummary   = "=== Summary ==="
	reportFilesRemoved    = "🗑️ Files that can be removed (replaced by Go):"
	reportDirsRemoved     = "🗑️ Directories that can be removed:"
	reportFilesKept       = "✅ Essential files to keep (ML functionality):"
	reportDirsKept        = "✅ Essential directories to keep:"
	reportRecommendations = "💡 Recommendations:"
	reportErrors          = "❌ Errors encountered:"
)

// Summary format strings.
const (
	summaryFilesToRemove   = "Files to remove: %d\n"
	summaryDirsToRemove    = "Dirs to remove: %d\n"
	summaryFilesToKeep     = "Files to keep: %d\n"
	summaryDirsToKeep      = "Dirs to keep: %d\n"
	summaryRecommendations = "Recommendations: %d\n"
)

// List item format.
const (
	listItemFormat = "  - %s\n"
)

// Static errors.
var (
	ErrMissingGoImpl = errors.New("missing Go implementations")
)

// Error messages.
const (
	replacementFormat    = "%s -> %s"
	replacementSeparator = ", "
)

// CleanupReport provides information about cleanup opportunities. This struct is
// used to generate a report of the cleanup analysis.
type CleanupReport struct {
	RemovedFiles    []string
	RemovedDirs     []string
	KeptFiles       []string
	KeptDirs        []string
	Recommendations []string
	Errors          []string
}

// AnalyzeOuteTTSCleanup analyzes what can be cleaned up from OuteTTS Python
// implementation.
func getRemovableFiles() []string {
	return []string{
		"utils/chunking.py",      // Replaced by internal/chunking/chunking.go
		"utils/preprocessing.py", // Can be implemented in Go
		"utils/helpers.py",       // Simple utilities, can be Go
		"anyascii/__init__.py",   // Can be pure Go implementation
		"whisper/transcribe.py",  // Can use Go HTTP client for API calls
	}
}

func getEssentialFiles() []string {
	return []string{
		"interface.py",              // Main OuteTTS interface
		"version/interface.py",      // Core model interfaces
		"version/playback.py",       // Audio playback
		"dac/interface.py",          // DAC audio codec (ML-based)
		"models/config.py",          // Model configuration
		"models/hf_model.py",        // Hugging Face model loading
		"models/gguf_model.py",      // GGUF model loading
		"models/exl2_model.py",      // ExL2 model loading
		"models/vllm_model.py",      // VLLM model loading
		"models/llamacpp_server.py", // Llama.cpp server integration
	}
}

func getDirectoryLists() (removableDirs, essentialDirs []string) {
	removableDirs = []string{
		"anyascii/_data", // ASCII conversion data (can be Go)
	}
	essentialDirs = []string{
		"version/v1",    // Version 1 interface
		"version/v2",    // Version 2 interface
		"version/v3",    // Version 3 interface
		"dac",           // DAC audio codec
		"models",        // Model implementations
		"wav_tokenizer", // Audio tokenization
	}

	return removableDirs, essentialDirs
}

func analyzeRemovableFiles(outettsDir string, report *CleanupReport) {
	removableFiles := getRemovableFiles()
	for _, file := range removableFiles {
		filePath := filepath.Join(outettsDir, file)

		_, statErr := os.Stat(filePath)
		if statErr == nil {
			report.RemovedFiles = append(report.RemovedFiles, file)
		}
	}
}

func analyzeEssentialFiles(outettsDir string, report *CleanupReport) {
	essentialFiles := getEssentialFiles()
	for _, file := range essentialFiles {
		filePath := filepath.Join(outettsDir, file)

		_, statErr := os.Stat(filePath)
		if statErr == nil {
			report.KeptFiles = append(report.KeptFiles, file)
		}
	}
}

func analyzeFiles(outettsDir string, report *CleanupReport) {
	analyzeRemovableFiles(outettsDir, report)
	analyzeEssentialFiles(outettsDir, report)
}

func analyzeRemovableDirectories(
	outettsDir string,
	removableDirs []string,
	report *CleanupReport,
) {
	for _, dir := range removableDirs {
		dirPath := filepath.Join(outettsDir, dir)

		_, statErr := os.Stat(dirPath)
		if statErr == nil {
			report.RemovedDirs = append(report.RemovedDirs, dir)
		}
	}
}

func analyzeEssentialDirectories(
	outettsDir string,
	essentialDirs []string,
	report *CleanupReport,
) {
	for _, dir := range essentialDirs {
		dirPath := filepath.Join(outettsDir, dir)

		_, statErr := os.Stat(dirPath)
		if statErr == nil {
			report.KeptDirs = append(report.KeptDirs, dir)
		}
	}
}

func analyzeDirectories(outettsDir string, report *CleanupReport) {
	removableDirs, essentialDirs := getDirectoryLists()
	analyzeRemovableDirectories(outettsDir, removableDirs, report)
	analyzeEssentialDirectories(outettsDir, essentialDirs, report)
}

func generateRecommendations() []string {
	return []string{
		"Text chunking has been replaced by Go implementation in internal/chunking/",
		"Text preprocessing can be implemented in Go using regex and unicode packages",
		"AnyASCII conversion can be pure Go using unicode normalization",
		"Whisper integration can use Go HTTP client for API calls",
		"Configuration management is now handled by Go structs",
		"Audio file operations can use Go audio libraries",
		"Keep core ML components (DAC, model loading, inference) in Python",
		"Use Go for orchestration, Python for ML inference",
	}
}

// AnalyzeOuteTTSCleanup analyzes the OuteTTS directory structure and generates a
// cleanup report with recommendations for file and directory management. This
// function is the main entry point for the cleanup analysis.
func AnalyzeOuteTTSCleanup(outettsDir string) (*CleanupReport, error) {
	report := &CleanupReport{
		RemovedFiles:    []string{},
		RemovedDirs:     []string{},
		KeptFiles:       []string{},
		KeptDirs:        []string{},
		Recommendations: []string{},
		Errors:          []string{},
	}

	analyzeFiles(outettsDir, report)
	analyzeDirectories(outettsDir, report)

	report.Recommendations = generateRecommendations()

	return report, nil
}

// formatFileSection formats a file section for the cleanup report.
func formatFileSection(title string, files []string) string {
	if len(files) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(title)
	result.WriteString("\n")

	for _, file := range files {
		result.WriteString(fmt.Sprintf(listItemFormat, file))
	}

	result.WriteString("\n")

	return result.String()
}

func formatDirectorySection(title string, dirs []string) string {
	if len(dirs) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(title)
	result.WriteString("\n")

	for _, dir := range dirs {
		result.WriteString(fmt.Sprintf(listItemFormat, dir))
	}

	result.WriteString("\n")

	return result.String()
}

func formatRecommendationsSection(recommendations []string) string {
	if len(recommendations) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(reportRecommendations)
	result.WriteString("\n")

	for _, rec := range recommendations {
		result.WriteString(fmt.Sprintf(listItemFormat, rec))
	}

	result.WriteString("\n")

	return result.String()
}

func formatErrorsSection(errorList []string) string {
	if len(errorList) == 0 {
		return ""
	}

	var result strings.Builder
	result.WriteString(reportErrors)
	result.WriteString("\n")

	for _, errorItem := range errorList {
		result.WriteString(fmt.Sprintf(listItemFormat, errorItem))
	}

	result.WriteString("\n")

	return result.String()
}

func formatSummarySection(report *CleanupReport) string {
	var result strings.Builder
	result.WriteString(reportHeaderSummary)
	result.WriteString("\n")
	result.WriteString(fmt.Sprintf(summaryFilesToRemove, len(report.RemovedFiles)))
	result.WriteString(fmt.Sprintf(summaryDirsToRemove, len(report.RemovedDirs)))
	result.WriteString(fmt.Sprintf(summaryFilesToKeep, len(report.KeptFiles)))
	result.WriteString(fmt.Sprintf(summaryDirsToKeep, len(report.KeptDirs)))
	result.WriteString(
		fmt.Sprintf(summaryRecommendations, len(report.Recommendations)),
	)

	return result.String()
}

// FormatCleanupReport formats a cleanup report as a string. This function is
// responsible for generating a human-readable report from the CleanupReport
// struct.
func FormatCleanupReport(report *CleanupReport) string {
	var result strings.Builder
	result.WriteString(reportHeaderAnalysis)
	result.WriteString("\n\n")

	result.WriteString(formatFileSection(reportFilesRemoved, report.RemovedFiles))
	result.WriteString(formatDirectorySection(reportDirsRemoved, report.RemovedDirs))
	result.WriteString(formatFileSection(reportFilesKept, report.KeptFiles))
	result.WriteString(formatDirectorySection(reportDirsKept, report.KeptDirs))
	result.WriteString(formatRecommendationsSection(report.Recommendations))
	result.WriteString(formatErrorsSection(report.Errors))
	result.WriteString(formatSummarySection(report))

	return result.String()
}

// GetGoReplacements returns a map of Python files to their Go replacements. This
// function is used to validate that the Go replacements exist.
func GetGoReplacements() map[string]string {
	return map[string]string{
		"utils/chunking.py":      "internal/chunking/chunking.go",
		"utils/preprocessing.py": "internal/tts/text/preprocessing.go",
		"utils/helpers.py":       "internal/tts/utils/helpers.go",
		"anyascii/__init__.py":   "internal/tts/text/anyascii.go",
		"whisper/transcribe.py":  "internal/tts/whisper/client.go",
		"models/config.py":       "internal/tts/config/config.go",
	}
}

// ValidateGoImplementation checks if Go replacements exist. This function is
// used to ensure that all the Python files that are supposed to be replaced by
// Go implementations have a corresponding Go file.
func ValidateGoImplementation() error {
	replacements := GetGoReplacements()

	var missing []string

	for pythonFile, goFile := range replacements {
		_, err := os.Stat(goFile)
		if os.IsNotExist(err) {
			missing = append(
				missing,
				fmt.Sprintf(replacementFormat, pythonFile, goFile),
			)
		}
	}

	if len(missing) > 0 {
		return fmt.Errorf(
			"%w: %s",
			ErrMissingGoImpl,
			strings.Join(missing, replacementSeparator),
		)
	}

	return nil
}
