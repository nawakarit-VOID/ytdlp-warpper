package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/gen2brain/dlgs"
)

// YtDlpWrapper คือตัวจัดการหลัก
type YtDlpWrapper struct {
	URL         string
	OutputDir   string
	MaxRetries  int
	Concurrent  int
	RetryCount  int
	YtdlpPath   string
	FfmpegPath  string
	CurrentFile string
	IsRetry     bool
}

// SegmentInfo เก็บข้อมูลของแต่ละ segment
type SegmentInfo struct {
	Index      int    `json:"index"`
	Downloaded bool   `json:"downloaded"`
	URL        string `json:"url"`
}

// YtdlFile โครงสร้างไฟล์ .ytdl
type YtdlFile struct {
	Filename   string        `json:"filename"`
	TotalBytes int64         `json:"total_bytes,omitempty"`
	Downloaded int64         `json:"downloaded_bytes,omitempty"`
	Fragments  []SegmentInfo `json:"fragments,omitempty"`
	URL        string        `json:"url,omitempty"`
}

// NewYtDlpWrapper สร้าง instance ใหม่
func NewYtDlpWrapper(url, outputDir string, maxRetries, concurrent int) *YtDlpWrapper {
	return &YtDlpWrapper{
		URL:        url,
		OutputDir:  outputDir,
		MaxRetries: maxRetries,
		Concurrent: concurrent,
		RetryCount: 0,
		IsRetry:    false,
		YtdlpPath:  findYtdlp(),
		FfmpegPath: findFfmpeg(),
	}
}

// findYtdlp หา path ของ yt-dlp
func findYtdlp() string {
	// ตรวจสอบใน PATH
	if path, err := exec.LookPath("yt-dlp"); err == nil {
		return path
	}
	// ถ้าไม่เจอ ให้ใช้ชื่อ default
	return "yt-dlp"
}

// findFfmpeg หา path ของ ffmpeg
func findFfmpeg() string {
	if path, err := exec.LookPath("ffmpeg"); err == nil {
		return path
	}
	return ""
}

// showURLDialog แสดง Dialog ให้ใส่ URL ใหม่ (ใช้ dlgs)
func showURLDialog(oldURL, errorMsg string) (string, bool) {
	message := fmt.Sprintf("URL เดิม: %s\n\nError: %s\n\nกรุณาใส่ URL ใหม่:", oldURL[:min(80, len(oldURL))], errorMsg)

	newURL, ok, err := dlgs.Entry("🔄 เปลี่ยน URL", message, oldURL)
	if err != nil {
		fmt.Printf("Error showing dialog: %v\n", err)
		return "", false
	}

	return newURL, ok
}

// min helper function
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// findYtdlFile ค้นหาไฟล์ .ytdl ใน output directory
func (w *YtDlpWrapper) findYtdlFile() (string, error) {
	files, err := filepath.Glob(filepath.Join(w.OutputDir, "*.ytdl"))
	if err != nil {
		return "", err
	}

	if len(files) == 0 {
		return "", fmt.Errorf("ไม่พบไฟล์ .ytdl")
	}

	// เลือกไฟล์ล่าสุด
	var latest string
	var latestTime time.Time

	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		if info.ModTime().After(latestTime) {
			latestTime = info.ModTime()
			latest = f
		}
	}

	return latest, nil
}

// updateYtdlFile อัปเดต URL ในไฟล์ .ytdl
func (w *YtDlpWrapper) updateYtdlFile(ytdlPath, oldPattern, newURL string) error {
	// อ่านไฟล์
	data, err := os.ReadFile(ytdlPath)
	if err != nil {
		return fmt.Errorf("อ่านไฟล์ .ytdl ไม่ได้: %v", err)
	}

	// พยายาม parse เป็น JSON
	var ytdlData YtdlFile
	if err := json.Unmarshal(data, &ytdlData); err == nil {
		// อัปเดต URL ใน fragments
		for i := range ytdlData.Fragments {
			if strings.Contains(ytdlData.Fragments[i].URL, oldPattern) {
				ytdlData.Fragments[i].URL = strings.ReplaceAll(
					ytdlData.Fragments[i].URL,
					oldPattern,
					newURL,
				)
			}
		}

		// อัปเดต URL หลัก
		if strings.Contains(ytdlData.URL, oldPattern) {
			ytdlData.URL = strings.ReplaceAll(ytdlData.URL, oldPattern, newURL)
		}

		// เขียนกลับ
		newData, err := json.MarshalIndent(ytdlData, "", "  ")
		if err != nil {
			return fmt.Errorf("marshal JSON ไม่ได้: %v", err)
		}

		if err := os.WriteFile(ytdlPath, newData, 0644); err != nil {
			return fmt.Errorf("เขียนไฟล์ .ytdl ไม่ได้: %v", err)
		}

		fmt.Printf("✅ อัปเดตไฟล์ .ytdl: %s\n", ytdlPath)
		return nil
	}

	// ถ้าไม่ใช่ JSON ให้ใช้ string replace
	content := string(data)
	newContent := strings.ReplaceAll(content, oldPattern, newURL)

	if err := os.WriteFile(ytdlPath, []byte(newContent), 0644); err != nil {
		return fmt.Errorf("เขียนไฟล์ .ytdl ไม่ได้: %v", err)
	}

	fmt.Printf("✅ อัปเดตไฟล์ .ytdl: %s\n", ytdlPath)
	return nil
}

// extractURLBase ดึงส่วน base ของ URL
func extractURLBase(url string) string {
	re := regexp.MustCompile(`(https?://[^/]+/[^?]+)`)
	matches := re.FindStringSubmatch(url)
	if len(matches) > 0 {
		return matches[1]
	}
	return url
}

// runYtdlp รัน yt-dlp และคืนค่า error
func (w *YtDlpWrapper) runYtdlp(url string) error {
	// สร้างคำสั่ง
	args := []string{
		"--no-progress",
		"--newline",
		"-N", fmt.Sprintf("%d", w.Concurrent),
		"--fragment-retries", "5",
		"--retries", "3",
		"--socket-timeout", "30",
		"-o", filepath.Join(w.OutputDir, "%(title)s.%(ext)s"),
		url,
	}

	if w.IsRetry {
		args = append([]string{"--continue", "--no-overwrites"}, args...)
	}

	cmd := exec.Command(w.YtdlpPath, args...)

	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	if w.IsRetry {
		fmt.Printf("🔄 Retry download: %s\n", url[:min(100, len(url))])
	} else {
		fmt.Printf("🔄 Start download: %s\n", url[:min(100, len(url))])
	}
	fmt.Printf("📂 Output: %s\n", w.OutputDir)
	fmt.Printf("%s\n\n", strings.Repeat("=", 60))

	// จัดการ stdout และ stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("สร้าง stdout pipe ไม่ได้: %v", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("สร้าง stderr pipe ไม่ได้: %v", err)
	}

	// เริ่ม process
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("เริ่ม yt-dlp ไม่ได้: %v", err)
	}

	// Goroutine สำหรับอ่าน stdout แบบ real-time
	errorChan := make(chan string, 10)
	scanErrChan := make(chan error, 2)
	doneChan := make(chan bool)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println(line)

			// ตรวจจับ error patterns
			errorPatterns := []string{
				"HTTP Error 502",
				"HTTP Error 403",
				"HTTP Error 404",
				"ERROR:.*failed",
				"ERROR:.*not found",
				"ERROR:.*expired",
				"fragment.*failed",
				"segments.*failed",
			}

			for _, pattern := range errorPatterns {
				matched, _ := regexp.MatchString(pattern, line)
				if matched {
					errorChan <- line
					break
				}
			}
		}
		if err := scanner.Err(); err != nil {
			scanErrChan <- fmt.Errorf("stdout scan error: %w", err)
		}
		doneChan <- true
	}()

	// Goroutine สำหรับอ่าน stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[stderr] %s\n", line)

			// ตรวจจับ error ใน stderr ด้วย
			if strings.Contains(strings.ToLower(line), "error") ||
				strings.Contains(line, "502") ||
				strings.Contains(line, "403") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			scanErrChan <- fmt.Errorf("stderr scan error: %w", err)
		}
	}()

	// รอให้ process จบ
	err = cmd.Wait()

	// เช็คว่ามี error จาก stdout/stderr หรือไม่
	select {
	case errMsg := <-errorChan:
		return fmt.Errorf("detected error: %s", errMsg)
	case scanErr := <-scanErrChan:
		return scanErr
	default:
		// ไม่มี error
	}

	if err != nil {
		return fmt.Errorf("yt-dlp return error: %v", err)
	}

	return nil
}

// Download เริ่มกระบวนการดาวน์โหลด
func (w *YtDlpWrapper) Download() error {
	currentURL := w.URL

	for w.RetryCount < w.MaxRetries {
		// รัน yt-dlp
		err := w.runYtdlp(currentURL)

		if err == nil {
			fmt.Println("\n✅ ดาวน์โหลดเสร็จสมบูรณ์!")
			return nil
		}

		fmt.Printf("\n⚠️ พบปัญหา: %v\n", err)
		w.RetryCount++

		if w.RetryCount >= w.MaxRetries {
			fmt.Printf("❌ ลองใหม่ %d ครั้งแล้ว ไม่สำเร็จ\n", w.MaxRetries)
			return fmt.Errorf("ดาวน์โหลดล้มเหลวหลังจากลอง %d ครั้ง", w.MaxRetries)
		}

		// ค้นหาไฟล์ .ytdl
		ytdlFile, err := w.findYtdlFile()
		if err != nil {
			fmt.Printf("⚠️ ไม่พบไฟล์ .ytdl: %v\n", err)
		} else {
			fmt.Printf("📄 พบไฟล์ .ytdl: %s\n", ytdlFile)
		}

		// แสดง Dialog ให้ใส่ URL ใหม่
		newURL, ok := showURLDialog(
			currentURL,
			fmt.Sprintf("%v (ครั้งที่ %d/%d)", err, w.RetryCount, w.MaxRetries),
		)

		if !ok || newURL == "" {
			fmt.Println("❌ ผู้ใช้ยกเลิกการดาวน์โหลด")
			return fmt.Errorf("ผู้ใช้ยกเลิก")
		}

		if newURL == currentURL {
			fmt.Println("ℹ️ URL ไม่เปลี่ยนแปลง, ลองใหม่ด้วย URL เดิม")
		}

		// อัปเดตไฟล์ .ytdl
		if ytdlFile != "" {
			oldBase := extractURLBase(currentURL)
			newBase := extractURLBase(newURL)

			if oldBase != newBase {
				if err := w.updateYtdlFile(ytdlFile, oldBase, newBase); err != nil {
					fmt.Printf("⚠️ อัปเดตไฟล์ .ytdl ไม่ได้: %v\n", err)
				}
			} else {
				fmt.Println("ℹ️ URL base ไม่เปลี่ยนแปลง, ไม่จำเป็นต้องอัปเดต .ytdl")
			}
		}

		currentURL = newURL
		w.IsRetry = true
		fmt.Printf("\n🔄 เริ่มลองใหม่ด้วย URL: %s\n", currentURL)
	}

	return fmt.Errorf("ดาวน์โหลดล้มเหลว")
}

func main() {
	// ตรวจสอบ arguments
	if len(os.Args) < 2 {
		fmt.Printf("Usage: %s <URL> [output_dir]\n", os.Args[0])
		fmt.Printf("Example: %s https://example.com/video.m3u8\n", os.Args[0])
		fmt.Printf("Example: %s https://example.com/video.m3u8 ./downloads\n", os.Args[0])
		os.Exit(1)
	}

	url := os.Args[1]
	outputDir := "."
	if len(os.Args) > 2 {
		outputDir = os.Args[2]
	}

	// สร้าง output directory ถ้ายังไม่มี
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("❌ สร้าง directory ไม่ได้: %v\n", err)
		os.Exit(1)
	}

	// สร้าง wrapper
	wrapper := NewYtDlpWrapper(
		url,
		outputDir,
		5, // max retries
		8, // concurrent (-N)
	)

	// เริ่มดาวน์โหลด
	if err := wrapper.Download(); err != nil {
		fmt.Printf("\n❌ %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n🎉 โหลดเสร็จเรียบร้อย!")
	os.Exit(0)
}
