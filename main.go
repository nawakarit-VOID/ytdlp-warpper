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

type YtDlpWrapper struct {
	URL        string
	OutputDir  string
	MaxRetries int
	Concurrent int
	RetryCount int
	YtdlpPath  string
	IsRetry    bool
	URLHistory []string
}

type YtdlFile struct {
	Filename  string `json:"filename"`
	Fragments []struct {
		Index int    `json:"index"`
		URL   string `json:"url"`
	} `json:"fragments"`
	URL string `json:"url"`
}

func NewYtDlpWrapper(url, outputDir string, maxRetries, concurrent int) *YtDlpWrapper {
	return &YtDlpWrapper{
		URL:        url,
		OutputDir:  outputDir,
		MaxRetries: maxRetries,
		Concurrent: concurrent,
		RetryCount: 0,
		IsRetry:    false,
		YtdlpPath:  findYtdlp(),
		URLHistory: []string{url},
	}
}

func findYtdlp() string {
	if path, err := exec.LookPath("yt-dlp"); err == nil {
		return path
	}
	return "yt-dlp"
}

func showURLDialogWithHistory(oldURL string, errorMsg string, history []string) (string, bool) {
	historyText := ""
	if len(history) > 1 {
		historyText = "\n📜 ประวัติ URL ที่ลองแล้ว:\n"
		for i, u := range history {
			historyText += fmt.Sprintf("  %d. %s\n", i+1, u)
		}
	}

	message := fmt.Sprintf(
		"URL ปัจจุบัน: %s\n\nError: %s\n%s\n\nกรุณาใส่ URL ใหม่ (หรือกด Cancel เพื่อยกเลิก):",
		oldURL,
		errorMsg,
		historyText,
	)

	newURL, ok, err := dlgs.Entry("🔄 เปลี่ยน URL (รอบที่ "+fmt.Sprint(len(history))+")", message, oldURL)
	if err != nil {
		fmt.Printf("Error showing dialog: %v\n", err)
		return "", false
	}

	return newURL, ok
}

func (w *YtDlpWrapper) findYtdlFile() (string, error) {
	files, err := filepath.Glob(filepath.Join(w.OutputDir, "*.ytdl"))
	if err != nil || len(files) == 0 {
		return "", fmt.Errorf("ไม่พบไฟล์ .ytdl")
	}

	var latest string
	var latestTime time.Time
	for _, f := range files {
		info, _ := os.Stat(f)
		if info.ModTime().After(latestTime) {
			latestTime = info.ModTime()
			latest = f
		}
	}
	return latest, nil
}

func (w *YtDlpWrapper) updateYtdlFile(ytdlPath, oldPattern, newURL string) error {
	data, err := os.ReadFile(ytdlPath)
	if err != nil {
		return err
	}

	var ytdlData YtdlFile
	if err := json.Unmarshal(data, &ytdlData); err == nil {
		for i := range ytdlData.Fragments {
			ytdlData.Fragments[i].URL = strings.ReplaceAll(
				ytdlData.Fragments[i].URL,
				oldPattern,
				newURL,
			)
		}
		ytdlData.URL = strings.ReplaceAll(ytdlData.URL, oldPattern, newURL)

		newData, _ := json.MarshalIndent(ytdlData, "", "  ")
		return os.WriteFile(ytdlPath, newData, 0644)
	}

	newContent := strings.ReplaceAll(string(data), oldPattern, newURL)
	return os.WriteFile(ytdlPath, []byte(newContent), 0644)
}

func extractURLBase(url string) string {
	re := regexp.MustCompile(`(https?://[^/]+/[^?]+)`)
	matches := re.FindStringSubmatch(url)
	if len(matches) > 0 {
		return matches[1]
	}
	return url
}

func (w *YtDlpWrapper) runYtdlp(url string) error {
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
		fmt.Printf("🔄 Retry #%d: %s\n", w.RetryCount, url)
	} else {
		fmt.Printf("🔄 Start: %s\n", url)
	}
	fmt.Printf("📂 Output: %s\n", w.OutputDir)
	fmt.Printf("%s\n\n", strings.Repeat("=", 60))

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("สร้าง stdout pipe ไม่ได้: %v", err)
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return fmt.Errorf("สร้าง stderr pipe ไม่ได้: %v", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("เริ่ม yt-dlp ไม่ได้: %v", err)
	}

	errorChan := make(chan string, 10)

	// อ่าน stdout
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Println(line)

			if strings.Contains(line, "HTTP Error 502") ||
				strings.Contains(line, "HTTP Error 403") ||
				strings.Contains(line, "HTTP Error 404") ||
				strings.Contains(line, "ERROR:") ||
				strings.Contains(line, "fragment failed") ||
				strings.Contains(line, "segments failed") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("stdout scanner error: %v", err)
		}
	}()

	// อ่าน stderr
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[stderr] %s\n", line)

			if strings.Contains(line, "502") ||
				strings.Contains(line, "403") ||
				strings.Contains(line, "ERROR:") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("stderr scanner error: %v", err)
		}
	}()

	err = cmd.Wait()

	// เช็ค error จาก channel
	select {
	case errMsg := <-errorChan:
		return fmt.Errorf("Error: %s", errMsg)
	default:
	}

	if err != nil {
		return err
	}

	return nil
}

// Download เริ่มดาวน์โหลด (รองรับหลายรอบ)
func (w *YtDlpWrapper) Download() error {
	currentURL := w.URL
	attempt := 0

	for {
		attempt++
		fmt.Printf("\n📌 รอบที่ %d (ใช้ URL: %s)\n", attempt, currentURL)

		err := w.runYtdlp(currentURL)

		if err == nil {
			fmt.Println("\n✅ ดาวน์โหลดเสร็จสมบูรณ์!")
			return nil
		}

		fmt.Printf("\n⚠️ รอบที่ %d เจอปัญหา: %v\n", attempt, err)

		ytdlFile, _ := w.findYtdlFile()
		if ytdlFile != "" {
			fmt.Printf("📄 พบไฟล์ .ytdl: %s\n", ytdlFile)
		}

		newURL, ok := showURLDialogWithHistory(
			currentURL,
			fmt.Sprintf("%v (รอบที่ %d)", err, attempt),
			w.URLHistory,
		)

		if !ok || newURL == "" {
			fmt.Println("❌ ผู้ใช้ยกเลิกการดาวน์โหลด")
			return fmt.Errorf("ผู้ใช้ยกเลิก")
		}

		if newURL == currentURL {
			fmt.Println("ℹ️ URL ไม่เปลี่ยนแปลง, ลองใหม่ด้วย URL เดิม")
			time.Sleep(2 * time.Second)
			continue
		}

		alreadyUsed := false
		for _, u := range w.URLHistory {
			if u == newURL {
				alreadyUsed = true
				break
			}
		}

		if alreadyUsed {
			fmt.Println("⚠️ URL นี้เคยใช้ไปแล้ว! อาจจะไม่ช่วยอะไร")
			retry, err := dlgs.Question("ยืนยัน", "URL นี้เคยใช้ไปแล้ว คุณแน่ใจว่าจะลองอีกครั้ง?", true)
			if err != nil {
				fmt.Printf("⚠️ Error showing dialog: %v\n", err)
				return fmt.Errorf("dialog error: %v", err)
			}
			if !retry {
				fmt.Println("❌ ผู้ใช้ยกเลิก")
				return fmt.Errorf("ผู้ใช้ยกเลิก")
			}
		}

		if ytdlFile != "" {
			oldBase := extractURLBase(currentURL)
			newBase := extractURLBase(newURL)
			if oldBase != newBase {
				if err := w.updateYtdlFile(ytdlFile, oldBase, newBase); err != nil {
					fmt.Printf("⚠️ อัปเดตไฟล์ .ytdl ไม่ได้: %v\n", err)
				} else {
					fmt.Println("✅ อัปเดตไฟล์ .ytdl สำเร็จ")
				}
			} else {
				fmt.Println("ℹ️ URL base ไม่เปลี่ยนแปลง, ไม่ต้องอัปเดต .ytdl")
			}
		}

		w.URLHistory = append(w.URLHistory, newURL)
		currentURL = newURL
		w.IsRetry = true
		w.RetryCount++

		fmt.Printf("\n🔄 เตรียมลองใหม่ด้วย URL: %s\n", currentURL)
		fmt.Printf("📊 จำนวน URL ที่ลองแล้ว: %d\n", len(w.URLHistory))
	}
}

func main() {
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

	os.MkdirAll(outputDir, 0755)

	wrapper := NewYtDlpWrapper(url, outputDir, 999, 8)

	if err := wrapper.Download(); err != nil {
		fmt.Printf("\n❌ %v\n", err)
		os.Exit(1)
	}

	fmt.Println("\n🎉 โหลดเสร็จเรียบร้อย!")
}
