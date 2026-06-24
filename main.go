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
	"sync"
	"time"

	"github.com/gen2brain/dlgs"
)

// ==================== ตัวจัดการต่อ 1 วิดีโอ ====================
type YtDlpWrapper struct {
	URL          string
	OutputDir    string
	Concurrent   int
	RetryCount   int
	YtdlpPath    string
	IsRetry      bool
	URLHistory   []string
	mu           sync.Mutex
	Progress     float64 // 0-100
	Status       string
	SegmentCount int
	DoneSegments int
}

type YtdlFile struct {
	Filename  string `json:"filename"`
	Fragments []struct {
		Index int    `json:"index"`
		URL   string `json:"url"`
	} `json:"fragments"`
	URL string `json:"url"`
}

func NewYtDlpWrapper(url, outputDir string, concurrent int) *YtDlpWrapper {
	return &YtDlpWrapper{
		URL:          url,
		OutputDir:    outputDir,
		Concurrent:   concurrent,
		RetryCount:   0,
		IsRetry:      false,
		YtdlpPath:    findYtdlp(),
		URLHistory:   []string{url},
		Progress:     0,
		Status:       "pending",
		SegmentCount: 0,
		DoneSegments: 0,
	}
}

func findYtdlp() string {
	if path, err := exec.LookPath("yt-dlp"); err == nil {
		return path
	}
	return "yt-dlp"
}

func (w *YtDlpWrapper) updateProgress(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// ดึงจำนวน segment ทั้งหมด
	if strings.Contains(line, "Downloading") && strings.Contains(line, "fragments") {
		re := regexp.MustCompile(`(\d+)\s+fragments`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &w.SegmentCount)
		}
	}

	// ดึง segment ที่กำลังโหลด
	if strings.Contains(line, "fragment") {
		re := regexp.MustCompile(`fragment\s+(\d+)`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &w.DoneSegments)
			if w.SegmentCount > 0 {
				w.Progress = float64(w.DoneSegments) / float64(w.SegmentCount) * 100
			}
		}
	}

	// ตรวจจับสถานะ
	if strings.Contains(line, "Merging") {
		w.Status = "merging"
	}
	if strings.Contains(line, "100%") {
		w.Progress = 100
		w.Status = "done"
	}
	if strings.Contains(line, "ERROR") || strings.Contains(line, "502") {
		w.Status = "error"
	}
}

func (w *YtDlpWrapper) GetProgress() (float64, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Progress, w.Status
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
		"URL ปัจจุบัน: %s\n\nError: %s\n%s\n\nกรุณาใส่ URL ใหม่:",
		oldURL,
		errorMsg,
		historyText,
	)

	newURL, ok, err := dlgs.Entry("🔄 เปลี่ยน URL", message, oldURL)
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

// runYtdlp รัน yt-dlp สำหรับ 1 วิดีโอ
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

	fmt.Printf("\n📥 [%s] เริ่มดาวน์โหลด...\n", w.OutputDir)

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
			fmt.Printf("[%s] %s\n", w.OutputDir, line)
			w.updateProgress(line)

			if strings.Contains(line, "HTTP Error 502") ||
				strings.Contains(line, "HTTP Error 403") ||
				strings.Contains(line, "HTTP Error 404") ||
				strings.Contains(line, "ERROR:") ||
				strings.Contains(line, "fragment failed") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("scanner error: %v", err)
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Printf("[%s][stderr] %s\n", w.OutputDir, line)
			if strings.Contains(line, "502") || strings.Contains(line, "403") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("stderr scanner error: %v", err)
		}
	}()

	err = cmd.Wait()

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

// Download ดาวน์โหลด 1 วิดีโอ (มี loop retry)
func (w *YtDlpWrapper) Download() error {
	currentURL := w.URL
	attempt := 0

	for {
		attempt++
		fmt.Printf("\n🔄 [%s] รอบที่ %d\n", w.OutputDir, attempt)

		err := w.runYtdlp(currentURL)

		if err == nil {
			w.Status = "done"
			w.Progress = 100
			fmt.Printf("✅ [%s] ดาวน์โหลดเสร็จ!\n", w.OutputDir)
			return nil
		}

		fmt.Printf("⚠️ [%s] Error: %v\n", w.OutputDir, err)
		w.Status = "error"

		ytdlFile, _ := w.findYtdlFile()
		if ytdlFile != "" {
			fmt.Printf("📄 [%s] พบไฟล์ .ytdl\n", w.OutputDir)
		}

		newURL, ok := showURLDialogWithHistory(
			currentURL,
			fmt.Sprintf("%v (รอบที่ %d)", err, attempt),
			w.URLHistory,
		)

		if !ok || newURL == "" {
			fmt.Printf("❌ [%s] ผู้ใช้ยกเลิก\n", w.OutputDir)
			return fmt.Errorf("ผู้ใช้ยกเลิก")
		}

		if newURL == currentURL {
			fmt.Printf("ℹ️ [%s] URL ไม่เปลี่ยนแปลง\n", w.OutputDir)
			time.Sleep(2 * time.Second)
			continue
		}

		if ytdlFile != "" {
			oldBase := extractURLBase(currentURL)
			newBase := extractURLBase(newURL)
			if oldBase != newBase {
				w.updateYtdlFile(ytdlFile, oldBase, newBase)
			}
		}

		w.URLHistory = append(w.URLHistory, newURL)
		currentURL = newURL
		w.IsRetry = true
	}
}

// ==================== Worker Pool สำหรับหลายวิดีโอ ====================

type DownloadJob struct {
	URL       string
	OutputDir string
	ID        int
}

type DownloadResult struct {
	ID      int
	URL     string
	Success bool
	Error   error
}

// WorkerPool จัดการการดาวน์โหลดหลายวิดีโอพร้อมกัน
type WorkerPool struct {
	NumWorkers int
	Concurrent int // ต่อวิดีโอ
	Jobs       chan DownloadJob
	Results    chan DownloadResult
	Wg         sync.WaitGroup
}

func NewWorkerPool(numWorkers, concurrent int) *WorkerPool {
	return &WorkerPool{
		NumWorkers: numWorkers,
		Concurrent: concurrent,
		Jobs:       make(chan DownloadJob, 100),
		Results:    make(chan DownloadResult, 100),
	}
}

func (p *WorkerPool) Start() {
	for i := 0; i < p.NumWorkers; i++ {
		p.Wg.Add(1)
		go p.worker(i)
	}
}

func (p *WorkerPool) worker(id int) {
	defer p.Wg.Done()
	fmt.Printf("🧵 Worker %d เริ่มทำงาน\n", id)

	for job := range p.Jobs {
		fmt.Printf("\n🚀 Worker %d: กำลังโหลด %s\n", id, job.URL)

		wrapper := NewYtDlpWrapper(job.URL, job.OutputDir, p.Concurrent)
		err := wrapper.Download()

		result := DownloadResult{
			ID:      job.ID,
			URL:     job.URL,
			Success: err == nil,
			Error:   err,
		}
		p.Results <- result

		if err == nil {
			fmt.Printf("✅ Worker %d: โหลด %s สำเร็จ!\n", id, job.URL)
		} else {
			fmt.Printf("❌ Worker %d: โหลด %s ล้มเหลว: %v\n", id, job.URL, err)
		}
	}
}

func (p *WorkerPool) AddJob(url, outputDir string, id int) {
	p.Jobs <- DownloadJob{
		URL:       url,
		OutputDir: outputDir,
		ID:        id,
	}
}

func (p *WorkerPool) Wait() {
	close(p.Jobs)
	p.Wg.Wait()
	close(p.Results)
}

// ==================== Main ====================

func main() {
	if len(os.Args) < 2 {
		fmt.Println("📖 วิธีใช้งาน:")
		fmt.Printf("  %s <URL> [output_dir]          - โหลดทีละตัว\n", os.Args[0])
		fmt.Printf("  %s -batch <file> [workers]     - โหลดหลายตัวจากไฟล์\n", os.Args[0])
		fmt.Println("\n📄 ไฟล์ batch: แต่ละบรรทัดคือ URL และ output_dir (คั่นด้วย space)")
		fmt.Println("   Example: https://video1.com/m3u8 ./videos1")
		fmt.Println("   Example: https://video2.com/m3u8 ./videos2")
		os.Exit(1)
	}

	// โหมดเดี่ยว (Single)
	if os.Args[1] != "-batch" {
		url := os.Args[1]
		outputDir := "."
		if len(os.Args) > 2 {
			outputDir = os.Args[2]
		}
		os.MkdirAll(outputDir, 0755)

		wrapper := NewYtDlpWrapper(url, outputDir, 8)
		if err := wrapper.Download(); err != nil {
			fmt.Printf("❌ %v\n", err)
			os.Exit(1)
		}
		fmt.Println("\n🎉 โหลดเสร็จเรียบร้อย!")
		os.Exit(0)
	}

	// โหมด Batch (หลายวิดีโอ)
	if len(os.Args) < 3 {
		fmt.Println("❌ ต้องระบุไฟล์ batch")
		os.Exit(1)
	}

	batchFile := os.Args[2]
	numWorkers := 3 // ค่าเริ่มต้น
	if len(os.Args) > 3 {
		fmt.Sscanf(os.Args[3], "%d", &numWorkers)
	}

	// อ่านไฟล์ batch
	data, err := os.ReadFile(batchFile)
	if err != nil {
		fmt.Printf("❌ อ่านไฟล์ไม่ได้: %v\n", err)
		os.Exit(1)
	}

	lines := strings.Split(string(data), "\n")
	jobs := []DownloadJob{}
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 1 {
			continue
		}
		url := parts[0]
		outputDir := "."
		if len(parts) > 1 {
			outputDir = parts[1]
		}
		os.MkdirAll(outputDir, 0755)

		jobs = append(jobs, DownloadJob{
			URL:       url,
			OutputDir: outputDir,
			ID:        i + 1,
		})
	}

	if len(jobs) == 0 {
		fmt.Println("❌ ไม่พบ URL ในไฟล์ batch")
		os.Exit(1)
	}

	fmt.Printf("📊 พบ %d วิดีโอ, ใช้ %d worker\n", len(jobs), numWorkers)

	// สร้าง Worker Pool
	pool := NewWorkerPool(numWorkers, 8)
	pool.Start()

	// ส่งงาน
	for _, job := range jobs {
		pool.AddJob(job.URL, job.OutputDir, job.ID)
	}

	// รอผล
	go func() {
		pool.Wait()
	}()

	// แสดงผลลัพธ์แบบ real-time
	successCount := 0
	failCount := 0
	for result := range pool.Results {
		if result.Success {
			successCount++
			fmt.Printf("✅ [Job %d] สำเร็จ\n", result.ID)
		} else {
			failCount++
			fmt.Printf("❌ [Job %d] ล้มเหลว: %v\n", result.ID, result.Error)
		}
		fmt.Printf("📊 ความคืบหน้า: %d/%d (สำเร็จ %d, ล้มเหลว %d)\n",
			successCount+failCount, len(jobs), successCount, failCount)
	}

	fmt.Printf("\n🎉 สรุป: สำเร็จ %d, ล้มเหลว %d\n", successCount, failCount)
}

/*

go run main.go "url"	โหลดทีละ 1 ตัว
go run main.go "url1" "url2" "url3"	โหลด 3 ตัวพร้อมกัน
go run main.go -batch videos.txt	โหลดจากไฟล์ batch
go run main.go -batch videos.txt 5	โหลดจากไฟล์ batch ใช้ 5 workers

*/
