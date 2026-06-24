package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/theme"
	"fyne.io/fyne/v2/widget"
)

// ==================== Core Downloader ====================

type YtDlpWrapper struct {
	URL          string
	OutputDir    string
	FileName     string
	Concurrent   int
	RetryCount   int
	YtdlpPath    string
	IsRetry      bool
	URLHistory   []string
	mu           sync.Mutex
	Progress     float64
	Status       string
	SegmentCount int
	DoneSegments int
	Title        string
	CancelChan   chan bool
	IsRunning    bool
	ID           int
}

type YtdlFile struct {
	Filename  string `json:"filename"`
	Fragments []struct {
		Index int    `json:"index"`
		URL   string `json:"url"`
	} `json:"fragments"`
	URL string `json:"url"`
}

// ==================== Constructor ====================

var idCounter int64

func NewYtDlpWrapper(url, outputDir, fileName string, concurrent, id int) *YtDlpWrapper {
	return &YtDlpWrapper{
		URL:          url,
		OutputDir:    outputDir,
		FileName:     fileName,
		Concurrent:   concurrent,
		RetryCount:   0,
		IsRetry:      false,
		YtdlpPath:    findYtdlp(),
		URLHistory:   []string{url},
		Progress:     0,
		Status:       "⏳ รอเริ่ม",
		SegmentCount: 0,
		DoneSegments: 0,
		Title:        fileName,
		CancelChan:   make(chan bool, 1),
		IsRunning:    false,
		ID:           id,
	}
}

func findYtdlp() string {
	if path, err := exec.LookPath("yt-dlp"); err == nil {
		return path
	}
	// Try common installation paths
	commonPaths := []string{
		"/usr/local/bin/yt-dlp",
		"/usr/bin/yt-dlp",
		filepath.Join(os.Getenv("HOME"), ".local/bin/yt-dlp"),
	}
	for _, path := range commonPaths {
		if _, err := os.Stat(path); err == nil {
			return path
		}
	}
	return "yt-dlp"
}

func (w *YtDlpWrapper) updateProgress(line string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if strings.Contains(line, "Downloading") && strings.Contains(line, "fragments") {
		re := regexp.MustCompile(`(\d+)\s+fragments`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &w.SegmentCount)
		}
	}

	if strings.Contains(line, "fragment") {
		re := regexp.MustCompile(`fragment\s+(\d+)`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			fmt.Sscanf(matches[1], "%d", &w.DoneSegments)
			if w.SegmentCount > 0 {
				w.Progress = float64(w.DoneSegments) / float64(w.SegmentCount) * 100
			}
		}
	}

	if strings.Contains(line, "Merging") {
		w.Status = "🔄 กำลังรวมไฟล์..."
	}
	if strings.Contains(line, "100%") || strings.Contains(line, "Merging completed") {
		w.Progress = 100
		w.Status = "✅ เสร็จ!"
	}
	if strings.Contains(line, "ERROR") || strings.Contains(line, "502") || strings.Contains(line, "403") {
		w.Status = "❌ Error: " + line
	}

	// ดึงชื่อวิดีโอ
	if strings.Contains(line, "[download]") && strings.Contains(line, ".mp4") {
		re := regexp.MustCompile(`\[download\]\s+(.+?\.mp4)`)
		if matches := re.FindStringSubmatch(line); len(matches) > 1 {
			w.Title = matches[1]
		}
	}
}

func (w *YtDlpWrapper) GetProgress() (float64, string, string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.Progress, w.Status, w.Title
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

// ✅ แก้ไข: ไม่ใช้เลขนำหน้า และบันทึกโดยตรง
func (w *YtDlpWrapper) runYtdlp(url string, statusChan chan<- string) error {

	// ✅ เพิ่ม Context timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	outputPath := filepath.Join(w.OutputDir, w.FileName)

	args := []string{
		"--no-progress",
		"--newline",
		"-N", fmt.Sprintf("%d", w.Concurrent),
		"--fragment-retries", "3",
		"--retries", "3",
		"--socket-timeout", "30",
		"-o", outputPath,
		"--limit-rate", "5M", // ✅ จำกัดความเร็ว เพื่อลดภาระ
		url,
	}

	// ✅ ใช้ CommandContext
	cmd := exec.CommandContext(ctx, w.YtdlpPath, args...)

	statusChan <- "🔄 กำลังดาวน์โหลด..."

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

	// Monitor cancellation
	go func() {
		<-w.CancelChan
		cmd.Process.Kill()
	}()

	// ✅ ใช้ WaitGroup สำหรับจัดการ goroutines
	var wg sync.WaitGroup
	errorChan := make(chan string, 100) // ✅ เพิ่ม buffer

	// Read stdout
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		// ✅ จำกัดขนาด buffer เพื่อป้องกัน memory leak
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			w.updateProgress(line)

			// ✅ ใช้ select เพื่อป้องกัน blocking
			select {
			case statusChan <- line:
			default:
				// ถ้า statusChan เต็ม ก็ข้ามไป
			}

			if strings.Contains(line, "HTTP Error") ||
				strings.Contains(line, "ERROR:") {
				select {
				case errorChan <- line:
				default:
				}
			}
		}
	}()

	// Read stderr
	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		scanner.Buffer(make([]byte, 64*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			select {
			case statusChan <- "[stderr] " + line:
			default:
			}

			if strings.Contains(line, "HTTP Error") ||
				strings.Contains(line, "ERROR:") {
				select {
				case errorChan <- line:
				default:
				}
			}
		}
	}()

	// ✅ รอให้ goroutines เสร็จ
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
		close(errorChan)
	}()

	// ✅ รอด้วย timeout
	select {
	case <-done:
		// เสร็จปกติ
	case <-time.After(30 * time.Minute):
		cmd.Process.Kill()
		return fmt.Errorf("การดาวน์โหลดใช้เวลานานเกินไป")
	case <-w.CancelChan:
		cmd.Process.Kill()
		return fmt.Errorf("ผู้ใช้ยกเลิก")
	}

	err = cmd.Wait()

	// ✅ สร้าง log file
	logFile, err := os.Create(filepath.Join(w.OutputDir, fmt.Sprintf("%s.log", w.FileName)))
	if err == nil {
		defer logFile.Close()
		logger := log.New(logFile, "", log.LstdFlags)
		logger.Printf("Starting download: %s", url)
		logger.Printf("Args: %v", args)

		// Collect errors
		var errorMessages []string
		for errMsg := range errorChan {
			errorMessages = append(errorMessages, errMsg)
		}

		if len(errorMessages) > 0 {
			return fmt.Errorf("HTTP Error: %s", strings.Join(errorMessages, "; "))
		}

		if err != nil {
			logger.Printf("Error: %v", err)
		}
	}

	// ✅ ตรวจสอบ Context error
	select {
	case <-ctx.Done():
		return fmt.Errorf("timeout หรือถูกยกเลิก: %v", ctx.Err())
	default:
	}
	// ✅ ไม่ต้องเปลี่ยนชื่อไฟล์ เพราะบันทึกโดยตรงแล้ว

	return nil
}

func (w *YtDlpWrapper) Download(statusChan chan<- string) error {
	currentURL := w.URL
	attempt := 0
	w.IsRunning = true
	defer func() { w.IsRunning = false }()

	for {
		attempt++
		statusChan <- fmt.Sprintf("🔄 รอบที่ %d", attempt)

		err := w.runYtdlp(currentURL, statusChan)

		if err == nil {
			w.Progress = 100
			w.Status = "✅ เสร็จ!"
			statusChan <- "✅ ดาวน์โหลดเสร็จ!"
			return nil
		}

		// ตรวจสอบว่าถูกยกเลิก
		select {
		case <-w.CancelChan:
			return fmt.Errorf("ผู้ใช้ยกเลิก")
		default:
		}

		errMsg := err.Error()
		statusChan <- fmt.Sprintf("⚠️ Error: %s", errMsg)
		w.Status = "❌ " + errMsg

		// ถ้าเป็น HTTP Error ให้หยุดรอผู้ใช้เปลี่ยน URL
		if strings.Contains(errMsg, "HTTP Error") ||
			strings.Contains(errMsg, "403") ||
			strings.Contains(errMsg, "404") ||
			strings.Contains(errMsg, "502") {

			statusChan <- "⏸️ กรุณาเปลี่ยน URL (HTTP Error)"
			return fmt.Errorf("HTTP Error: %s", errMsg)
		}

		// ถ้า Error อื่นๆ ให้ลองใหม่อัตโนมัติ
		statusChan <- "🔄 ลองใหม่อัตโนมัติ..."
		time.Sleep(2 * time.Second)
		w.IsRetry = true
	}
}

func (w *YtDlpWrapper) autoGenerateNewURL(oldURL string) string {
	// ตัวอย่าง: ถ้า URL เปลี่ยนแค่ token หรือ timestamp
	// ตรงนี้คุณสามารถเขียน logic ของคุณเอง
	return ""
}

// ==================== GUI Application ====================

// ==================== GUI Application ====================

type DownloadItem struct {
	URL            string
	OutputDir      string
	FileName       string
	FileNameLocked bool // ✅ เพิ่ม: ใช้ล็อคชื่อไฟล์
	Wrapper        *YtDlpWrapper
	Status         string
	Progress       float64
	Title          string
	ID             int
}

type App struct {
	Window        fyne.Window
	DownloadList  *widget.List
	Items         []*DownloadItem
	mu            sync.Mutex
	QueueCount    *widget.Label
	AddBtn        *widget.Button
	Wg            sync.WaitGroup
	Semaphore     chan struct{} // ✅ เพิ่ม Semaphore สำหรับจำกัด concurrent
	MaxConcurrent int           // ✅ จำกัดจำนวนที่โหลดพร้อมกัน
}

func NewApp() *App {
	return &App{
		Items:         []*DownloadItem{},
		Semaphore:     make(chan struct{}, 3), // ✅ จำกัดที่ 3 พร้อมกัน
		MaxConcurrent: 3,
	}
}

func (a *App) AddDownload(url, outputDir, fileName string) {
	a.mu.Lock()
	id := int(atomic.AddInt64(&idCounter, 1))

	if fileName == "" {
		fileName = filepath.Base(url)
		if strings.HasSuffix(fileName, ".m3u8") {
			fileName = strings.TrimSuffix(fileName, ".m3u8") + ".mp4"
		}
	}

	// ✅ ตรวจสอบและเพิ่มนามสกุล
	if !strings.Contains(fileName, ".") {
		fileName = fileName + ".mp4"
	}

	item := &DownloadItem{
		URL:            url,
		OutputDir:      outputDir,
		FileName:       fileName,
		FileNameLocked: false, // เริ่มต้นยังไม่ล็อค
		Wrapper:        NewYtDlpWrapper(url, outputDir, fileName, 8, id),
		Status:         "⏳ กำลังเริ่ม...",
		Progress:       0,
		Title:          fileName,
		ID:             id,
	}
	a.Items = append(a.Items, item)
	a.mu.Unlock()

	a.UpdateUI()

	// ✅ ใช้ Semaphore เพื่อจำกัดจำนวน
	a.Wg.Add(1)
	go func() {
		a.Semaphore <- struct{}{}        // รอจนกว่าจะมีช่องว่าง
		defer func() { <-a.Semaphore }() // ปล่อยช่องเมื่อเสร็จ
		a.startDownload(item)
	}()
}

func (a *App) ShowAddDialog() {
	urlEntry := widget.NewEntry()
	urlEntry.SetPlaceHolder("ใส่ URL วิดีโอ...")

	fileNameEntry := widget.NewEntry()
	fileNameEntry.SetPlaceHolder("ชื่อไฟล์ (ไม่ต้องใส่นามสกุล)")

	outputEntry := widget.NewEntry()
	outputEntry.SetPlaceHolder("โฟลเดอร์ปลายทาง (ค่าเริ่มต้น: ./downloads)")
	outputEntry.Text = "./downloads"

	items := []*widget.FormItem{
		widget.NewFormItem("URL", urlEntry),
		widget.NewFormItem("ชื่อไฟล์", fileNameEntry),
		widget.NewFormItem("Output", outputEntry),
	}

	dialog.ShowForm("➕ เพิ่มวิดีโอ", "เพิ่ม", "ยกเลิก", items,
		func(ok bool) {
			if ok {
				url := urlEntry.Text
				fileName := fileNameEntry.Text
				output := outputEntry.Text

				if output == "" {
					output = "./downloads"
				}

				if fileName == "" {
					fileName = filepath.Base(url)
					if strings.HasSuffix(fileName, ".m3u8") {
						fileName = strings.TrimSuffix(fileName, ".m3u8") + ".mp4"
					}
				}

				if !strings.Contains(fileName, ".") {
					fileName = fileName + ".mp4"
				}

				os.MkdirAll(output, 0755)
				a.AddDownload(url, output, fileName)
			}
		}, a.Window)
}

// ✅ แก้ไข: ตรวจสอบว่าล็อคชื่อแล้วหรือยัง
func (a *App) ShowRenameDialog(item *DownloadItem) {
	// ✅ ถ้าล็อคแล้ว (กำลังดาวน์โหลด) ไม่ให้เปลี่ยนชื่อ
	if item.FileNameLocked {
		dialog.ShowInformation("ไม่สามารถเปลี่ยนชื่อได้",
			"❌ ไม่สามารถเปลี่ยนชื่อไฟล์ได้หลังจากเริ่มดาวน์โหลดแล้ว\nกรุณารอให้ดาวน์โหลดเสร็จก่อน",
			a.Window)
		return
	}

	nameEntry := widget.NewEntry()
	nameEntry.Text = item.FileName
	nameEntry.SetPlaceHolder("ชื่อไฟล์ใหม่...")

	items := []*widget.FormItem{
		widget.NewFormItem("ชื่อไฟล์ใหม่", nameEntry),
	}

	dialog.ShowForm("✏️ เปลี่ยนชื่อไฟล์", "เปลี่ยน", "ยกเลิก", items,
		func(ok bool) {
			if ok {
				newName := nameEntry.Text
				if newName != "" && newName != item.FileName {
					if !strings.Contains(newName, ".") {
						newName = newName + ".mp4"
					}
					item.FileName = newName
					item.Title = newName
					item.Wrapper.FileName = newName
					a.UpdateUI()
				}
			}
		}, a.Window)
}

// ✅ แก้ไข: ล็อคชื่อไฟล์เมื่อเริ่มดาวน์โหลด
func (a *App) startDownload(item *DownloadItem) {

	defer func() {
		if r := recover(); r != nil {
			// ✅ จัดการ panic
			item.Status = fmt.Sprintf("💥 Panic: %v", r)
			a.UpdateUI()
			// ✅ Log panic
			log.Printf("Recovered from panic: %v", r)
		}
		a.Wg.Done()
	}()

	// ✅ ล็อคชื่อไฟล์
	item.FileNameLocked = true

	// ✅ ใช้ buffer ขนาดเล็กลง
	statusChan := make(chan string, 50)

	// ✅ รีดักซ์ความถี่ในการอัปเดต UI
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		err := item.Wrapper.Download(statusChan)
		if err != nil {
			// ... handle error ...
		}
	}()

	for {
		select {
		case msg, ok := <-statusChan:
			if !ok {
				return
			}
			item.Status = msg
			progress, _, title := item.Wrapper.GetProgress()
			item.Progress = progress
			if title != "" && title != item.Title {
				item.Title = title
			}
			// ✅ ไม่ต้อง UpdateUI ทุกครั้ง ใช้ ticker แทน
		case <-ticker.C:
			a.UpdateUI()
		}
	}
}

func (a *App) CancelDownload(index int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if index < len(a.Items) {
		item := a.Items[index]
		if item.Wrapper.IsRunning {
			select {
			case item.Wrapper.CancelChan <- true:
			default:
			}
			item.Status = "⏹️ ยกเลิกแล้ว"
			a.UpdateUI()
		}
	}
}

func (a *App) RemoveDownload(index int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if index < len(a.Items) {
		item := a.Items[index]
		if item.Wrapper.IsRunning {
			select {
			case item.Wrapper.CancelChan <- true:
			default:
			}
			// ✅ รอให้หยุดทำงาน
			time.Sleep(100 * time.Millisecond)
		}

		// ✅ ลบไฟล์ .ytdl ที่ค้างอยู่
		ytdlFiles, _ := filepath.Glob(filepath.Join(item.OutputDir, "*.ytdl"))
		for _, f := range ytdlFiles {
			os.Remove(f)
		}

		a.Items = append(a.Items[:index], a.Items[index+1:]...)
		a.UpdateUI()
	}
}

func (a *App) UpdateUI() {
	// ใช้ fyne.Do() เพื่ออัปเดต UI ใน Main Thread
	fyne.Do(func() {
		if a.DownloadList != nil {
			a.DownloadList.Refresh()
		}
		if a.QueueCount != nil {
			a.mu.Lock()
			count := len(a.Items)
			a.mu.Unlock()
			a.QueueCount.SetText(fmt.Sprintf("📊 คิว: %d", count))
		}
	})
}

// เพิ่มใน ShowAddDialog หรือสร้าง dialog แยก
func (a *App) ShowSettingsDialog() {
	concurrentEntry := widget.NewEntry()
	concurrentEntry.Text = fmt.Sprintf("%d", a.MaxConcurrent)
	concurrentEntry.SetPlaceHolder("จำนวนที่โหลดพร้อมกัน (1-10)")

	items := []*widget.FormItem{
		widget.NewFormItem("โหลดพร้อมกันสูงสุด", concurrentEntry),
	}

	dialog.ShowForm("⚙️ ตั้งค่า", "บันทึก", "ยกเลิก", items,
		func(ok bool) {
			if ok {
				val, err := strconv.Atoi(concurrentEntry.Text)
				if err == nil && val >= 1 && val <= 10 {
					a.MaxConcurrent = val
					// ✅ ปรับขนาด Semaphore
					a.Semaphore = make(chan struct{}, val)
					dialog.ShowInformation("สำเร็จ",
						fmt.Sprintf("ตั้งค่าโหลดพร้อมกันสูงสุดที่ %d รายการ", val),
						a.Window)
				} else {
					dialog.ShowInformation("ข้อผิดพลาด",
						"กรุณาใส่ตัวเลขระหว่าง 1-10",
						a.Window)
				}
			}
		}, a.Window)
}

func (a *App) ShowURLDialog(item *DownloadItem) {
	urlEntry := widget.NewEntry()
	urlEntry.SetPlaceHolder("ใส่ URL ใหม่...")
	urlEntry.Text = item.URL

	// ✅ แสดง error ปัจจุบัน
	errorLabel := widget.NewLabel("⚠️ " + item.Status)
	errorLabel.Wrapping = fyne.TextWrapWord
	errorLabel.TextStyle = fyne.TextStyle{Bold: true}
	errorLabel.Importance = widget.DangerImportance

	items := []*widget.FormItem{
		widget.NewFormItem("", errorLabel),
		widget.NewFormItem("URL ใหม่", urlEntry),
	}

	dialog.ShowForm("🔄 เปลี่ยน URL", "เปลี่ยนและโหลดต่อ", "ยกเลิก", items,
		func(ok bool) {
			if ok {
				newURL := urlEntry.Text
				if newURL != "" && newURL != item.URL {
					// ✅ อัปเดต URL
					item.URL = newURL
					item.Wrapper.URL = newURL
					item.Wrapper.IsRetry = true
					item.Status = "🔄 กำลังลองใหม่..."
					a.UpdateUI()

					// ✅ รอสักครู่แล้วเริ่มใหม่
					time.Sleep(500 * time.Millisecond)
					a.Wg.Add(1)
					go a.startDownload(item)
				} else if newURL == item.URL {
					item.Status = "⏳ URL เดิม, ลองใหม่..."
					a.UpdateUI()
					a.Wg.Add(1)
					go a.startDownload(item)
				}
			} else {
				item.Status = "⏹️ รอผู้ใช้เปลี่ยน URL"
				a.UpdateUI()
			}
		}, a.Window)
}

func main() {
	// สร้างแอป
	myApp := app.New()
	myWindow := myApp.NewWindow("🎬 IDM Clone")
	myWindow.Resize(fyne.NewSize(800, 500))

	appInstance := NewApp()
	appInstance.Window = myWindow
	appInstance.MaxConcurrent = 2 // ✅ ตั้งค่าเริ่มต้นที่ 2
	appInstance.Semaphore = make(chan struct{}, appInstance.MaxConcurrent)

	// สร้าง UI Components
	addBtn := widget.NewButtonWithIcon("➕ เพิ่ม URL", theme.ContentAddIcon(), func() {
		appInstance.ShowAddDialog()
	})

	// ✅ เพิ่มปุ่มตั้งค่า
	settingsBtn := widget.NewButtonWithIcon("⚙️", theme.SettingsIcon(), func() {
		appInstance.ShowSettingsDialog()
	})

	appInstance.AddBtn = addBtn

	appInstance.QueueCount = widget.NewLabel("📊 คิว: 0")

	// สร้างรายการดาวน์โหลด
	appInstance.DownloadList = widget.NewList(
		func() int {
			appInstance.mu.Lock()
			defer appInstance.mu.Unlock()
			return len(appInstance.Items)
		},
		func() fyne.CanvasObject {
			// Template สำหรับแต่ละรายการ
			progressBar := widget.NewProgressBar()
			progressBar.Min = 0
			progressBar.Max = 100
			titleLabel := widget.NewLabel("ชื่อวิดีโอ")
			statusLabel := widget.NewLabel("สถานะ")
			urlLabel := widget.NewLabel("URL")

			btnContainer := container.NewHBox(
				widget.NewButtonWithIcon("", theme.CancelIcon(), func() {}),
				widget.NewButtonWithIcon("", theme.DeleteIcon(), func() {}),
				widget.NewButtonWithIcon("", theme.ConfirmIcon(), func() {}),
				widget.NewButtonWithIcon("", theme.DocumentCreateIcon(), func() {}), // ✅ ปุ่มเปลี่ยนชื่อ
			)

			return container.NewVBox(
				container.NewHBox(
					titleLabel,
					widget.NewLabel("|"),
					urlLabel,
				),
				container.NewHBox(
					progressBar,
					statusLabel,
					btnContainer,
				),
			)
		},
		// ในฟังก์ชัน Update CanvasObject
		func(id int, obj fyne.CanvasObject) {
			appInstance.mu.Lock()
			defer appInstance.mu.Unlock()

			if id >= len(appInstance.Items) {
				return
			}

			item := appInstance.Items[id]
			container := obj.(*fyne.Container)
			topBox := container.Objects[0].(*fyne.Container)
			bottomBox := container.Objects[1].(*fyne.Container)

			titleLabel := topBox.Objects[0].(*widget.Label)
			urlLabel := topBox.Objects[2].(*widget.Label)

			progressBar := bottomBox.Objects[0].(*widget.ProgressBar)
			statusLabel := bottomBox.Objects[1].(*widget.Label)
			btnContainer := bottomBox.Objects[2].(*fyne.Container)

			// อัปเดตข้อมูล
			title := item.Title
			if len(title) > 35 {
				title = title[:35] + "..."
			}
			titleLabel.SetText("📹 " + title)

			url := item.URL
			if len(url) > 40 {
				url = url[:40] + "..."
			}
			urlLabel.SetText(url)

			progressBar.SetValue(item.Progress)

			statusText := item.Status
			if len(statusText) > 50 {
				statusText = statusText[:50] + "..."
			}
			statusLabel.SetText(statusText)

			// อัปเดตปุ่ม
			cancelBtn := btnContainer.Objects[0].(*widget.Button)
			cancelBtn.OnTapped = func() {
				appInstance.CancelDownload(id)
			}

			removeBtn := btnContainer.Objects[1].(*widget.Button)
			removeBtn.OnTapped = func() {
				appInstance.RemoveDownload(id)
			}

			changeURLBtn := btnContainer.Objects[2].(*widget.Button)
			changeURLBtn.OnTapped = func() {
				appInstance.ShowURLDialog(item)
			}

			// ✅ เพิ่มปุ่มเปลี่ยนชื่อ
			renameBtn := btnContainer.Objects[3].(*widget.Button)
			renameBtn.OnTapped = func() {
				appInstance.ShowRenameDialog(item)
			}
			renameBtn.Icon = theme.DocumentCreateIcon()
		},
	)

	// Layout
	topBar := container.NewHBox(
		addBtn,
		settingsBtn,
		widget.NewSeparator(),
		appInstance.QueueCount,
		appInstance.QueueCount,
		widget.NewLabel("|"),
		widget.NewLabel("💡 คลิก 🔄 เพื่อเปลี่ยน URL"),
	)

	content := container.NewBorder(
		topBar,
		nil,
		nil,
		nil,
		appInstance.DownloadList,
	)

	myWindow.SetContent(content)

	// เริ่ม Goroutine สำหรับอัปเดต UI อัตโนมัติ
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			appInstance.UpdateUI()
		}
	}()

	myWindow.ShowAndRun()
}
