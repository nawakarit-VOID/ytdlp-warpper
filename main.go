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

func (w *YtDlpWrapper) runYtdlp(url string, statusChan chan<- string) error {
	outputPath := filepath.Join(w.OutputDir, w.FileName)
	tempOutput := filepath.Join(w.OutputDir, fmt.Sprintf("%d_%s", w.ID, w.FileName))

	args := []string{
		"--no-progress",
		"--newline",
		"-N", fmt.Sprintf("%d", w.Concurrent),
		"--fragment-retries", "3",
		"--retries", "3",
		"--socket-timeout", "30",
		"-o", tempOutput,
		url,
	}

	if w.IsRetry {
		args = append([]string{"--continue", "--no-overwrites"}, args...)
	}

	cmd := exec.Command(w.YtdlpPath, args...)

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

	errorChan := make(chan string, 10)
	doneChan := make(chan bool, 2)

	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Text()
			w.updateProgress(line)
			statusChan <- line

			if strings.Contains(line, "HTTP Error") ||
				strings.Contains(line, "ERROR:") ||
				strings.Contains(line, "fragment failed") ||
				strings.Contains(line, "segments failed") ||
				strings.Contains(line, "Got error:") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("scanner error: %v", err)
		}
		doneChan <- true
	}()

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			statusChan <- "[stderr] " + line

			if strings.Contains(line, "HTTP Error") ||
				strings.Contains(line, "ERROR:") ||
				strings.Contains(line, "403") ||
				strings.Contains(line, "404") ||
				strings.Contains(line, "502") {
				errorChan <- line
			}
		}
		if err := scanner.Err(); err != nil {
			errorChan <- fmt.Sprintf("stderr scanner error: %v", err)
		}
		doneChan <- true
	}()

	select {
	case <-w.CancelChan:
		cmd.Process.Kill()
		return fmt.Errorf("ผู้ใช้ยกเลิก")
	case <-doneChan:
		<-doneChan
	}

	err = cmd.Wait()

	select {
	case errMsg := <-errorChan:
		return fmt.Errorf("HTTP Error: %s", errMsg)
	default:
	}

	if err != nil {
		return err
	}

	if tempOutput != outputPath {
		if _, err := os.Stat(tempOutput); err == nil {
			os.Rename(tempOutput, outputPath)
			statusChan <- fmt.Sprintf("📝 เปลี่ยนชื่อไฟล์เป็น: %s", w.FileName)
		}
	}

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

		select {
		case <-w.CancelChan:
			return fmt.Errorf("ผู้ใช้ยกเลิก")
		default:
		}

		errMsg := err.Error()
		statusChan <- fmt.Sprintf("⚠️ Error: %s", errMsg)
		w.Status = "❌ " + errMsg

		if strings.Contains(errMsg, "HTTP Error") ||
			strings.Contains(errMsg, "403") ||
			strings.Contains(errMsg, "404") ||
			strings.Contains(errMsg, "502") {

			statusChan <- "⏸️ กรุณาเปลี่ยน URL (HTTP Error)"
			return fmt.Errorf("HTTP Error: %s", errMsg)
		}

		statusChan <- "🔄 ลองใหม่อัตโนมัติ..."
		time.Sleep(2 * time.Second)
		w.IsRetry = true
	}
}

/*
func (w *YtDlpWrapper) autoGenerateNewURL(oldURL string) string {
	// ตัวอย่าง: ถ้า URL เปลี่ยนแค่ token หรือ timestamp
	// ตรงนี้คุณสามารถเขียน logic ของคุณเอง
	return ""
}
*/

// ==================== GUI Application ====================

type DownloadItem struct {
	URL       string
	OutputDir string
	FileName  string
	Wrapper   *YtDlpWrapper
	Status    string
	Progress  float64
	Title     string
	ID        int
}

type App struct {
	Window       fyne.Window
	DownloadList *widget.List
	Items        []*DownloadItem
	mu           sync.Mutex
	QueueCount   *widget.Label
	AddBtn       *widget.Button
	Wg           sync.WaitGroup // ✅ เพิ่ม
}

func NewApp() *App {
	return &App{
		Items: []*DownloadItem{},
	}
}

func (a *App) AddDownload(url, outputDir, fileName string) {
	a.mu.Lock()
	id := len(a.Items) + 1

	if fileName == "" {
		fileName = filepath.Base(url)
		if strings.HasSuffix(fileName, ".m3u8") {
			fileName = strings.TrimSuffix(fileName, ".m3u8") + ".mp4"
		}
	}

	if !strings.Contains(fileName, ".") {
		fileName = fileName + ".mp4"
	}

	item := &DownloadItem{
		URL:       url,
		OutputDir: outputDir,
		FileName:  fileName,
		Wrapper:   NewYtDlpWrapper(url, outputDir, fileName, 8, id),
		Status:    "⏳ กำลังเริ่ม...",
		Progress:  0,
		Title:     fileName,
		ID:        id,
	}
	a.Items = append(a.Items, item)
	a.mu.Unlock()

	a.UpdateUI()

	a.Wg.Add(1)
	go a.startDownload(item)
}

func (a *App) startDownload(item *DownloadItem) {
	defer a.Wg.Done()

	statusChan := make(chan string, 100)

	go func() {
		err := item.Wrapper.Download(statusChan)
		if err != nil {
			if strings.Contains(err.Error(), "HTTP Error") {
				item.Status = "⚠️ " + err.Error()
				a.UpdateUI()

				fyne.Do(func() {
					a.ShowURLDialog(item)
				})
			} else {
				item.Status = "❌ " + err.Error()
				a.UpdateUI()
			}
		}
	}()

	for msg := range statusChan {
		item.Status = msg
		progress, _, title := item.Wrapper.GetProgress()
		item.Progress = progress
		if title != "" && title != item.Title {
			item.Title = title
		}
		a.UpdateUI()
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
		}
		a.Items = append(a.Items[:index], a.Items[index+1:]...)
		a.UpdateUI()
	}
}

func (a *App) UpdateUI() {
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

func (a *App) ShowRenameDialog(item *DownloadItem) {
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

func (a *App) ShowURLDialog(item *DownloadItem) {
	urlEntry := widget.NewEntry()
	urlEntry.SetPlaceHolder("ใส่ URL ใหม่...")
	urlEntry.Text = item.URL

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
					item.URL = newURL
					item.Wrapper.URL = newURL
					item.Wrapper.IsRetry = true
					item.Status = "🔄 กำลังลองใหม่..."
					a.UpdateUI()

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
	myApp := app.New()
	myWindow := myApp.NewWindow("🎬 IDM Clone - ตัวโหลดวิดีโอ")
	myWindow.Resize(fyne.NewSize(800, 500))

	appInstance := NewApp()
	appInstance.Window = myWindow

	addBtn := widget.NewButtonWithIcon("➕ เพิ่ม URL", theme.ContentAddIcon(), func() {
		appInstance.ShowAddDialog()
	})
	appInstance.AddBtn = addBtn

	appInstance.QueueCount = widget.NewLabel("📊 คิว: 0")

	appInstance.DownloadList = widget.NewList(
		func() int {
			appInstance.mu.Lock()
			defer appInstance.mu.Unlock()
			return len(appInstance.Items)
		},
		func() fyne.CanvasObject {
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
				widget.NewButtonWithIcon("", theme.DocumentCreateIcon(), func() {}),
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

			renameBtn := btnContainer.Objects[3].(*widget.Button)
			renameBtn.OnTapped = func() {
				appInstance.ShowRenameDialog(item)
			}
			renameBtn.Icon = theme.DocumentCreateIcon()
		},
	)

	topBar := container.NewHBox(
		addBtn,
		widget.NewSeparator(),
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

	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			appInstance.UpdateUI()
		}
	}()

	myWindow.ShowAndRun()
}
