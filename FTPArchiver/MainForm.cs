namespace FTPArchiver;
public enum SourceType { Kistler, FTP, DigiForce }
public sealed class SourceEntry
{
    public string Path { get; set; } = "";
    public SourceType Type { get; set; } = SourceType.FTP;
}
public class MainForm : Form
{
    private readonly TextBox txtDestination;
    private readonly Button btnBrowseDest;
    private readonly ComboBox cboYear;
    private readonly CheckBox chkDryRun;
    private readonly Button btnStart;
    private readonly Button btnCancel;
    private readonly TextBox txtLog;
    private readonly Label lblStatus;
    private readonly ProgressBar pb;
    private readonly CheckBox chkSampleMode;
    private readonly Button btnCopyLog;
    private readonly CheckBox chkRobocopy;
    private CancellationTokenSource? _cts;
    private Archiver? _archiver;
    public MainForm()
    {
        Text = "Yearly Archive App";
        Width = 1000; Height = 650; StartPosition = FormStartPosition.CenterScreen;
        var lblDest = new Label { Left = 20, Top = 20, Width = 120, Text = "Archive root:" };
        txtDestination = new TextBox { Left = 140, Top = 18, Width = 720, Text = @"\\pd-pc-vfs1\Shares\LineDeviceStorage" };
        btnBrowseDest = new Button { Left = 870, Top = 16, Width = 90, Text = "Browse…" };
        btnBrowseDest.Click += (_, __) => BrowseFolder(txtDestination);
        var lblYear = new Label { Left = 20, Top = 60, Width = 120, Text = "Year:" };
        cboYear = new ComboBox { Left = 140, Top = 56, Width = 120, DropDownStyle = ComboBoxStyle.DropDownList };
        var now = DateTime.Now.Year;
        for (int y = now; y >= 2000; y--) cboYear.Items.Add(y.ToString());
        cboYear.SelectedItem = (now - 1).ToString();
        chkDryRun = new CheckBox { Left = 280, Top = 58, Width = 180, Text = "Dry run (no changes)", Checked = true };
        chkRobocopy = new CheckBox { Left = 740, Top = 58, Width = 220, Text = "Use Robocopy (FTP & Kistler)", Checked = true };
        Controls.Add(chkRobocopy);
        chkSampleMode = new CheckBox { Left = 470, Top = 58, Width = 260, Text = "Sample mode (1 per source)", Checked = false };
        chkSampleMode.CheckedChanged += (_, __) =>
        {
            if (chkSampleMode.Checked) { chkDryRun.Checked = true; chkDryRun.Enabled = false; }
            else { chkDryRun.Enabled = true; }
        };
        Controls.Add(chkSampleMode);
        btnCopyLog = new Button { Left = 870, Top = 135, Width = 90, Text = "Copy log" };
        btnCopyLog.Click += (_, __) =>
        {
            try
            {
                if (txtLog?.Text != null)
                {
                    Clipboard.SetText(txtLog.Text);
                    AppendLog("Log copied to clipboard.");
                }
                else
                {
                    AppendLog("Log is empty or unavailable.");
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(this, "Failed to copy log: " + ex.Message, "Copy log", MessageBoxButtons.OK, MessageBoxIcon.Error);
            }
        };
        Controls.Add(btnCopyLog);
        btnStart = new Button { Left = 20, Top = 100, Width = 120, Text = "Start Archive" };
        btnCancel = new Button { Left = 150, Top = 100, Width = 120, Text = "Cancel", Enabled = false };
        pb = new ProgressBar { Left = 280, Top = 100, Width = 680, Style = ProgressBarStyle.Marquee };
        lblStatus = new Label { Left = 20, Top = 135, Width = 940, Text = "Ready." };
        txtLog = new TextBox { Left = 20, Top = 160, Width = 940, Height = 430, Multiline = true, ScrollBars = ScrollBars.Both, ReadOnly = true, WordWrap = false };
        Controls.AddRange([ lblDest, txtDestination, btnBrowseDest, lblYear, cboYear, chkDryRun, btnStart, btnCancel, pb, lblStatus, txtLog ]);
        btnStart.Click += BtnStart_Click;
        btnCancel.Click += (_, __) => { _cts?.Cancel(); btnCancel.Enabled = false; };
    }
    private void BrowseFolder(TextBox target)
    {
        using var dlg = new FolderBrowserDialog() { ShowNewFolderButton = true };
        if (dlg.ShowDialog(this) == DialogResult.OK) target.Text = dlg.SelectedPath;
    }
    private async void BtnStart_Click(object? sender, EventArgs e)
    {
        if (string.IsNullOrWhiteSpace(txtDestination.Text)) { MessageBox.Show(this, "Provide a Destination root.", "Validation", MessageBoxButtons.OK, MessageBoxIcon.Warning); return; }
        if (cboYear.SelectedItem is null) { MessageBox.Show(this, "Select a Year to archive.", "Validation", MessageBoxButtons.OK, MessageBoxIcon.Warning); return; }
        int year = int.Parse((string)cboYear.SelectedItem);
        bool dryRun = chkDryRun.Checked;
        string dest = txtDestination.Text.Trim();
        txtLog.Clear();
        AppendLog($"Starting {(dryRun ? "DRY RUN" : "ARCHIVE")} for year {year}.");
        AppendLog($"Using {SourcesProvider.All.Count} hard-coded sources.");
        lblStatus.Text = "Working…";
        pb.Style = ProgressBarStyle.Marquee;
        btnStart.Enabled = false; btnCancel.Enabled = true;
        _cts = new CancellationTokenSource();
        _archiver = new Archiver(AppendLog, UpdateStatus, UpdateProgress);
        try
        {
            await Task.Run(() => _archiver.Run(new ArchiveOptions
            {
                DestinationRoot = dest,
                SelectedYear = year,
                DryRun = dryRun,
                Sources = [.. SourcesProvider.All],
                Verbose = chkSampleMode.Checked || chkDryRun.Checked,
                SampleMode = chkSampleMode.Checked,
                SampleScope = SampleScope.PerSource,
                SamplesPerFolder = 1,
                UseRobocopyForFtp = chkRobocopy.Checked,
                UseRobocopyForKistler = chkRobocopy.Checked,
                RobocopyThreads = 16,
                RobocopyLogStdout = false,
                RobocopyExe = "robocopy",
                MaxConcurrentSources = 2,
                MaxConcurrentFilesPerSource = 4
            }, _cts.Token));
        }
        catch (OperationCanceledException) { AppendLog("Operation cancelled."); }
        catch (Exception ex) { AppendLog("FATAL: " + ex); }
        finally
        {
            pb.Style = ProgressBarStyle.Continuous; pb.Value = 0;
            btnStart.Enabled = true; btnCancel.Enabled = false;
            lblStatus.Text = "Done.";
        }
    }
    private void AppendLog(string message)
    {
        if (InvokeRequired) { BeginInvoke(new Action<string>(AppendLog), message); return; }
        txtLog.AppendText($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] {message}{Environment.NewLine}");
    }
    private void UpdateStatus(string status)
    {
        if (InvokeRequired) { BeginInvoke(new Action<string>(UpdateStatus), status); return; }
        lblStatus.Text = status;
    }
    private void UpdateProgress(ProgressSnapshot snap)
    {
        if (InvokeRequired) { BeginInvoke(new Action<ProgressSnapshot>(UpdateProgress), snap); return; }
        lblStatus.Text = $"Scanned: {snap.FilesScanned:N0} | Moved: {snap.Moved:N0} | Duplicates: {snap.Duplicates:N0} | Skipped: {snap.Skipped:N0} | Errors: {snap.Errors:N0}";
    }
}