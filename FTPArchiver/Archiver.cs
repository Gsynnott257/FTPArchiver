using System.Globalization;
using System.Text.RegularExpressions;
using System.Threading.Channels;
using System.Diagnostics;
namespace FTPArchiver;
public enum SampleScope { PerSource, PerLeafFolder }
public sealed class ArchiveOptions
{
    public required string DestinationRoot { get; set; }
    public required int SelectedYear { get; set; }
    public required bool DryRun { get; set; }
    public required List<SourceEntry> Sources { get; set; }
    public bool Verbose { get; set; } = true;
    public bool SampleMode { get; set; } = false;
    public SampleScope SampleScope { get; set; } = SampleScope.PerSource;
    public int SamplesPerFolder { get; set; } = 1;
    public int MaxConcurrentSources { get; set; } = 2;
    public int MaxConcurrentFilesPerSource { get; set; } = 4;
    public int MaxRetries { get; set; } = 3;
    public int RetryBackoffMs { get; set; } = 500;
    public bool UseRobocopyForFtp { get; set; } = true;
    public bool UseRobocopyForKistler { get; set; } = true;
    public int RobocopyThreads { get; set; } = 16;
    public string RobocopyExe { get; set; } = "robocopy";
    public bool RobocopyLogStdout { get; set; } = false;
}
public readonly struct ProgressSnapshot(long scanned, long moved, long dupes, long skipped, long errors)
{
    public long FilesScanned { get; } = scanned;
    public long Moved { get; } = moved;
    public long Duplicates { get; } = dupes;
    public long Skipped { get; } = skipped;
    public long Errors { get; } = errors;
}
public sealed partial class Archiver(Action<string> log, Action<string> status, Action<ProgressSnapshot> progress)
{
    private long _scanned, _moved, _dupes, _skipped, _errors;
    private static readonly Regex YearSegment = new(@"^\d{4}$", RegexOptions.Compiled);
    private static readonly Regex DateY_M_D_Underscore = new(@"(\d{4})_(\d{2})_(\d{2})", RegexOptions.Compiled);
    private static readonly Regex DateY_M_D_Dash = new(@"(\d{4})-(\d{2})-(\d{2})", RegexOptions.Compiled);
    private static readonly Regex DateYYYYMMDD = new(@"(^|[^0-9])(20\d{2}|19\d{2})(\d{2})(\d{2})([^0-9]|$)", RegexOptions.Compiled);
    private static readonly HashSet<string> FtpExts = new(StringComparer.OrdinalIgnoreCase)
    {
        ".jpg",".jpeg",".png",".bmp",".tif",".tiff",".gif",".webp",".jfif",
        ".svg"
    };
    private static readonly Regex DigiForceStationRegex = new(@"^SA\d+_OP\d+", RegexOptions.IgnoreCase | RegexOptions.Compiled);
    private static bool TryGetDigiForceStation(string srcRoot, string file, out string station)
    {
        station = string.Empty;
        var rel = Path.GetRelativePath(srcRoot, file);
        var parts = SplitPath(rel);
        int idxYear = IndexOfYearSegment(parts);
        if (idxYear < 0) return false;
        int i = idxYear + 1;
        if (i < parts.Length && Regex.IsMatch(parts[i], @"^\d{4}$"))
            i++;
        if (i < parts.Length)
        {
            var candidate = parts[i];
            if (DigiForceStationRegex.IsMatch(candidate))
            {
                station = candidate;
                return true;
            }
        }
        return false;
    }
    private static Channel<string> CreateFileChannel(int capacity = 2048)
    {
        var ch = Channel.CreateBounded<string>(new BoundedChannelOptions(capacity)
        {
            SingleWriter = true,
            SingleReader = false,
            FullMode = BoundedChannelFullMode.Wait
        });
        return ch;
    }
    public void Run(ArchiveOptions opt, CancellationToken token)
    {
        string dstRoot = NormalizeRoot(opt.DestinationRoot);
        log($"Destination: {dstRoot}");
        log($"Year: {opt.SelectedYear}");
        log(opt.DryRun ? "Mode: DRY RUN" : "Mode: LIVE");
        if (opt.SampleMode)
        {
            log($"Sample mode: {(opt.SampleScope == SampleScope.PerSource ? "1 file per top-level source" : "1 per folder")} (dry run enforced).");
            opt.DryRun = true;
        }
        using var sourceSemaphore = new SemaphoreSlim(Math.Max(1, opt.MaxConcurrentSources));
        var sourceTasks = new List<Task>();
        foreach (var src in opt.Sources)
        {
            token.ThrowIfCancellationRequested();
            awaitAcquire(sourceSemaphore, token);
            var srcCopy = src;
            var t = Task.Run(async () =>
            {
                try { await ProcessSingleSourceAsync(srcCopy, dstRoot, opt, token); }
                finally { sourceSemaphore.Release(); }
            }, token);
            sourceTasks.Add(t);
        }
        Task.WaitAll([.. sourceTasks], token);
        progress(new ProgressSnapshot(_scanned, _moved, _dupes, _skipped, _errors));
        status("Completed.");
        static void awaitAcquire(SemaphoreSlim sem, CancellationToken ct) => sem.Wait(ct);
    }
    private async Task ProcessSingleSourceAsync(SourceEntry src, string dstRoot, ArchiveOptions opt, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(src.Path))
        {
            log("WARN: Source path empty.");
            Interlocked.Increment(ref _errors);
            return;
        }
        var resolved = ResolveExistingPath(src.Path);
        if (!Directory.Exists(resolved))
        {
            log($"WARN: Source missing or not found → {src.Path}");
            Interlocked.Increment(ref _errors);
            return;
        }
        var srcRoot = NormalizeRoot(resolved);
        if (!srcRoot.Equals(src.Path, StringComparison.OrdinalIgnoreCase))
            log($"Resolved source: {src.Path} → {srcRoot}");
        log($"==> Processing [{src.Type}] {srcRoot}");
        if (!opt.SampleMode)
        {
            bool handledByRobo = false;
            if (src.Type == SourceType.FTP && opt.UseRobocopyForFtp) handledByRobo = TryRobocopyFtpSource(srcRoot, dstRoot, opt);
            else if (src.Type == SourceType.Kistler && opt.UseRobocopyForKistler) handledByRobo = TryRobocopyKistlerSource(srcRoot, dstRoot, opt);
            if (handledByRobo) return;
        }
        bool sampledThisSource = false;
        HashSet<string>? sampledStations = null;
        if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource && src.Type == SourceType.DigiForce)
            sampledStations = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        bool folderSkipLogs = opt.Verbose && !opt.SampleMode;
        var ch = CreateFileChannel();
        var writer = ch.Writer;
        var reader = ch.Reader;
        var producer = Task.Run(async () =>
        {
            try
            {
                foreach (var file in EnumerateAllFiles(srcRoot, opt.SelectedYear, folderSkipLogs, log, token))
                {
                    token.ThrowIfCancellationRequested();
                    if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource && src.Type != SourceType.DigiForce && sampledThisSource) break;
                    await writer.WriteAsync(file, token);
                }
            }
            finally { writer.TryComplete(); }
        }, token);
        int workers = Math.Max(1, opt.MaxConcurrentFilesPerSource);
        var consumers = Enumerable.Range(0, workers).Select(_ => Task.Run(async () =>
        {
            while (await reader.WaitToReadAsync(token))
            {
                while (reader.TryRead(out var file))
                {
                    token.ThrowIfCancellationRequested();
                    Interlocked.Increment(ref _scanned);
                    try
                    {
                        if (!IncludeByType(src.Type, file))
                        {
                            if (opt.Verbose && !opt.SampleMode) log($"SKIP (type): {file}");
                            Interlocked.Increment(ref _skipped);
                            continue;
                        }
                        int year = DetermineYear(file, srcRoot);
                        if (year != opt.SelectedYear)
                        {
                            if (opt.Verbose && !opt.SampleMode) log($"SKIP (year {year} != {opt.SelectedYear}): {file}");
                            Interlocked.Increment(ref _skipped);
                            continue;
                        }
                        var destPath = BuildDestinationPath(dstRoot, src, year, srcRoot, file);
                        if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource)
                        {
                            if (src.Type == SourceType.DigiForce)
                            {
                                if (TryGetDigiForceStation(srcRoot, file, out var station))
                                {
                                    lock (sampledStations!)
                                    {
                                        if (sampledStations.Contains(station)) { Interlocked.Increment(ref _skipped); continue; }
                                        sampledStations.Add(station);
                                    }
                                    log($"SAMPLE (station {station}): {file}  →  {destPath}");
                                    Interlocked.Increment(ref _moved);
                                    continue;
                                }
                                else
                                {
                                    if (!sampledThisSource)
                                    {
                                        log($"SAMPLE: {file}  →  {destPath}");
                                        Interlocked.Increment(ref _moved);
                                        sampledThisSource = true;
                                    }
                                    continue;
                                }
                            }
                            else
                            {
                                if (!sampledThisSource)
                                {
                                    log($"SAMPLE: {file}  →  {destPath}");
                                    Interlocked.Increment(ref _moved);
                                    sampledThisSource = true;
                                }
                                continue;
                            }
                        }
                        if (opt.DryRun)
                        {
                            if (opt.Verbose) log($"MOVE: {file}  →  {destPath}");
                            Interlocked.Increment(ref _moved);
                        }
                        else
                        {
                            var destDir = Path.GetDirectoryName(destPath)!;
                            RobustIo(opt, () => Directory.CreateDirectory(destDir));
                            if (File.Exists(destPath))
                            {
                                long sLen = SafeLength(file);
                                long dLen = SafeLength(destPath);
                                if (sLen == dLen && sLen >= 0)
                                {
                                    log($"DUPLICATE (same size) → delete source: {file}");
                                    RobustIo(opt, () => File.Delete(file));
                                    Interlocked.Increment(ref _dupes);
                                }
                                else
                                {
                                    var unique = GetUniquePath(destPath);
                                    log($"CONFLICT → rename & move: {file}  →  {unique}");
                                    RobustIo(opt, () => File.Move(file, unique));
                                    Interlocked.Increment(ref _moved);
                                }
                            }
                            else
                            {
                                if (opt.Verbose) log($"MOVE: {file}  →  {destPath}");
                                RobustIo(opt, () => File.Move(file, destPath));
                                Interlocked.Increment(ref _moved);
                            }
                        }
                    }
                    catch (OperationCanceledException) { throw; }
                    catch (Exception ex) { log("ERROR: " + ex.Message); Interlocked.Increment(ref _errors); }
                }
            }
        }, token)).ToArray();
        var allTasks = consumers.Concat([producer]).ToArray();
        await Task.WhenAll(allTasks);
        if (opt.SampleMode && src.Type == SourceType.DigiForce && sampledStations is { Count: > 0 })
            log($"Sampled {sampledStations.Count} DigiForce station(s) under: {srcRoot} (year {opt.SelectedYear})");
        if (opt.SampleMode && !sampledThisSource && (sampledStations == null || sampledStations.Count == 0))
            log($"NO MATCH in source: {srcRoot} (year {opt.SelectedYear})");
    }
    private static readonly int[] RoboSuccessCodes = [0, 1, 2, 3, 5, 6, 7];
    private int RunRobocopy(string exe, string source, string dest, IEnumerable<string> switches, bool dryRun, bool echoStdout, out string stdOut)
    {
        var args = new List<string> { Quote(source), Quote(dest) };
        args.AddRange(switches);
        if (dryRun) args.Add("/L");
        var psi = new ProcessStartInfo
        {
            FileName = exe,
            Arguments = string.Join(" ", args),
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            CreateNoWindow = true
        };
        using var p = new Process { StartInfo = psi };
        p.Start();
        stdOut = p.StandardOutput.ReadToEnd();
        var stdErr = p.StandardError.ReadToEnd();
        p.WaitForExit();
        if (echoStdout && !string.IsNullOrWhiteSpace(stdOut)) log(stdOut.Trim());
        if (!string.IsNullOrWhiteSpace(stdErr)) log("ROBOCOPY STDERR: " + stdErr.Trim());
        return p.ExitCode;
        static string Quote(string s) => "\"" + s.TrimEnd('\\') + "\"";
    }
    private bool TryRobocopyFtpSource(string srcRoot, string dstRoot, ArchiveOptions opt)
    {
        IEnumerable<string> yearDirs;
        try
        {
            yearDirs = [.. Directory.EnumerateDirectories(srcRoot).Where(d => FolderYearOnly.IsMatch(Path.GetFileName(d)))];
        }
        catch (Exception ex) { log($"ROBO: Failed to list {srcRoot} – {ex.Message}"); return false; }
        var targetYearName = opt.SelectedYear.ToString(CultureInfo.InvariantCulture);
        var yearFolder = yearDirs.FirstOrDefault(d => string.Equals(Path.GetFileName(d), targetYearName, StringComparison.OrdinalIgnoreCase));
        if (yearFolder == null) return false;
        var destBase = Path.Combine(dstRoot, targetYearName, SourceType.FTP.ToString());
        Directory.CreateDirectory(destBase);
        var sw = new List<string> { "/E", "/MOV", "/COPY:DAT", "/DCOPY:T", "/R:1", "/W:1", "/NFL", "/NDL", "/NP", $"/MT:{Math.Max(1, opt.RobocopyThreads)}" };
        log($"ROBO FTP: {yearFolder}  =>  {destBase}");
        var rc = RunRobocopy(opt.RobocopyExe, yearFolder, destBase, sw, opt.DryRun, opt.RobocopyLogStdout, out var output);
        if (RoboSuccessCodes.Contains(rc)) { log($"ROBO FTP OK (code {rc}): {yearFolder} → {destBase}"); return true; }
        else { log($"ROBO FTP FAILED (code {rc}): {yearFolder} → {destBase}"); return false; }
    }
    private bool TryRobocopyKistlerSource(string srcRoot, string dstRoot, ArchiveOptions opt)
    {
        string[] dateDirs;
        try
        {
            dateDirs = [.. Directory.EnumerateDirectories(srcRoot)
                .Where(d =>
                {
                    var name = Path.GetFileName(d);
                    return FolderDateDash.IsMatch(name) || FolderDateUnders.IsMatch(name) || FolderDateCompact.IsMatch(name);
                })];
        }
        catch (Exception ex) { log($"ROBO: Failed to list {srcRoot} – {ex.Message}"); return false; }
        var any = false;
        foreach (var d in dateDirs)
        {
            var fn = Path.GetFileName(d);
            if (!TryGetFolderYear(fn, out var y) || y != opt.SelectedYear) continue;
            var dest = Path.Combine(dstRoot, y.ToString(CultureInfo.InvariantCulture), SourceType.Kistler.ToString(), fn);
            Directory.CreateDirectory(dest);
            var sw = new List<string> { "/E", "/MOV", "/COPY:DAT", "/DCOPY:T", "/R:1", "/W:1", "/NFL", "/NDL", "/NP", $"/MT:{Math.Max(1, opt.RobocopyThreads)}" };
            log($"ROBO KISTLER: {d}  =>  {dest}");
            var rc = RunRobocopy(opt.RobocopyExe, d, dest, sw, opt.DryRun, opt.RobocopyLogStdout, out var output);
            if (RoboSuccessCodes.Contains(rc)) { log($"ROBO KISTLER OK (code {rc}): {d} → {dest}"); any = true; }
            else { log($"ROBO KISTLER FAILED (code {rc}): {d} → {dest}  (falling back to .NET for this folder)"); }
        }

        return any;
    }
    private static IEnumerable<string> EnumerateAllFiles(string root, int selectedYear, bool verbose, Action<string> log, CancellationToken token)
    {
        var stack = new Stack<string>();
        stack.Push(root);
        while (stack.Count > 0)
        {
            token.ThrowIfCancellationRequested();
            string dir = stack.Pop();
            var folderName = Path.GetFileName(dir);
            if (TryGetFolderYear(folderName, out int folderYear))
            {
                if (folderYear != selectedYear)
                {
                    if (verbose) log($"SKIP folder (year {folderYear} != {selectedYear}): {dir}");
                    continue;
                }
            }
            IEnumerable<string> files;
            try { files = Directory.EnumerateFiles(dir); }
            catch (UnauthorizedAccessException) { files = []; }
            catch (PathTooLongException) { files = []; }
            foreach (var f in files)
            {
                token.ThrowIfCancellationRequested();
                yield return f;
            }
            IEnumerable<string> subDirs;
            try { subDirs = Directory.EnumerateDirectories(dir); }
            catch (UnauthorizedAccessException) { subDirs = []; }
            catch (PathTooLongException) { subDirs = []; }
            foreach (var sd in subDirs) stack.Push(sd);
        }
    }
    private static readonly Regex FolderDateDash = new(@"^(?<y>\d{4})-(?<m>\d{2})-(?<d>\d{2})$", RegexOptions.Compiled);
    private static readonly Regex FolderDateUnders = new(@"^(?<y>\d{4})_(?<m>\d{2})_(?<d>\d{2})$", RegexOptions.Compiled);
    private static readonly Regex FolderDateCompact = new(@"^(?<y>(?:19|20)\d{2})(?<m>\d{2})(?<d>\d{2})$", RegexOptions.Compiled);
    private static readonly Regex FolderYearOnly = new(@"^(?<y>\d{4})$", RegexOptions.Compiled);
    private static bool TryGetFolderYear(string folderName, out int year)
    {
        year = default;
        if (string.IsNullOrEmpty(folderName)) return false;
        var m = FolderDateDash.Match(folderName);
        if (m.Success && int.TryParse(m.Groups["y"].Value, out year)) return true;
        m = FolderDateUnders.Match(folderName);
        if (m.Success && int.TryParse(m.Groups["y"].Value, out year)) return true;
        m = FolderDateCompact.Match(folderName);
        if (m.Success && int.TryParse(m.Groups["y"].Value, out year)) return true;
        m = FolderYearOnly.Match(folderName);
        if (m.Success && int.TryParse(m.Groups["y"].Value, out year)) return true;
        return false;
    }
    private static bool IncludeByType(SourceType t, string file)
    {
        var ext = Path.GetExtension(file);
        return t switch
        {
            SourceType.Kistler => ext.Equals(".csv", StringComparison.OrdinalIgnoreCase),
            SourceType.FTP => FtpExts.Contains(ext),
            SourceType.DigiForce => ext.StartsWith(".meas", true, CultureInfo.InvariantCulture),
            _ => true
        };
    }
    private static int DetermineYear(string filePath, string srcRoot)
    {
        var rel = Path.GetRelativePath(srcRoot, filePath);
        foreach (var seg in SplitPath(rel))
        {
            if (YearSegment.IsMatch(seg) && int.TryParse(seg, out var y) && InReasonableYear(y)) return y;
        }
        var name = Path.GetFileName(filePath);
        var m1 = DateY_M_D_Underscore.Match(name);
        if (m1.Success && int.TryParse(m1.Groups[1].Value, out int y2) && InReasonableYear(y2)) return y2;
        var m2 = DateY_M_D_Dash.Match(name);
        if (m2.Success && int.TryParse(m2.Groups[1].Value, out y2) && InReasonableYear(y2)) return y2;
        var m3 = DateYYYYMMDD.Match(name);
        if (m3.Success && int.TryParse(m3.Groups[2].Value, out y2) && InReasonableYear(y2)) return y2;
        return File.GetLastWriteTime(filePath).Year;
    }
    private static bool InReasonableYear(int y) => y >= 1990 && y <= 9999;
    private static string[] SplitPath(string p) => p.Split([Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar], StringSplitOptions.RemoveEmptyEntries);
    private static int IndexOfYearSegment(string[] parts)
    {
        for (int i = 0; i < parts.Length; i++) if (YearSegment.IsMatch(parts[i])) return i;
        return -1;
    }
    private static string NormalizeRoot(string p)
    {
        if (string.IsNullOrWhiteSpace(p)) return p;
        return p.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
    }
    private static long SafeLength(string p) { try { return new FileInfo(p).Length; } catch { return -1; } }
    private static string GetUniquePath(string path)
    {
        var dir = Path.GetDirectoryName(path) ?? "";
        var name = Path.GetFileNameWithoutExtension(path);
        var ext = Path.GetExtension(path);
        int i = 1; string candidate;
        do { candidate = Path.Combine(dir, $"{name} ({i++}){ext}"); } while (File.Exists(candidate));
        return candidate;
    }
    private static string[] TransformDirPartsForDigiForce(string[] dirParts)
    {
        if (dirParts == null || dirParts.Length == 0) return [];
        var first = dirParts[0];
        if (Regex.IsMatch(first, @"^\d{4}$"))
        {
            var mm = first[..2];
            var dd = first.Substring(2, 2);
            if (int.TryParse(mm, out var m) && int.TryParse(dd, out var d) && m >= 1 && m <= 12 && d >= 1 && d <= 31)
            {
                var list = new List<string>(dirParts.Length + 1) { mm, dd };
                list.AddRange(dirParts.Skip(1));
                return [.. list];
            }
        }
        return dirParts;
    }
    private static string BuildDestinationPath(string dstRoot, SourceEntry src, int year, string srcRoot, string file)
    {
        var rel = Path.GetRelativePath(srcRoot, file);
        var parts = SplitPath(rel);
        int idxYear = IndexOfYearSegment(parts);
        int startIdx = idxYear >= 0 ? idxYear + 1 : 0;
        var dirParts = parts.Length > 1 ? parts[startIdx..(parts.Length - 1)] : [];
        if (src.Type == SourceType.DigiForce) dirParts = TransformDirPartsForDigiForce(dirParts);
        var prefix = new[] { dstRoot, year.ToString(CultureInfo.InvariantCulture), src.Type.ToString() };
        var allParts = prefix.Concat(dirParts).ToArray();
        var destDir = Path.Combine(allParts);
        var destPath = Path.Combine(destDir, Path.GetFileName(file));
        return destPath;
    }
    private static string ResolveExistingPath(string p)
    {
        try
        {
            if (Directory.Exists(p)) return p;
            const string ipRoot = "\\\\10.170.50.130\\\\";
            const string dnsRoot = "\\\\pc-nas3\\";
            if (p.StartsWith(ipRoot, StringComparison.OrdinalIgnoreCase))
            {
                var alt = string.Concat(dnsRoot, p.AsSpan(ipRoot.Length));
                if (Directory.Exists(alt)) return alt;
            }
            else if (p.StartsWith(dnsRoot, StringComparison.OrdinalIgnoreCase))
            {
                var alt = string.Concat(ipRoot, p.AsSpan(dnsRoot.Length));
                if (Directory.Exists(alt)) return alt;
            }
        }
        catch { }
        return p;
    }
    private void RobustIo(ArchiveOptions opt, Action action)
    {
        int attempt = 0;
        for (;;)
        {
            try { action(); return; }
            catch (IOException) when (++attempt <= opt.MaxRetries) { Thread.Sleep(opt.RetryBackoffMs * attempt); Interlocked.Increment(ref _errors); }
            catch (UnauthorizedAccessException) when (attempt++ <= opt.MaxRetries) { Thread.Sleep(opt.RetryBackoffMs * attempt); Interlocked.Increment(ref _errors); }
        }
    }
}