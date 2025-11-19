using FTPArchiver;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Channels;
using System.Diagnostics;

namespace FTPArchiver
{
    public enum SampleScope { PerSource, PerLeafFolder }   // keep enum for future flexibility


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

        // NEW: Concurrency (safe defaults)
        public int MaxConcurrentSources { get; set; } = 2;          // how many source roots at once
        public int MaxConcurrentFilesPerSource { get; set; } = 4;   // per-source file workers

        public int MaxRetries { get; set; } = 3;
        public int RetryBackoffMs { get; set; } = 500;

        // Use Robocopy for bulk moves (recommended for speed)
        public bool UseRobocopyForFtp { get; set; } = true;
        public bool UseRobocopyForKistler { get; set; } = true;

        // Robocopy tuning
        public int RobocopyThreads { get; set; } = 16;      // /MT:n (try 8–32 depending on NAS)
        public string RobocopyExe { get; set; } = "robocopy";
        public bool RobocopyLogStdout { get; set; } = false; // set true to echo robocopy output in your log

    }


    // NEW: scope for sampling behavior
    //public enum SampleScope { PerSource, PerLeafFolder }

    public readonly struct ProgressSnapshot(long scanned, long moved, long dupes, long skipped, long errors)
    {
        public long FilesScanned { get; } = scanned; public long Moved { get; } = moved; public long Duplicates { get; } = dupes; public long Skipped { get; } = skipped; public long Errors { get; } = errors;
    }

    public sealed class Archiver
    {
        private readonly Action<string> _log;
        private readonly Action<string> _status;
        private readonly Action<ProgressSnapshot> _progress;

        private long _scanned, _moved, _dupes, _skipped, _errors;

        private static readonly Regex YearSegment = new(@"^\d{4}$", RegexOptions.Compiled);
        private static readonly Regex DateY_M_D_Underscore = new(@"(\d{4})_(\d{2})_(\d{2})", RegexOptions.Compiled);
        private static readonly Regex DateY_M_D_Dash = new(@"(\d{4})-(\d{2})-(\d{2})", RegexOptions.Compiled);
        private static readonly Regex DateYYYYMMDD = new(@"(^|[^0-9])(20\d{2}|19\d{2})(\d{2})(\d{2})([^0-9]|$)", RegexOptions.Compiled);

        // Add near top of Archiver class, replacing ImageExts
        private static readonly HashSet<string> FtpExts = new(StringComparer.OrdinalIgnoreCase)
        {
            // Raster
            ".jpg",".jpeg",".png",".bmp",".tif",".tiff",".gif",".webp",".jfif",
            // Vector / sidecar
            ".svg"
        };

        private static readonly Regex DigiForceStationRegex = new(@"^SA\d+_OP\d+", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        private static bool TryGetDigiForceStation(string srcRoot, string file, out string station)
        {
            station = string.Empty;
            var rel = Path.GetRelativePath(srcRoot, file);
            var parts = SplitPath(rel);

            // Find the YEAR segment
            int idxYear = IndexOfYearSegment(parts);
            if (idxYear < 0) return false;

            // Candidate index immediately after YEAR
            int i = idxYear + 1;

            // Skip MMDD if present (e.g., "0822")
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

        public Archiver(Action<string> log, Action<string> status, Action<ProgressSnapshot> progress)
        {
            _log = log; _status = status; _progress = progress;
        }

        public void Run(ArchiveOptions opt, CancellationToken token)
        {
            string dstRoot = NormalizeRoot(opt.DestinationRoot);
            _log($"Destination: {dstRoot}");
            _log($"Year: {opt.SelectedYear}");
            _log(opt.DryRun ? "Mode: DRY RUN" : "Mode: LIVE");
            if (opt.SampleMode)
            {
                _log($"Sample mode: {(opt.SampleScope == SampleScope.PerSource ? "1 file per top-level source" : "1 per folder")} (dry run enforced).");
                opt.DryRun = true; // enforce dry-run during sampling
            }

            var lastProgress = DateTime.UtcNow;

            // Process sources concurrently
            using var sourceSemaphore = new SemaphoreSlim(Math.Max(1, opt.MaxConcurrentSources));
            var sourceTasks = new List<Task>();

            foreach (var src in opt.Sources)
            {
                token.ThrowIfCancellationRequested();
                awaitAcquire(sourceSemaphore, token);

                var srcCopy = src; // capture
                var t = Task.Run(async () =>
                {
                    try
                    {
                        await ProcessSingleSourceAsync(srcCopy, dstRoot, opt, token);
                    }
                    finally
                    {
                        sourceSemaphore.Release();
                    }
                }, token);

                sourceTasks.Add(t);
            }

            Task.WaitAll(sourceTasks.ToArray(), token);

            _progress(new ProgressSnapshot(_scanned, _moved, _dupes, _skipped, _errors));
            _status("Completed.");

            void awaitAcquire(SemaphoreSlim sem, CancellationToken ct)
            {
                // synchronous wait to keep signature; safe here in host thread
                sem.Wait(ct);
            }

            void maybeProgress()
            {
                var now = DateTime.UtcNow;
                if ((now - lastProgress).TotalMilliseconds > 500)
                {
                    lastProgress = now;
                    _progress(new ProgressSnapshot(_scanned, _moved, _dupes, _skipped, _errors));
                }
            }
        }
        private async Task ProcessSingleSourceAsync(SourceEntry src, string dstRoot, ArchiveOptions opt, CancellationToken token)
        {
            if (string.IsNullOrWhiteSpace(src.Path))
            {
                _log("WARN: Source path empty.");
                Interlocked.Increment(ref _errors);
                return;
            }

            var resolved = ResolveExistingPath(src.Path);
            if (!Directory.Exists(resolved))
            {
                _log($"WARN: Source missing or not found → {src.Path}");
                Interlocked.Increment(ref _errors);
                return;
            }

            var srcRoot = NormalizeRoot(resolved);
            if (!srcRoot.Equals(src.Path, StringComparison.OrdinalIgnoreCase))
                _log($"Resolved source: {src.Path} → {srcRoot}");

            _log($"==> Processing [{src.Type}] {srcRoot}");

            // ---------------- ROB0COPY SHORT-CIRCUIT (place right after the Processing log) ----------------
            if (!opt.SampleMode) // we can also allow DryRun with /L; defaulting to per-file for dry-run clarity
            {
                bool handledByRobo = false;

                if (src.Type == SourceType.FTP && opt.UseRobocopyForFtp)
                {
                    handledByRobo = TryRobocopyFtpSource(srcRoot, dstRoot, opt);
                }
                else if (src.Type == SourceType.Kistler && opt.UseRobocopyForKistler)
                {
                    handledByRobo = TryRobocopyKistlerSource(srcRoot, dstRoot, opt);
                }

                if (handledByRobo)
                {
                    // FAST PATH: Robocopy completed (or largely completed) this source/year.
                    // For absolute speed, exit now and move on to the next source:
                    return;

                    // If you'd prefer to "top-up" any files Robocopy skipped (locked, etc.),
                    // comment out the 'return;' above and allow the per-file pipeline below to run.
                }
            }
            // -----------------------------------------------------------------------------------------------

            // Sample mode trackers
            bool sampledThisSource = false;
            HashSet<string>? sampledStations = null;
            if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource && src.Type == SourceType.DigiForce)
                sampledStations = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            // Suppress folder-skip logs in sample mode
            bool folderSkipLogs = opt.Verbose && !opt.SampleMode;

            // Channel pipeline
            var ch = CreateFileChannel();
            var writer = ch.Writer;
            var reader = ch.Reader;

            // Producer
            var producer = Task.Run(async () =>
            {
                try
                {
                    foreach (var file in EnumerateAllFiles(srcRoot, opt.SelectedYear, folderSkipLogs, _log, token))
                    {
                        token.ThrowIfCancellationRequested();

                        // Sample-mode short-circuit for non-DigiForce one-per-source
                        if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource && src.Type != SourceType.DigiForce && sampledThisSource)
                            break;

                        // Enqueue
                        await writer.WriteAsync(file, token);
                    }
                }
                finally
                {
                    writer.TryComplete();
                }
            }, token);

            // Consumers
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
                            // Type filter
                            if (!IncludeByType(src.Type, file))
                            {
                                if (opt.Verbose && !opt.SampleMode) _log($"SKIP (type): {file}");
                                Interlocked.Increment(ref _skipped);
                                continue;
                            }

                            // Year filter
                            int year = DetermineYear(file, srcRoot);
                            if (year != opt.SelectedYear)
                            {
                                if (opt.Verbose && !opt.SampleMode) _log($"SKIP (year {year} != {opt.SelectedYear}): {file}");
                                Interlocked.Increment(ref _skipped);
                                continue;
                            }

                            // Destination path
                            var destPath = BuildDestinationPath(dstRoot, src, year, srcRoot, file);

                            // SAMPLE MODE
                            if (opt.SampleMode && opt.SampleScope == SampleScope.PerSource)
                            {
                                if (src.Type == SourceType.DigiForce)
                                {
                                    if (TryGetDigiForceStation(srcRoot, file, out var station))
                                    {
                                        lock (sampledStations!)
                                        {
                                            if (sampledStations.Contains(station))
                                            {
                                                Interlocked.Increment(ref _skipped);
                                                continue;
                                            }
                                            sampledStations.Add(station);
                                        }
                                        _log($"SAMPLE (station {station}): {file}  →  {destPath}");
                                        Interlocked.Increment(ref _moved);
                                        continue;
                                    }
                                    else
                                    {
                                        // no station detected; one generic sample
                                        if (!sampledThisSource)
                                        {
                                            _log($"SAMPLE: {file}  →  {destPath}");
                                            Interlocked.Increment(ref _moved);
                                            sampledThisSource = true;
                                        }
                                        continue;
                                    }
                                }
                                else
                                {
                                    // Kistler/FTP: one per source
                                    if (!sampledThisSource)
                                    {
                                        _log($"SAMPLE: {file}  →  {destPath}");
                                        Interlocked.Increment(ref _moved);
                                        sampledThisSource = true;
                                    }
                                    continue;
                                }
                            }

                            // LIVE / DRY-RUN
                            if (opt.DryRun)
                            {
                                if (opt.Verbose) _log($"MOVE: {file}  →  {destPath}");
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
                                        _log($"DUPLICATE (same size) → delete source: {file}");
                                        RobustIo(opt, () => File.Delete(file));
                                        Interlocked.Increment(ref _dupes);
                                    }
                                    else
                                    {
                                        var unique = GetUniquePath(destPath);
                                        _log($"CONFLICT → rename & move: {file}  →  {unique}");
                                        RobustIo(opt, () => File.Move(file, unique));
                                        Interlocked.Increment(ref _moved);
                                    }
                                }
                                else
                                {
                                    if (opt.Verbose) _log($"MOVE: {file}  →  {destPath}");
                                    RobustIo(opt, () => File.Move(file, destPath));
                                    Interlocked.Increment(ref _moved);
                                }
                            }
                        }
                        catch (OperationCanceledException) { throw; }
                        catch (Exception ex)
                        {
                            _log("ERROR: " + ex.Message);
                            Interlocked.Increment(ref _errors);
                        }
                    }
                }
            }, token)).ToArray();

            await Task.WhenAll(consumers.Concat(new[] { producer }));

            // Sample summaries
            if (opt.SampleMode && src.Type == SourceType.DigiForce && sampledStations is { Count: > 0 })
                _log($"Sampled {sampledStations.Count} DigiForce station(s) under: {srcRoot} (year {opt.SelectedYear})");

            if (opt.SampleMode && !sampledThisSource && (sampledStations == null || sampledStations.Count == 0))
                _log($"NO MATCH in source: {srcRoot} (year {opt.SelectedYear})");
        }

        // ---------------- Robocopy plumbing ----------------

        private static readonly int[] RoboSuccessCodes = { 0, 1, 2, 3, 5, 6, 7 };
        // 0  = No files copied
        // 1  = Copied some files
        // 2  = Extra files/dirs
        // 3  = Copied + extra
        // 5/6/7 include mismatched extras; all considered non-failure in practice

        private int RunRobocopy(
            string exe,
            string source,
            string dest,
            IEnumerable<string> switches,
            bool dryRun,
            bool echoStdout,
            out string stdOut)
        {
            // Build args
            var args = new List<string>
    {
        Quote(source),
        Quote(dest)
    };
            args.AddRange(switches);

            // /L for list-only (dry run) – leave it out for live
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

            if (echoStdout && !string.IsNullOrWhiteSpace(stdOut))
                _log(stdOut.Trim());

            if (!string.IsNullOrWhiteSpace(stdErr))
                _log("ROBOCOPY STDERR: " + stdErr.Trim());

            return p.ExitCode;

            static string Quote(string s) => "\"" + s.TrimEnd('\\') + "\"";
        }

        // FTP: Robocopy whole \YYYY subtree → <dst>\YYYY\FTP\
        private bool TryRobocopyFtpSource(string srcRoot, string dstRoot, ArchiveOptions opt)
        {
            // Find subfolders exactly named YYYY under this FTP source root
            IEnumerable<string> yearDirs;
            try
            {
                yearDirs = Directory.EnumerateDirectories(srcRoot)
                    .Where(d => FolderYearOnly.IsMatch(Path.GetFileName(d)));
            }
            catch (Exception ex)
            {
                _log($"ROBO: Failed to list {srcRoot} – {ex.Message}");
                return false;
            }

            var targetYearName = opt.SelectedYear.ToString(CultureInfo.InvariantCulture);
            var yearFolder = yearDirs.FirstOrDefault(d => string.Equals(Path.GetFileName(d), targetYearName, StringComparison.OrdinalIgnoreCase));
            if (yearFolder == null)
            {
                // Nothing to do for this source/year via robocopy
                return false;
            }

            // Destination base: <dst>\YYYY\FTP\
            var destBase = Path.Combine(dstRoot, targetYearName, SourceType.FTP.ToString());
            Directory.CreateDirectory(destBase);

            // Standard robust switches
            var sw = new List<string>
    {
        "/E",            // include subdirs
        "/MOV",          // move files (not dirs)
        "/COPY:DAT",     // data+attributes+timestamps
        "/DCOPY:T",      // preserve dir timestamps
        "/R:1", "/W:1",  // quick retries
        "/NFL", "/NDL", "/NP", // quiet
        $"/MT:{Math.Max(1, opt.RobocopyThreads)}"
    };

            _log($"ROBO FTP: {yearFolder}  =>  {destBase}");
            var rc = RunRobocopy(opt.RobocopyExe, yearFolder, destBase, sw, opt.DryRun, opt.RobocopyLogStdout, out var output);

            if (RoboSuccessCodes.Contains(rc))
            {
                _log($"ROBO FTP OK (code {rc}): {yearFolder} → {destBase}");
                return true; // handled this source via robocopy
            }
            else
            {
                _log($"ROBO FTP FAILED (code {rc}): {yearFolder} → {destBase}");
                return false; // fall back to .NET per-file
            }
        }

        // KISTLER: Robocopy each date folder (YYYY-MM-DD) that matches selected year
        private bool TryRobocopyKistlerSource(string srcRoot, string dstRoot, ArchiveOptions opt)
        {
            IEnumerable<string> dateDirs;
            try
            {
                dateDirs = Directory.EnumerateDirectories(srcRoot)
                    .Where(d =>
                    {
                        var name = Path.GetFileName(d);
                        // date folder must parse to a year
                        return FolderDateDash.IsMatch(name) || FolderDateUnders.IsMatch(name) || FolderDateCompact.IsMatch(name);
                    })
                    .ToArray();
            }
            catch (Exception ex)
            {
                _log($"ROBO: Failed to list {srcRoot} – {ex.Message}");
                return false;
            }

            var any = false;
            foreach (var d in dateDirs)
            {
                var fn = Path.GetFileName(d);
                if (!TryGetFolderYear(fn, out var y) || y != opt.SelectedYear) continue;

                // Destination: <dst>\YYYY\Kistler\{fn}\...
                var dest = Path.Combine(dstRoot, y.ToString(CultureInfo.InvariantCulture), SourceType.Kistler.ToString(), fn);
                Directory.CreateDirectory(dest);

                var sw = new List<string>
        {
            "/E", "/MOV", "/COPY:DAT", "/DCOPY:T", "/R:1", "/W:1", "/NFL", "/NDL", "/NP",
            $"/MT:{Math.Max(1, opt.RobocopyThreads)}"
        };

                _log($"ROBO KISTLER: {d}  =>  {dest}");
                var rc = RunRobocopy(opt.RobocopyExe, d, dest, sw, opt.DryRun, opt.RobocopyLogStdout, out var output);

                if (RoboSuccessCodes.Contains(rc))
                {
                    _log($"ROBO KISTLER OK (code {rc}): {d} → {dest}");
                    any = true;
                }
                else
                {
                    _log($"ROBO KISTLER FAILED (code {rc}): {d} → {dest}  (falling back to .NET for this folder)");
                    // We don't return; the .NET enumerator will still pick up files left behind
                }
            }

            return any; // if we moved at least one date folder via robocopy, consider handled
        }
        // -----------------------------------------------------------------------------
        // Optimized enumerator: skips entire folders when folder name encodes a year/date
        // -----------------------------------------------------------------------------
        private static IEnumerable<string> EnumerateAllFiles(
            string root,
            int selectedYear,
            bool verbose,
            Action<string> log,
            CancellationToken token)
        {
            var stack = new Stack<string>();
            stack.Push(root);

            while (stack.Count > 0)
            {
                token.ThrowIfCancellationRequested();
                string dir = stack.Pop();

                // --- Folder-level year detection & skip ---
                // If this directory's leaf name looks like a date (YYYY-MM-DD / YYYY_MM_DD / YYYYMMDD)
                // or a plain year (YYYY), and it DOES NOT match the selected year, skip the entire subtree.
                var folderName = Path.GetFileName(dir);
                if (TryGetFolderYear(folderName, out int folderYear))
                {
                    if (folderYear != selectedYear)
                    {
                        if (verbose) log($"SKIP folder (year {folderYear} != {selectedYear}): {dir}");
                        // Do not push its subdirectories; continue with next item on the stack.
                        continue;
                    }
                }

                // --- Enumerate files (materialize outside try/catch to keep yields out of try) ---
                IEnumerable<string> files;
                try
                {
                    files = Directory.EnumerateFiles(dir);
                }
                catch (UnauthorizedAccessException)
                {
                    files = Array.Empty<string>();
                }
                catch (PathTooLongException)
                {
                    files = Array.Empty<string>();
                }

                foreach (var f in files)
                {
                    token.ThrowIfCancellationRequested();
                    yield return f; // yield occurs outside try/catch (required by C#)
                }

                // --- Enumerate subdirectories, push onto stack (depth-first) ---
                IEnumerable<string> subDirs;
                try
                {
                    subDirs = Directory.EnumerateDirectories(dir);
                }
                catch (UnauthorizedAccessException)
                {
                    subDirs = Array.Empty<string>();
                }
                catch (PathTooLongException)
                {
                    subDirs = Array.Empty<string>();
                }

                foreach (var sd in subDirs)
                    stack.Push(sd);
            }
        }

        // -----------------------------------------------------------------------------
        // Backward-compat shim (if some callers still use the old signature). It performs
        // no folder-level skipping because we don't know the selected year. Prefer using
        // the new overload above in your main loop.
        // -----------------------------------------------------------------------------
        private static IEnumerable<string> EnumerateAllFiles(string root, CancellationToken token)
        {
            return EnumerateAllFiles_NoFolderSkip(root, token);
        }

        private static IEnumerable<string> EnumerateAllFiles_NoFolderSkip(string root, CancellationToken token)
        {
            var stack = new Stack<string>();
            stack.Push(root);

            while (stack.Count > 0)
            {
                token.ThrowIfCancellationRequested();
                string dir = stack.Pop();

                IEnumerable<string> files;
                try
                {
                    files = Directory.EnumerateFiles(dir);
                }
                catch (UnauthorizedAccessException)
                {
                    files = Array.Empty<string>();
                }
                catch (PathTooLongException)
                {
                    files = Array.Empty<string>();
                }

                foreach (var f in files)
                {
                    token.ThrowIfCancellationRequested();
                    yield return f;
                }

                IEnumerable<string> subDirs;
                try
                {
                    subDirs = Directory.EnumerateDirectories(dir);
                }
                catch (UnauthorizedAccessException)
                {
                    subDirs = Array.Empty<string>();
                }
                catch (PathTooLongException)
                {
                    subDirs = Array.Empty<string>();
                }

                foreach (var sd in subDirs)
                    stack.Push(sd);
            }
        }

        // -----------------------------------------------------------------------------
        // Helpers: detect year from folder names like "2023-08-28", "2023_08_28", "20230828", or "2023"
        // -----------------------------------------------------------------------------
        private static readonly Regex FolderDateDash = new(@"^(?<y>\d{4})-(?<m>\d{2})-(?<d>\d{2})$", RegexOptions.Compiled);
        private static readonly Regex FolderDateUnders = new(@"^(?<y>\d{4})_(?<m>\d{2})_(?<d>\d{2})$", RegexOptions.Compiled);
        private static readonly Regex FolderDateCompact = new(@"^(?<y>(?:19|20)\d{2})(?<m>\d{2})(?<d>\d{2})$", RegexOptions.Compiled);
        private static readonly Regex FolderYearOnly = new(@"^(?<y>\d{4})$", RegexOptions.Compiled);

        private static bool TryGetFolderYear(string folderName, out int year)
        {
            year = default;

            if (string.IsNullOrEmpty(folderName))
                return false;

            // YYYY-MM-DD
            var m = FolderDateDash.Match(folderName);
            if (m.Success && int.TryParse(m.Groups["y"].Value, out year))
                return true;

            // YYYY_MM_DD
            m = FolderDateUnders.Match(folderName);
            if (m.Success && int.TryParse(m.Groups["y"].Value, out year))
                return true;

            // YYYYMMDD
            m = FolderDateCompact.Match(folderName);
            if (m.Success && int.TryParse(m.Groups["y"].Value, out year))
                return true;

            // YYYY
            m = FolderYearOnly.Match(folderName);
            if (m.Success && int.TryParse(m.Groups["y"].Value, out year))
                return true;

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
            // 1) Year directory segment under the source root
            var rel = Path.GetRelativePath(srcRoot, filePath);
            foreach (var seg in SplitPath(rel))
            {
                if (YearSegment.IsMatch(seg) && int.TryParse(seg, out var y) && InReasonableYear(y))
                    return y;
            }

            // 2) Filename date patterns
            var name = Path.GetFileName(filePath);
            int y2;
            var m1 = DateY_M_D_Underscore.Match(name);
            if (m1.Success && int.TryParse(m1.Groups[1].Value, out y2) && InReasonableYear(y2)) return y2;

            var m2 = DateY_M_D_Dash.Match(name);
            if (m2.Success && int.TryParse(m2.Groups[1].Value, out y2) && InReasonableYear(y2)) return y2;

            var m3 = DateYYYYMMDD.Match(name);
            if (m3.Success && int.TryParse(m3.Groups[2].Value, out y2) && InReasonableYear(y2)) return y2;

            // 3) Fallback to last write time
            return File.GetLastWriteTime(filePath).Year;
        }

        private static bool InReasonableYear(int y) => y >= 1990 && y <= 9999;

        private static string[] SplitPath(string p) =>
            p.Split(new[] { Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar }, StringSplitOptions.RemoveEmptyEntries);

        private static int IndexOfYearSegment(string[] parts)
        {
            for (int i = 0; i < parts.Length; i++)
                if (YearSegment.IsMatch(parts[i])) return i;
            return -1;
        }

        private static string NormalizeRoot(string p)
        {
            if (string.IsNullOrWhiteSpace(p)) return p;
            return p.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar);
        }

        private static long SafeLength(string p)
        {
            try { return new FileInfo(p).Length; } catch { return -1; }
        }

        private static string GetUniquePath(string path)
        {
            var dir = Path.GetDirectoryName(path) ?? "";
            var name = Path.GetFileNameWithoutExtension(path);
            var ext = Path.GetExtension(path);
            int i = 1;
            string candidate;
            do { candidate = Path.Combine(dir, $"{name} ({i++}){ext}"); }
            while (File.Exists(candidate));
            return candidate;
        }
        // Splits first directory after the year for DigiForce if it's an MMDD like "0822" -> "08","22"
        private static string[] TransformDirPartsForDigiForce(string[] dirParts)
        {
            if (dirParts == null || dirParts.Length == 0) return dirParts;

            var first = dirParts[0];
            // Match exactly 4 digits (e.g., "0822")
            if (Regex.IsMatch(first, @"^\d{4}$"))
            {
                var mm = first.Substring(0, 2);
                var dd = first.Substring(2, 2);

                // Basic plausibility check (01..12 for month, 01..31 for day)
                if (int.TryParse(mm, out var m) && int.TryParse(dd, out var d) &&
                    m >= 1 && m <= 12 && d >= 1 && d <= 31)
                {
                    var list = new List<string>(dirParts.Length + 1) { mm, dd };
                    list.AddRange(dirParts.Skip(1));
                    return list.ToArray();
                }
            }

            return dirParts;
        }
        // Inside Archiver class
        private string BuildDestinationPath(string dstRoot, SourceEntry src, int year, string srcRoot, string file)
        {
            var rel = Path.GetRelativePath(srcRoot, file);
            var parts = SplitPath(rel);
            int idxYear = IndexOfYearSegment(parts);
            int startIdx = idxYear >= 0 ? idxYear + 1 : 0;

            var dirParts = parts.Length > 1 ? parts[startIdx..(parts.Length - 1)] : Array.Empty<string>();
            if (src.Type == SourceType.DigiForce)
                dirParts = TransformDirPartsForDigiForce(dirParts);

            var destDir = Path.Combine(new[]
            {
                dstRoot,
                year.ToString(CultureInfo.InvariantCulture),
                src.Type.ToString()
            }.Concat(dirParts).ToArray());

            var destPath = Path.Combine(destDir, Path.GetFileName(file));
            return destPath;
        }
        private static string ResolveExistingPath(string p)
        {
            try
            {
                if (Directory.Exists(p)) return p;

                const string ipRoot = @"\\10.170.50.130\";
                const string dnsRoot = @"\\pc-nas3\";

                if (p.StartsWith(ipRoot, StringComparison.OrdinalIgnoreCase))
                {
                    var alt = dnsRoot + p.Substring(ipRoot.Length);
                    if (Directory.Exists(alt)) return alt;
                }
                else if (p.StartsWith(dnsRoot, StringComparison.OrdinalIgnoreCase))
                {
                    var alt = ipRoot + p.Substring(dnsRoot.Length);
                    if (Directory.Exists(alt)) return alt;
                }
            }
            catch
            {
                // ignore and fall through
            }
            return p;
        }

        private void RobustIo(ArchiveOptions opt, Action action)
        {
            int attempt = 0;
            for (; ; )
            {
                try { action(); return; }
                catch (IOException) when (++attempt <= opt.MaxRetries) { Thread.Sleep(opt.RetryBackoffMs * attempt); Interlocked.Increment(ref _errors); }
                catch (UnauthorizedAccessException) when (attempt++ <= opt.MaxRetries) { Thread.Sleep(opt.RetryBackoffMs * attempt); Interlocked.Increment(ref _errors); }
            }
        }
    }
}