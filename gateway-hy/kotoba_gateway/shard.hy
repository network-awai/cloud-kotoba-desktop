;; Shard host: one heap for every profile's cron schedule.
;;
;; Upstream's multiplexed gateway visits EVERY served profile every 60 s:
;; liveness probe, cron lock, jobs.json read, sweeps and a heartbeat write,
;; whether or not anything is due (measured 2026-10-09: 1,012 profiles, ~205%
;; CPU and 722 threads with zero agents running). The shard host keeps one
;; min-heap keyed by each profile's earliest `next_run_at` and calls upstream's
;; own `tick()`, in that profile's scope, only when the profile is due. Job
;; execution, delivery, misfire grace and fire claims all stay upstream's.
;;
;; Modes:
;;   observe  index and schedule, record what WOULD fire, execute nothing.
;;            Safe next to a live upstream multiplexer (it keeps ticking).
;;   run      tick due profiles. Refuses to start while an upstream
;;            multiplexer is live, so no job can run twice.

(import heapq json os sqlite3 sys threading time
        pathlib [Path]
        collections [deque]
        concurrent.futures [ThreadPoolExecutor]
        datetime [datetime timezone timedelta])

(setv INACTIVE-STATES #{"paused" "completed" "disabled"}
      DEFAULT-RESCAN-SECONDS 60
      DEFAULT-HOUSEKEEPING-SECONDS (* 6 3600)
      ;; The default home's ticker heartbeat is the host-level liveness signal
      ;; (hermes-cron-guard falls back to ticking itself after 20 min stale).
      HOST-HEARTBEAT-SECONDS 60
      ;; ... written from its own thread, so a slow pass (a rescan took 12 min
      ;; under swap thrash on 2026-10-09) doesn't silence it. A pass running
      ;; longer than this counts as wedged and the beats stop, so the guard
      ;; still takes over from a truly stuck loop.
      HOST-STALL-SECONDS 1800
      COST-EMA-ALPHA 0.3
      HISTORY-DAYS 7)

;; ─── jobs.json ───────────────────────────────────────────────────────────

(defn parse-iso [text]
  "Epoch seconds for an ISO-8601 timestamp, or None."
  (when (not (isinstance text str)) (return None))
  (try
    (setv dt (datetime.fromisoformat (.replace text "Z" "+00:00")))
    (when (is dt.tzinfo None)
      (setv dt (.replace dt :tzinfo timezone.utc)))
    (.timestamp dt)
    (except [ValueError] None)))

(defn job-list [data]
  (setv jobs (if (isinstance data dict) (.get data "jobs" data) data))
  (cond
    (isinstance jobs list) (lfor j jobs :if (isinstance j dict) j)
    (isinstance jobs dict) (lfor j (.values jobs) :if (isinstance j dict) j)
    True []))

(defn is-active-job [job]
  (and (.get job "enabled" True)
       (not-in (.get job "state" "scheduled") INACTIVE-STATES)))

(defn profile-schedule [jobs-path now]
  "[earliest-due active-job-count] for a jobs.json; due is None with no
  active jobs. An active job without next_run_at is due now (upstream's
  tick computes it)."
  (try
    (with [f (open jobs-path :encoding "utf-8")]
      (setv data (json.load f)))
    (except [[OSError ValueError]] (return [None 0])))
  (setv active (lfor j (job-list data) :if (is-active-job j) j))
  (when (not active) (return [None 0]))
  (setv dues (lfor j active (or (parse-iso (.get j "next_run_at")) now)))
  [(min dues) (len active)])

;; ─── profile index ───────────────────────────────────────────────────────

(defn profile-homes [hermes-home]
  "[[name home] ...]: the default home plus every named profile directory.
  `default` always means HERMES_HOME itself, as upstream resolves it; a stray
  `profiles/default` directory is not a profile (it exists on the workstation,
  with no jobs, and used to shadow the root home's entry)."
  (setv out [["default" hermes-home]]
        root (os.path.join hermes-home "profiles"))
  (when (os.path.isdir root)
    (for [name (sorted (os.listdir root))]
      (setv home (os.path.join root name))
      (when (and (not (.startswith name ".")) (!= name "default") (os.path.isdir home))
        (.append out [name home]))))
  out)

(defclass ProfileIndex []
  "Each profile's earliest due time, re-read only when its jobs.json changes."
  (defn __init__ [self hermes-home]
    (setv self.hermes-home hermes-home
          self.entries {}))      ; name -> {"home" "mtime" "due" "jobs"}

  (defn jobs-path [self home] (os.path.join home "cron" "jobs.json"))

  (defn scan [self now]
    "Stat every jobs.json; re-read changed ones. Returns the names whose
    schedule changed (added, edited or removed)."
    (setv changed []
          seen (set))
    (for [[name home] (profile-homes self.hermes-home)]
      (.add seen name)
      (setv path (.jobs-path self home))
      (try (setv mtime (. (os.stat path) st_mtime_ns))
           (except [OSError] (setv mtime None)))
      (setv entry (.get self.entries name))
      (when (or (is entry None) (!= (get entry "mtime") mtime))
        (setv [due jobs] (if (is mtime None) [None 0] (profile-schedule path now)))
        (setv (get self.entries name) {"home" home "mtime" mtime "due" due "jobs" jobs})
        (.append changed name)))
    (for [name (list (.keys self.entries))]
      (when (not-in name seen)
        (del (get self.entries name))
        (.append changed name)))
    changed)

  (defn refresh [self name now]
    "Re-read one profile now (after it was ticked)."
    (setv entry (.get self.entries name))
    (when (is entry None) (return None))
    (setv path (.jobs-path self (get entry "home")))
    (try (setv (get entry "mtime") (. (os.stat path) st_mtime_ns))
         (except [OSError] (setv (get entry "mtime") None)))
    (setv [due jobs] (if (is (get entry "mtime") None) [None 0] (profile-schedule path now))
          (get entry "due") due
          (get entry "jobs") jobs)
    due)

  (defn totals [self]
    {"profiles" (len self.entries)
     "profiles_with_jobs" (sum (gfor e (.values self.entries) (int (> (get e "jobs") 0))))
     "jobs" (sum (gfor e (.values self.entries) (get e "jobs")))}))

;; ─── the heap scheduler ──────────────────────────────────────────────────

(defn usage-snapshot []
  "Process CPU seconds and peak RSS (MB) via getrusage."
  (import resource)
  (setv r (resource.getrusage resource.RUSAGE_SELF)
        ;; ru_maxrss is bytes on macOS, KiB on Linux.
        rss (if (= sys.platform "darwin") (/ r.ru_maxrss 1048576) (/ r.ru_maxrss 1024)))
  {"cpu_seconds" (round (+ r.ru_utime r.ru_stime) 3) "max_rss_mb" (round rss 1)})

(defclass ShardHost []
  (defn __init__ [self index tick-fn
                  [mode "observe"] [max-turns 4]
                  [rescan-seconds DEFAULT-RESCAN-SECONDS]
                  [housekeeping-seconds DEFAULT-HOUSEKEEPING-SECONDS]
                  [clock time.time]
                  [heartbeat-fn None]
                  [allow None] [reserved-profiles None] [before-tick None]]
    (when (not-in mode #{"observe" "run"})
      (raise (ValueError f"unknown shard mode {mode}")))
    (setv self.index index
          self.tick-fn tick-fn
          self.mode mode
          self.max-turns (max 1 max-turns)
          self.rescan-seconds rescan-seconds
          self.housekeeping-seconds housekeeping-seconds
          self.clock clock
          ;; run mode: (heartbeat-fn name home error-or-None), as upstream's
          ;; ticker loop records per-profile liveness after each tick.
          self.heartbeat-fn heartbeat-fn
          ;; Lease mode: (allow name) says whether this node holds the
          ;; profile's lease now. Checked when scheduling AND right before a
          ;; tick, so a lease lost between the two is never acted on.
          self.allow (or allow (fn [name] True))
          ;; (before-tick name home), e.g. drop a profile's cached secrets
          ;; when its short-lived GitHub lease is about to end.
          self.before-tick before-tick
          self.reserved-profiles (set (or reserved-profiles []))
          self.closed False
          self.stop-event None
          self.reserved-pool (when (and (= mode "run") self.reserved-profiles)
                               (ThreadPoolExecutor :max_workers 1 :thread_name_prefix "kotoba-shard-reserved"))
          self.last-host-beat None
          self.pass-started None     ; clock time the current step began, or None
          self.last-error None
          self.heap []
          self.version {}            ; name -> int, for lazy heap deletion
          self.seq 0
          self.lock (threading.RLock)
          self.running (set)
          self.pool (when (= mode "run") (ThreadPoolExecutor :max_workers self.max-turns
                                                              :thread_name_prefix "kotoba-shard"))
          self.last-rescan None
          self.rescan-ms None
          self.started-at (clock)
          self.cpu-at-start (get (usage-snapshot) "cpu_seconds")
          self.fires 0
          self.recent (deque :maxlen 50)
          self.cost-ema {}
          self.wake (threading.Event)))

  ;; -- heap --

  (defn schedule [self name due]
    "Put (or move) profile `name` on the heap at `due`; None removes it."
    (with [self.lock]
      (setv v (+ (.get self.version name 0) 1)
            (get self.version name) v)
      (when (is-not due None)
        (+= self.seq 1)
        (heapq.heappush self.heap #(due self.seq name v))))
    (.set self.wake))

  (defn next-due [self]
    "Earliest live heap entry's due time (dropping stale entries), or None."
    (with [self.lock]
      (while self.heap
        (setv [due _ name v] (get self.heap 0))
        (if (= v (.get self.version name))
            (return due)
            (heapq.heappop self.heap)))
      None))

  (defn pop-due [self now]
    "Live entries due at or before `now`, removed from the heap."
    (setv out [])
    (with [self.lock]
      (while self.heap
        (setv [due _ name v] (get self.heap 0))
        (cond
          (!= v (.get self.version name)) (heapq.heappop self.heap)
          (> due now) (break)
          True (do (heapq.heappop self.heap)
                   (setv (get self.version name) (+ v 1))
                   (.append out [name due])))))
    out)

  ;; -- index → heap --

  (defn housekeeping-due [self name now]
    "Run mode ticks every profile at least this often (upstream sweeps and
    heartbeats ride on a tick); spread by name so they don't align."
    (+ now (* self.housekeeping-seconds (+ 0.5 (/ (% (hash name) 1000) 2000)))))

  (defn rescan [self]
    (setv t0 (time.perf_counter)
          now (self.clock)
          changed (.scan self.index now))
    (for [name changed]
      (setv entry (.get self.index.entries name))
      (if (is entry None)
          (.schedule self name None)
          (when (not-in name self.running)
            (.schedule self name (.target-due self name (get entry "due") now)))))
    (setv self.last-rescan now
          ms (* 1000 (- (time.perf_counter) t0))
          self.rescan-ms (if (is self.rescan-ms None) ms (+ (* 0.7 self.rescan-ms) (* 0.3 ms))))
    changed)

  (defn target-due [self name due now]
    (cond
      (not (self.allow name)) None
      (= self.mode "observe") due
      (is due None) (.housekeeping-due self name now)
      True (min due (.housekeeping-due self name now))))

  ;; -- firing --

  (defn fire [self name due now]
    (when (or self.closed (not (self.allow name)))
      (return))
    (+= self.fires 1)
    (.append self.recent {"profile" name "due" due "at" now
                          "late_seconds" (round (- now due) 1) "mode" self.mode})
    (if (= self.mode "observe")
        ;; Upstream still ticks in observe mode and will advance jobs.json;
        ;; the next rescan picks that up. Until then don't re-observe.
        None
        (with [self.lock]
          (when (or self.closed (in name self.running)) (return))
          (.add self.running name)
          (.submit (if (in name self.reserved-profiles) self.reserved-pool self.pool) self.tick-profile name))))

  (defn reapply-leases [self]
    "The lease set changed: (re)schedule every indexed profile against it."
    (setv now (self.clock))
    (for [[name entry] (list (.items self.index.entries))]
      (when (not-in name self.running)
        (.schedule self name (.target-due self name (get entry "due") now)))))

  (defn tick-profile [self name]
    (when (not (self.allow name))
      (with [self.lock] (.discard self.running name))
      (return))
    (setv entry (.get self.index.entries name)
          t0 (time.perf_counter)
          error None)
    (when (and entry self.before-tick)
      (try (self.before-tick name (get entry "home"))
           (except [e Exception]
             (print f"[kotoba-shard] before-tick {name} failed: {e !r}" :file sys.stderr))))
    (try
      (when entry (self.tick-fn name (get entry "home")))
      (except [e Exception]
        (setv error f"{(. (type e) __name__)}: {e}"
              self.last-error error)
        (print f"[kotoba-shard] tick {name} failed: {e !r}" :file sys.stderr))
      (finally
        (when (and entry self.heartbeat-fn)
          (try (self.heartbeat-fn name (get entry "home") error)
               (except [e Exception]
                 (print f"[kotoba-shard] heartbeat {name} failed: {e !r}" :file sys.stderr))))
        (setv dt (- (time.perf_counter) t0)
              prev (.get self.cost-ema name)
              (get self.cost-ema name) (if (is prev None) dt
                                           (+ (* (- 1 COST-EMA-ALPHA) prev) (* COST-EMA-ALPHA dt))))
        (with [self.lock] (.discard self.running name))
        (setv now (self.clock))
        (.schedule self name (.target-due self name (.refresh self.index name now) now)))))

  ;; -- loop --

  (defn is-stalled [self now]
    "The current scheduler pass has run longer than HOST-STALL-SECONDS."
    (setv started self.pass-started)
    (and (is-not started None) (>= (- now started) HOST-STALL-SECONDS)))

  (defn host-heartbeat [self now]
    "Run mode: keep the default home's ticker heartbeat fresh while the loop
    makes progress, so watchers of that file see a live cron ticker."
    (when (and self.heartbeat-fn
               (not (.is-stalled self now))
               (or (is self.last-host-beat None)
                   (>= (- now self.last-host-beat) HOST-HEARTBEAT-SECONDS)))
      (setv self.last-host-beat now)
      (try (self.heartbeat-fn "default" self.index.hermes-home None)
           (except [e Exception]
             (print f"[kotoba-shard] host heartbeat failed: {e !r}" :file sys.stderr)))))

  (defn step [self]
    "One scheduler pass: rescan if due, fire what's due. Returns seconds to sleep."
    (setv now (self.clock)
          self.pass-started now)
    (try
      (when (or (is self.last-rescan None) (>= (- now self.last-rescan) self.rescan-seconds))
        (.rescan self))
      (for [[name due] (.pop-due self now)]
        (.fire self name due now))
      (finally (setv self.pass-started None)))
    (setv nd (.next-due self)
          until-rescan (- (+ self.last-rescan self.rescan-seconds) (self.clock))
          wait (if (is nd None) until-rescan (min until-rescan (- nd (self.clock)))))
    (max 0.05 wait))

  (defn heartbeat-forever [self stop-event]
    (while (not (.is-set stop-event))
      (.host-heartbeat self (self.clock))
      (.wait stop-event HOST-HEARTBEAT-SECONDS)))

  (defn run-forever [self stop-event]
    (while (not (.is-set stop-event))
      (setv wait (.step self))
      (.clear self.wake)
      (.wait self.wake (min wait 60))))

  (defn start [self]
    (setv stop (threading.Event) self.stop-event stop)
    (.start (threading.Thread :target self.run-forever :args #(stop)
                              :name "kotoba-shard" :daemon True))
    (when self.heartbeat-fn
      (.start (threading.Thread :target self.heartbeat-forever :args #(stop)
                                :name "kotoba-shard-heartbeat" :daemon True)))
    stop)

  (defn close [self [wait True]]
    "Stop admission and cancel unstarted ticks; active jobs retain their receipts."
    (with [self.lock]
      (setv self.closed True)
      (when self.stop-event (.set self.stop-event)))
    (for [pool [self.pool self.reserved-pool]]
      (when pool (.shutdown pool :wait wait :cancel_futures True))))

  (defn status [self]
    (setv now (self.clock)
          usage (usage-snapshot)
          uptime (max 1e-6 (- now self.started-at))
          nd (.next-due self))
    {"object" "kotoba.shard"
     "mode" self.mode
     "max_turns" self.max-turns
     "reserved_profiles" (sorted self.reserved-profiles)
     "reserved_turns" (if self.reserved-pool 1 0)
     "closed" self.closed
     #** (.totals self.index)
     "heap" (len self.heap)
     "running" (sorted self.running)
     "next_due_in_seconds" (when nd (round (- nd now) 1))
     "rescan_seconds" self.rescan-seconds
     "rescan_ms" (when self.rescan-ms (round self.rescan-ms 1))
     "fires" self.fires
     "last_tick_error" self.last-error
     "last_host_heartbeat" self.last-host-beat
     "scheduled_profiles" (len (sfor n self.index.entries :if (self.allow n) n))
     "recent_fires" (list self.recent)
     "uptime_seconds" (round uptime 1)
     "cpu_percent" (round (* 100 (/ (- (get usage "cpu_seconds") self.cpu-at-start) uptime)) 2)
     #** usage}))

;; ─── upstream integration ────────────────────────────────────────────────

(defn upstream-tick []
  "tick-fn that runs Hermes' own cron tick inside one profile's scope."
  (import cron.scheduler_provider [_profile_cron_scope]
          cron.scheduler_tick [tick])
  (fn [name home]
    (with [(_profile_cron_scope home)]
      (tick :verbose False :adapters None :loop None :sync True))))

(defn upstream-heartbeat []
  "heartbeat-fn that records ticker liveness with Hermes' own markers, in the
  profile's scope (`cron/ticker_heartbeat`, `ticker_last_success`,
  `ticker_last_error`) -- what upstream's ticker loop writes after a cycle."
  (import cron.scheduler_provider [_profile_cron_scope]
          cron.jobs [record_ticker_heartbeat clear_ticker_error record_ticker_error])
  (fn [name home error]
    (with [(_profile_cron_scope home)]
      (record_ticker_heartbeat :success (is error None))
      (if (is error None)
          (clear_ticker_error)
          (record_ticker_error error)))))

(defn upstream-secret-refresh [due-fn]
  "before-tick hook: when (due-fn profile) says the profile's lease ends soon,
  drop upstream's per-home secret-source cache so the next tick runs the
  secrets.command helper again (upstream caches it for the process lifetime)."
  (import hermes_cli.env_loader :as el
          kotoba_gateway.fleet_secrets [profile-of-home])
  (fn [name home]
    (when (due-fn (profile-of-home home))
      (setv key (str (.resolve (Path home))))
      (with [el._SECRET_SOURCE_CACHE_LOCK]
        (.discard el._APPLIED_HOMES key)
        (.pop el._SECRET_SOURCE_VALUES_BY_HOME key None)))))

(defn live-multiplexer [hermes-home]
  "The upstream multiplexer's pid when its gateway_state.json says it is
  running and the pid is alive, else None."
  (try
    (with [f (open (os.path.join hermes-home "gateway_state.json") :encoding "utf-8")]
      (setv record (json.load f)))
    (except [[OSError ValueError]] (return None)))
  (setv pid (.get record "pid"))
  (when (or (!= (.get record "gateway_state" "running") "running") (not (isinstance pid int)))
    (return None))
  (try (os.kill pid 0) (except [OSError] (return None)))
  pid)

;; ─── cost report ─────────────────────────────────────────────────────────

(defn execution-history [home now]
  "[[duration-seconds ...] runs-in-window] from a profile's executions.db."
  (setv db (os.path.join home "cron" "executions.db"))
  (when (not (os.path.isfile db)) (return [[] 0]))
  (setv since (- now (* HISTORY-DAYS 86400)))
  (try
    (setv conn (sqlite3.connect f"file:{db}?mode=ro" :uri True :timeout 1))
    (try
      (setv rows (.fetchall (.execute conn "SELECT started_at, finished_at FROM executions WHERE status IN ('completed','failed') AND started_at IS NOT NULL AND finished_at IS NOT NULL ORDER BY started_at")))
      (finally (.close conn)))
    (except [sqlite3.Error] (return [[] 0])))
  (setv durations [] recent 0)
  (for [[s f] rows]
    (setv a (parse-iso s) b (parse-iso f))
    (when (and a b (>= b a))
      (.append durations (- b a))
      (when (>= a since) (+= recent 1))))
  [durations recent])

(defn ema [values [alpha COST-EMA-ALPHA]]
  (setv out None)
  (for [v values] (setv out (if (is out None) v (+ (* (- 1 alpha) out) (* alpha v)))))
  out)

(defn cost-report [hermes-home [nodes 9] [turns-per-node 5] [target-utilization 0.5] [now None]
                   [top 20]]
  "Per-profile cost (EMA of run duration) and fire rate from execution
  history, and what that means for placement on `nodes` fleet nodes."
  (setv now (or now (time.time))
        profiles [])
  (for [[name home] (profile-homes hermes-home)]
    (setv [durations recent] (execution-history home now))
    (when durations
      (setv cost (ema durations)
            per-day (/ recent HISTORY-DAYS))
      (.append profiles {"profile" name "runs" (len durations)
                         "cost_ema_seconds" (round cost 2)
                         "fires_per_day" (round per-day 2)
                         "busy_seconds_per_day" (round (* cost per-day) 1)})))
  (.sort profiles :key (fn [p] (- (get p "busy_seconds_per_day"))))
  (setv busy (sum (gfor p profiles (get p "busy_seconds_per_day")))
        node-capacity (* 86400 turns-per-node target-utilization)
        fires (sum (gfor p profiles (get p "fires_per_day"))))
  {"object" "kotoba.shard.cost_report"
   "history_days" HISTORY-DAYS
   "profiles_with_history" (len profiles)
   "fires_per_day" (round fires 1)
   "busy_seconds_per_day" (round busy 1)
   "mean_concurrent_turns" (round (/ busy 86400) 2)
   "assumptions" {"nodes" nodes "turns_per_node" turns-per-node
                  "target_utilization" target-utilization}
   "nodes_needed_at_target" (round (/ busy node-capacity) 2)
   "utilization_on_nodes" (round (/ busy (* 86400 turns-per-node nodes)) 4)
   "profiles_per_node" (round (/ (len (profile-homes hermes-home)) nodes) 1)
   "costliest" (if top (cut profiles top) profiles)})
