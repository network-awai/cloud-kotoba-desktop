;; Shard host: one heap for all profiles' cron schedules, ticking only due
;; profiles. Upstream tick is replaced by a recording fake; time by a clock.

(import json os sqlite3 tempfile threading time unittest
        datetime [datetime timezone timedelta])
(import kotoba_gateway.shard [ProfileIndex ShardHost HOST-STALL-SECONDS cost-report live-multiplexer
                              profile-schedule]
        kotoba_gateway.server [default-state-dir])

(setv T0 1791500000.0)   ; 2026-10-08T... UTC, an arbitrary fixed "now"

(defn iso [epoch]
  (.isoformat (datetime.fromtimestamp epoch timezone.utc)))

(defn write-jobs [home jobs]
  (os.makedirs (os.path.join home "cron") :exist_ok True)
  (setv path (os.path.join home "cron" "jobs.json"))
  (with [f (open path "w")] (json.dump {"jobs" jobs} f))
  ;; distinct mtimes even within one clock tick of the filesystem
  (setv st (os.stat path))
  (os.utime path :ns #(st.st_atime_ns (+ st.st_mtime_ns 1000000)))
  path)

(defn job [due [enabled True] [state "scheduled"]]
  {"id" "j" "enabled" enabled "state" state "next_run_at" (when due (iso due))})

(defclass Clock []
  (defn __init__ [self t] (setv self.t t))
  (defn __call__ [self] self.t))

(defclass Fixture [unittest.TestCase]
  (defn setUp [self]
    (setv self.tmp (tempfile.TemporaryDirectory)
          self.home self.tmp.name)
    (os.makedirs (os.path.join self.home "profiles") :exist_ok True))
  (defn tearDown [self] (.cleanup self.tmp))
  (defn profile [self name jobs]
    (setv home (os.path.join self.home "profiles" name))
    (os.makedirs home :exist_ok True)
    (write-jobs home jobs)
    home))

(defclass Schedules [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Earliest active job sets a profile's due time]]
  (defn test-earliest-active-job [self]
    (setv home (.profile self "a" [(job (+ T0 300)) (job (+ T0 100))
                                   (job (+ T0 50) :enabled False)
                                   (job (+ T0 10) :state "paused")]))
    (.assertEqual self (profile-schedule (os.path.join home "cron" "jobs.json") T0)
                  [(+ T0 100) 2])
    (setv home2 (.profile self "b" [(job None)]))
    (.assertEqual self (get (profile-schedule (os.path.join home2 "cron" "jobs.json") T0) 0) T0)
    (setv home3 (.profile self "c" [(job (+ T0 5) :state "paused")]))
    (.assertEqual self (profile-schedule (os.path.join home3 "cron" "jobs.json") T0) [None 0]))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Unchanged profiles cost one stat]]
  (defn test-rescan-rereads-only-changed [self]
    (for [i (range 50)] (.profile self f"p{i}" [(job (+ T0 3600))]))
    (setv index (ProfileIndex self.home))
    (.assertEqual self (len (.scan index T0)) 51)        ; default + 50, first sight
    (.assertEqual self (.scan index T0) [])              ; nothing changed
    (write-jobs (os.path.join self.home "profiles" "p7") [(job (+ T0 60))])
    (.assertEqual self (.scan index T0) ["p7"])
    (.assertEqual self (get index.entries "p7" "due") (+ T0 60))))

(defclass Host [Fixture]
  (defn host [self mode clock calls [max-turns 2] [on-tick None] [reserved-profiles None]]
    (defn tick-fn [name home]
      (.append calls name)
      (when on-tick (on-tick name home)))
    (ShardHost (ProfileIndex self.home) tick-fn :mode mode :max-turns max-turns
               :reserved-profiles reserved-profiles
               :rescan-seconds 30 :housekeeping-seconds 1e9 :clock clock))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Only due profiles are ticked]]
  (defn test-only-due-profiles-tick [self]
    (.profile self "soon" [(job (+ T0 100))])
    (.profile self "later" [(job (+ T0 10000))])
    (.profile self "idle" [])
    (setv clock (Clock T0) calls []
          host (.host self "run" clock calls
                      :on-tick (fn [name home] (write-jobs home [(job (+ T0 86400))]))))
    (.step host)
    (.assertEqual self calls [])                         ; nothing due yet
    (setv clock.t (+ T0 150))
    (.step host)
    (.shutdown host.pool :wait True)
    (.assertEqual self calls ["soon"])
    ;; ticked profile re-read and rescheduled for its advanced next_run_at
    (.assertAlmostEqual self (.next-due host) (+ T0 10000)))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Observe mode never executes]]
  (defn test-observe-never-ticks [self]
    (.profile self "a" [(job (- T0 5))])
    (.profile self "b" [(job (- T0 1))])
    (setv clock (Clock T0) calls [] host (.host self "observe" clock calls))
    (.step host)
    (.step host)
    (.assertEqual self calls [])
    (.assertEqual self host.fires 2)
    (.assertEqual self (sorted (lfor r host.recent (get r "profile"))) ["a" "b"]))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Concurrent ticks are bounded]]
  (defn test-bounded-concurrency [self]
    (for [i (range 6)] (.profile self f"p{i}" [(job (- T0 1))]))
    (setv gate (threading.Event) lock (threading.Lock) state {"now" 0 "peak" 0})
    (defn on-tick [name home]
      (with [lock]
        (+= (get state "now") 1)
        (setv (get state "peak") (max (get state "peak") (get state "now"))))
      (.wait gate 2)
      (write-jobs home [(job (+ T0 86400))])
      (with [lock] (-= (get state "now") 1)))
    (setv calls [] host (.host self "run" (Clock T0) calls :max-turns 2 :on-tick on-tick))
    (.step host)
    (time.sleep 0.2)
    (.set gate)
    (.shutdown host.pool :wait True)
    (.assertEqual self (len calls) 6)
    (.assertEqual self (get state "peak") 2))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Reserved profiles progress during slow ordinary ticks]]
  (defn test-reserved-profile-progress [self]
    (.profile self "ordinary" [(job (- T0 2))])
    (.profile self "itonami" [(job (- T0 1))])
    (setv gate (threading.Event) ordinary-started (threading.Event) critical (threading.Event))
    (defn on-tick [name home]
      (if (= name "ordinary")
          (do (.set ordinary-started) (.wait gate 2))
          (.set critical))
      (write-jobs home [(job (+ T0 86400))]))
    (setv calls [] host (.host self "run" (Clock T0) calls :max-turns 1
                       :on-tick on-tick :reserved-profiles ["itonami"]))
    (try
      (.step host)
      (.assertTrue self (.wait ordinary-started 1))
      (.assertTrue self (.wait critical 0.5))
      (.assertFalse self (.is-set gate))
      (.assertEqual self (get (.status host) "reserved_turns") 1)
      (.assertEqual self (get (.status host) "reserved_profiles") ["itonami"])
      (finally (.set gate) (.close host)))
    (.assertEqual self (set calls) #{"ordinary" "itonami"})
    (.assertTrue self host.closed)
    (.fire host "itonami" T0 T0)
    (.assertEqual self (len calls) 2))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Reserved ticks retain lease checks]]
  (defn test-reserved-profile-lease-check [self]
    (.profile self "itonami" [(job (- T0 1))])
    (setv calls [] host (.host self "run" (Clock T0) calls :reserved-profiles ["itonami"]))
    (setv host.allow (fn [name] False))
    (try (.step host) (finally (.close host)))
    (.assertEqual self calls []))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Run mode refuses a live multiplexer]]
  (defn test-live-multiplexer-guard [self]
    (setv state (os.path.join self.home "gateway_state.json"))
    (.assertIsNone self (live-multiplexer self.home))
    (with [f (open state "w")] (json.dump {"gateway_state" "running" "pid" (os.getpid)} f))
    (.assertEqual self (live-multiplexer self.home) (os.getpid))
    (with [f (open state "w")] (json.dump {"gateway_state" "stopped" "pid" (os.getpid)} f))
    (.assertIsNone self (live-multiplexer self.home))))

(defclass Heartbeats [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Run mode keeps ticker heartbeats]]
  (defn test-heartbeats [self]
    (.profile self "ok" [(job (- T0 1))])
    (.profile self "bad" [(job (- T0 1))])
    (setv beats [] clock (Clock T0))
    (defn tick-fn [name home]
      (write-jobs home [(job (+ T0 86400))])
      (when (= name "bad") (raise (RuntimeError "boom"))))
    (setv host (ShardHost (ProfileIndex self.home) tick-fn :mode "run" :max-turns 2
                          :rescan-seconds 30 :housekeeping-seconds 1e9 :clock clock
                          :heartbeat-fn (fn [name home error] (.append beats [name error]))))
    (.step host)
    (.shutdown host.pool :wait True)
    (setv by-name (dict beats))
    (.assertIsNone self (get by-name "ok"))
    (.assertIn self "boom" (get by-name "bad"))
    (.assertNotIn self "default" by-name "the host beat has its own thread, not step")
    ;; host beat at most once a minute
    (defn host-beats [] (len (lfor [nm _] beats :if (= nm "default") nm)))
    (.host-heartbeat host T0)
    (.assertEqual self (host-beats) 1)
    (.host-heartbeat host (+ T0 30))
    (.assertEqual self (host-beats) 1)
    (.host-heartbeat host (+ T0 61))
    (.assertEqual self (host-beats) 2)
    (.assertIsNone self (get (dict beats) "default")))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Observe mode writes no heartbeats]]
  (defn test-observe-writes-nothing [self]
    (.profile self "a" [(job (- T0 1))])
    (setv beats []
          host (ShardHost (ProfileIndex self.home) None :mode "observe" :clock (Clock T0)
                          :heartbeat-fn None))
    (.step host)
    (.assertEqual self beats [])))

(defclass RootHome [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#A profiles/default directory never shadows the root home]]
  (defn test-profiles-default-is-not-a-profile [self]
    (write-jobs self.home [(job (+ T0 10))])
    (.profile self "default" [(job (+ T0 99))])
    (setv index (ProfileIndex self.home))
    (.scan index T0)
    (.assertEqual self (get index.entries "default" "home") self.home)
    (.assertEqual self (get index.entries "default" "due") (+ T0 10))
    (setv beats []
          host (ShardHost index (fn [n h]) :mode "run" :clock (Clock T0)
                          :rescan-seconds 30 :housekeeping-seconds 1e9
                          :heartbeat-fn (fn [name home error] (.append beats home))))
    (.host-heartbeat host T0)
    (.assertEqual self beats [self.home]))

  ;; @lat: [[profile-distribution#Shard host implementation#Tests#The host heartbeat survives slow passes but not a wedged loop]]
  (defn test-host-heartbeat-during-slow-pass [self]
    (setv beats []
          clock (Clock T0)
          host (ShardHost (ProfileIndex self.home) (fn [n h]) :mode "run" :clock clock
                          :heartbeat-fn (fn [name home error] (.append beats clock.t))))
    ;; a pass that started 20 min ago is slow, not dead: keep beating
    (setv host.pass-started (- T0 1200))
    (.host-heartbeat host T0)
    (.assertEqual self beats [T0])
    ;; past the stall bound the beats stop, so the guard can take over
    (setv clock.t (+ T0 (- HOST-STALL-SECONDS 1200)))
    (.host-heartbeat host clock.t)
    (.assertEqual self beats [T0])
    ;; the pass finishes: beats resume
    (setv host.pass-started None)
    (.host-heartbeat host clock.t)
    (.assertEqual self beats [T0 clock.t])))

(defclass BeforeTick [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#The before-tick hook runs before each tick and cannot block it]]
  (defn test-before-tick [self]
    (.profile self "ok" [(job (- T0 1))])
    (setv order [])
    (defn tick-fn [name home] (.append order ["tick" name]) (write-jobs home [(job (+ T0 86400))]))
    (defn hook [name home] (.append order ["before" name]) (raise (RuntimeError "hook down")))
    (setv host (ShardHost (ProfileIndex self.home) tick-fn :mode "run" :max-turns 1
                          :rescan-seconds 30 :housekeeping-seconds 1e9 :clock (Clock T0)
                          :before-tick hook))
    (.step host)
    (.shutdown host.pool :wait True)
    (.assertEqual self order [["before" "ok"] ["tick" "ok"]])))

(defclass NodeState [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Node state lives outside HERMES_HOME]]
  (defn test-node-state-moves-out [self]
    (setv root (os.path.join self.home "kotoba-root"))
    (with [f (open (os.path.join self.home "kotoba-node.key") "w")] (.write f "KEY"))
    (setv (get os.environ "KOTOBA_STATE_ROOT") root)
    (try
      (setv state (default-state-dir self.home))
      (finally (del (get os.environ "KOTOBA_STATE_ROOT"))))
    (.assertTrue self (.startswith state root))
    (.assertFalse self (os.path.exists (os.path.join self.home "kotoba-node.key")))
    (with [f (open (os.path.join state "kotoba-node.key"))]
      (.assertEqual self (.read f) "KEY"))
    ;; idempotent, and a second home gets its own directory
    (setv (get os.environ "KOTOBA_STATE_ROOT") root)
    (try
      (.assertEqual self (default-state-dir self.home) state)
      (.assertNotEqual self (default-state-dir (os.path.join self.home "profiles")) state)
      (finally (del (get os.environ "KOTOBA_STATE_ROOT"))))))

(defclass Costs [Fixture]
  ;; @lat: [[profile-distribution#Shard host implementation#Tests#Cost report comes from execution history]]
  (defn test-cost-report [self]
    (setv home (.profile self "busy" [(job (+ T0 60))])
          db (sqlite3.connect (os.path.join home "cron" "executions.db")))
    (.execute db "CREATE TABLE executions (id TEXT, job_id TEXT, status TEXT, started_at TEXT, finished_at TEXT)")
    ;; 14 runs over the last 7 days, 60 s each
    (for [i (range 14)]
      (setv start (- T0 (* i 43200) 100))
      (.execute db "INSERT INTO executions VALUES (?,?,?,?,?)"
                #(f"e{i}" "j" "completed" (iso start) (iso (+ start 60)))))
    (.commit db) (.close db)
    (setv report (cost-report self.home :nodes 2 :turns-per-node 1 :now T0)
          busy (get report "costliest" 0))
    (.assertEqual self (get busy "profile") "busy")
    (.assertAlmostEqual self (get busy "cost_ema_seconds") 60 :places 1)
    (.assertAlmostEqual self (get busy "fires_per_day") 2 :places 1)
    (.assertAlmostEqual self (get report "busy_seconds_per_day") 120 :places 0)))

(when (= __name__ "__main__")
  (unittest.main))
