;; Fleet secrets through kagi: a profile's secrets reach only the node that
;; runs it, without a central store and without plaintext files on that node.
;;
;; Owner side (the workstation holds the kagi vault):
;;   - each profile's .env becomes the kagi item `hermes-env.<profile>`, and
;;     the env every profile shares (provider keys) becomes `hermes-env.fleet`,
;;     both in compartment `hermes-fleet`;
;;   - each node is a kagi agent principal (its own key, approved by
;;     fingerprint). The owner grants a node exactly the items of the
;;     profiles placement gives it, and ungrants (re-keys) on a move;
;;   - the vault file is ciphertext, so it is copied to every node as is.
;;
;; Node side: upstream Hermes' `command` secret source runs a helper once per
;; profile home and keeps the KEY=VALUE map it prints in that profile's secret
;; scope (never os.environ, never disk). `tools/kagi_env.hy` is that helper:
;; it opens the granted items from the node's vault copy with the node's own
;; agent key (`kagi agent get`, offline, audited in the node's own chain).

(import json os re subprocess time)

(setv COMPARTMENT "hermes-fleet"
      FLEET-ITEM "hermes-env.fleet"
      ITEM-PREFIX "hermes-env."
      ;; The helper starts a JVM per item (~3 s each on an M-series Mac).
      HELPER-TIMEOUT-SECONDS 120
      NODE-CONFIG "~/.kagi-agent/node.json"
      ;; GitHub leases (kagi ADR-0003) last at most an hour; a profile's
      ;; cached secrets are dropped this long before its lease expires.
      LEASE-MARGIN-SECONDS 600
      LEASE-STATE-DIR "~/.kagi-agent/lease-expiry")

(defn item-name [profile]
  (+ ITEM-PREFIX profile))

(defn profile-of-home [hermes-home]
  "The profile a HERMES_HOME belongs to: `<root>/profiles/<name>` -> name,
  anything else (the root home) -> \"default\"."
  (setv path (os.path.normpath hermes-home)
        parent (os.path.dirname path))
  (if (= (os.path.basename parent) "profiles")
      (os.path.basename path)
      "default"))

(defn parse-env [text]
  "KEY=VALUE lines -> dict (comments, blanks and `export ` skipped; later wins)."
  (setv out {})
  (for [line (.splitlines (or text ""))]
    (setv line (.strip line))
    (when (.startswith line "export ")
      (setv line (.strip (cut line 7 None))))
    (when (and line (not (.startswith line "#")) (in "=" line))
      (setv [k v] (.split line "=" 1)
            k (.strip k))
      (when (and k (.isidentifier k))
        (setv (get out k) (.strip v)))))
  out)

(defn render-env [mapping]
  (.join "" (gfor [k v] (.items mapping) f"{k}={v}\n")))

(defn merge-envs [texts]
  "Fleet env first, then the profile's: a profile value overrides the fleet's."
  (setv out {})
  (for [t texts] (.update out (parse-env t)))
  out)

;; ─── node side: open items with the node's agent key ─────────────────────

(defn load-node-config [[path None]]
  "{kagi_bin kagi_home identity_ref agent_id [path]} written when the node was enrolled."
  (with [f (open (os.path.expanduser (or path NODE-CONFIG)) :encoding "utf-8")]
    (json.load f)))

(defn is-absent [stdout]
  "`kagi agent get` prints an EDN status map instead of a value when the item
  is missing or not granted to this agent."
  (.startswith (.lstrip stdout) "{:status"))

(defn kagi-env [cfg]
  {"HOME" (os.path.expanduser "~")
   "PATH" (.get cfg "path" "/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin")
   "KAGI_HOME" (os.path.expanduser (get cfg "kagi_home"))
   "KAGI_IDENTITY_REF" (get cfg "identity_ref")
   "KAGI_AGENT_ID" (get cfg "agent_id")})

(defn kagi-get [cfg item purpose [run subprocess.run]]
  "The item's plaintext, or None when it is absent/not granted. Raises on any
  other failure, so the helper exits non-zero and upstream retries later."
  (setv r (run [(os.path.expanduser (get cfg "kagi_bin")) "agent" "get" item "--purpose" purpose]
               :env (kagi-env cfg) :capture_output True :text True :timeout HELPER-TIMEOUT-SECONDS))
  (cond
    (is-absent r.stdout) None
    ;; an item that was never put (e.g. a profile with no secrets)
    (in "no such item" (.lower (+ (or r.stdout "") (or r.stderr "")))) None
    (!= r.returncode 0) (raise (RuntimeError f"kagi agent get {item} exited {r.returncode}"))
    True r.stdout))

(defn parse-lease-result [stdout]
  "The EDN map `kagi lease request` prints -> {status lease_id expires_at}."
  (setv status (re.search #[[:status :([a-z-]+)]] stdout)
        lease-id (re.search #[[:lease/id "([^"]+)"]] stdout)
        expires (re.search r":lease/expires-at (\d+)" stdout))
  {"status" (when status (.group status 1))
   "lease_id" (when lease-id (.group lease-id 1))
   "expires_at" (when expires (int (.group expires 1)))})

(defn lease-expiry-path [profile [state-dir None]]
  (os.path.join (os.path.expanduser (or state-dir LEASE-STATE-DIR)) profile))

(defn lease-env [cfg profile [run subprocess.run] [state-dir None]]
  "GH_TOKEN for this profile from a kagi lease, when the node is configured for
  one (`github_lease` {from access resources} in node.json). A lease that is
  pending or denied is not fatal: the profile runs without the token, and the
  reason goes to stderr."
  (setv spec (.get cfg "github_lease"))
  (when (not spec) (return {}))
  (setv kagi (os.path.expanduser (get cfg "kagi_bin"))
        resources (get spec "resources")
        r (run [kagi "lease" "request" "--from" (get spec "from")
                "--access" (.get spec "access" "contents:read")
                "--resources" (if (isinstance resources str) resources (.join "," resources))
                "--purpose" f"hermes-cron:{profile}"]
               :env (kagi-env cfg) :capture_output True :text True :timeout HELPER-TIMEOUT-SECONDS)
        lease (parse-lease-result (or r.stdout "")))
  (when (!= (get lease "status") "issued")
    (import sys)
    (setv why (get lease "status"))
    (print f"kagi_env: no GitHub lease for {profile}: {why}" :file sys.stderr)
    (return {}))
  (setv o (run [kagi "lease" "open" "--lease" (get lease "lease_id")]
               :env (kagi-env cfg) :capture_output True :text True :timeout HELPER-TIMEOUT-SECONDS))
  (when (or (!= o.returncode 0) (not o.stdout))
    (raise (RuntimeError f"kagi lease open exited {o.returncode}")))
  (setv path (lease-expiry-path profile state-dir))
  (os.makedirs (os.path.dirname path) :exist_ok True)
  (with [f (open path "w" :encoding "utf-8")] (.write f (str (get lease "expires_at"))))
  {"GH_TOKEN" (.strip o.stdout) "GITHUB_TOKEN" (.strip o.stdout)})

(defn lease-refresh-due [profile [now None] [margin LEASE-MARGIN-SECONDS] [state-dir None]]
  "True when this profile holds a lease that ends within `margin` seconds, so
  its cached secrets should be dropped before the next tick."
  (try
    (with [f (open (lease-expiry-path profile state-dir) :encoding "utf-8")]
      (setv expires (float (.strip (.read f)))))
    (except [[OSError ValueError]] (return False)))
  (< (- expires (or now (time.time))) margin))

(defn profile-env [cfg hermes-home [run subprocess.run] [state-dir None]]
  "The merged KEY=VALUE map the node may give this profile: fleet env, the
  profile's own env, then a GitHub lease when configured."
  (setv profile (profile-of-home hermes-home)
        purpose f"hermes-cron:{profile}"
        texts (lfor item [FLEET-ITEM (item-name profile)]
                    :setv text (kagi-get cfg item purpose :run run)
                    :if (is-not text None)
                    text))
  (setv out (merge-envs texts))
  ;; Once broker leases are configured, old environment tokens must not
  ;; survive a pending or denied request. The lease is the only GitHub
  ;; credential source for this profile.
  (when (.get cfg "github_lease")
    (.pop out "GH_TOKEN" None)
    (.pop out "GITHUB_TOKEN" None))
  (.update out (lease-env cfg profile :run run :state-dir state-dir))
  out)

;; ─── owner side: what a staged profile's config.yaml gets ────────────────

(defn secrets-command-block [python gateway-dir]
  "The `secrets.command` config upstream reads per profile home."
  {"enabled" True
   "command" (+ f"PYTHONPATH={gateway-dir}/.deps:{gateway-dir} "
                f"{python} -m hy {gateway-dir}/tools/kagi_env.hy")
   "helper_timeout_seconds" HELPER-TIMEOUT-SECONDS})

(defn with-secrets-command [config block]
  "config (a parsed config.yaml dict) with secrets.command set to block,
  other secrets sources untouched."
  (setv config (dict (or config {}))
        secrets (dict (or (.get config "secrets") {})))
  (setv (get secrets "command") block
        (get config "secrets") secrets)
  config)
