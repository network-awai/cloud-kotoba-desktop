;; Fleet secrets via kagi: kagi is replaced by a fake runner standing in for
;; `kagi agent get`, which returns a value only for items granted to the node.

(import os tempfile types unittest
        kotoba_gateway.fleet_secrets [FLEET-ITEM item-name profile-of-home parse-env
                                      merge-envs kagi-get profile-env
                                      secrets-command-block with-secrets-command
                                      lease-env lease-refresh-due])

(setv CFG {"kagi_bin" "/k/bin/kagi" "kagi_home" "/k/home"
           "identity_ref" "file:///k/id" "agent_id" "agent-1"})

(defn fake-kagi [granted [calls None]]
  "A subprocess.run stand-in: granted maps item -> plaintext."
  (fn [argv #** kw]
    (when (is-not calls None) (.append calls [argv (get kw "env")]))
    (setv item (get argv 3))
    (cond
      (in item granted) (types.SimpleNamespace :returncode 0 :stdout (get granted item) :stderr "")
      (= item FLEET-ITEM) (types.SimpleNamespace :returncode 0 :stderr ""
                                                 :stdout "{:status :absent, :vault-home \"/k/home\"}\n")
      ;; never put at all: kagi exits 1
      True (types.SimpleNamespace :returncode 1 :stdout "" :stderr (+ "no such item: " item)))))

(defclass Names [unittest.TestCase]
  (defn test-profile-of-home [self]
    (.assertEqual self (profile-of-home "/u/.hermes/profiles/x402-pnl") "x402-pnl")
    (.assertEqual self (profile-of-home "/u/.hermes/profiles/x402-pnl/") "x402-pnl")
    (.assertEqual self (profile-of-home "/u/.hermes") "default")
    (.assertEqual self (item-name "x402-pnl") "hermes-env.x402-pnl"))

  (defn test-parse-env [self]
    (.assertEqual self (parse-env "# c\nA=1\nexport B = two\n\nbad line\n9X=no\nA=3\n")
                  {"A" "3" "B" "two"})))

(defclass Delivery [unittest.TestCase]
  ;; @lat: [[profile-distribution#Fleet secrets#Tests#A node gets the fleet env plus only its own profile's env]]
  (defn test-profile-env [self]
    (setv calls []
          run (fake-kagi {FLEET-ITEM "KOTOBA_API_TOKEN=fleet\nSHARED=fleet\n"
                          "hermes-env.p1" "SHARED=mine\nP1_KEY=k1\n"}
                         calls))
    (.assertEqual self (profile-env CFG "/u/.hermes/profiles/p1" :run run)
                  {"KOTOBA_API_TOKEN" "fleet" "SHARED" "mine" "P1_KEY" "k1"})
    ;; the agent's identity and the profile in the audit purpose
    (setv [argv env] (get calls 1))
    (.assertEqual self argv ["/k/bin/kagi" "agent" "get" "hermes-env.p1" "--purpose" "hermes-cron:p1"])
    (.assertEqual self (get env "KAGI_AGENT_ID") "agent-1")
    ;; a profile with no item of its own still gets the fleet env
    (.assertEqual self (profile-env CFG "/u/.hermes/profiles/p2" :run run)
                  {"KOTOBA_API_TOKEN" "fleet" "SHARED" "fleet"})
    ;; and nothing at all when the fleet item is not granted either
    (.assertEqual self (profile-env CFG "/u/.hermes/profiles/p2" :run (fake-kagi {})) {}))

  ;; @lat: [[profile-distribution#Fleet secrets#Tests#A kagi failure is an error, not an empty env]]
  (defn test-failure-raises [self]
    (defn broken [argv #** kw] (types.SimpleNamespace :returncode 3 :stdout "" :stderr "boom"))
    (with [(.assertRaises self RuntimeError)]
      (kagi-get CFG FLEET-ITEM "p" :run broken))))

(defclass Config [unittest.TestCase]
  ;; @lat: [[profile-distribution#Fleet secrets#Tests#Staging a profile adds the kagi helper and keeps other secret sources]]
  (defn test-secrets-command-block [self]
    (setv block (secrets-command-block "/py" "/gw")
          cfg (with-secrets-command {"model" {"provider" "murakumo"}
                                     "secrets" {"bitwarden" {"enabled" False}}}
                                    block))
    (.assertEqual self (get cfg "model") {"provider" "murakumo"})
    (.assertEqual self (get cfg "secrets" "bitwarden") {"enabled" False})
    (.assertTrue self (get cfg "secrets" "command" "enabled"))
    (.assertIn self "/py -m hy /gw/tools/kagi_env.hy" (get cfg "secrets" "command" "command"))
    (.assertEqual self (with-secrets-command None block) {"secrets" {"command" block}})))

(when (= __name__ "__main__")
  (unittest.main))

(defclass Leases [unittest.TestCase]
  (defn setUp [self] (setv self.tmp (tempfile.TemporaryDirectory)))
  (defn tearDown [self] (.cleanup self.tmp))

  ;; @lat: [[profile-distribution#Fleet secrets#Tests#A GitHub lease becomes GH_TOKEN and its expiry is remembered]]
  (defn test-lease-env [self]
    (setv calls []
          cfg {#** CFG "github_lease" {"from" "http://minter:8978/v1/lease" "access" "contents:read"
                                       "resources" ["cloud-itonami/*"]}})
    (defn run [argv #** kw]
      (.append calls argv)
      (if (= (get argv 2) "request")
          (types.SimpleNamespace :returncode 0 :stderr ""
                                 :stdout "{:status :issued, :lease {:lease/id \"lease-7\", :lease/expires-at 1791603600}}\n")
          (types.SimpleNamespace :returncode 0 :stderr "" :stdout "ghs_TOKEN")))
    (.assertEqual self (lease-env cfg "p1" :run run :state-dir self.tmp.name)
                  {"GH_TOKEN" "ghs_TOKEN" "GITHUB_TOKEN" "ghs_TOKEN"})
    (.assertEqual self (cut (get calls 0) 1 None)
                  ["lease" "request" "--from" "http://minter:8978/v1/lease" "--access" "contents:read"
                   "--resources" "cloud-itonami/*" "--purpose" "hermes-cron:p1"])
    (.assertEqual self (get calls 1) ["/k/bin/kagi" "lease" "open" "--lease" "lease-7"])
    ;; the expiry drives the cache drop: due within ten minutes of the end
    (.assertFalse self (lease-refresh-due "p1" :now (- 1791603600 3000) :state-dir self.tmp.name))
    (.assertTrue self (lease-refresh-due "p1" :now (- 1791603600 300) :state-dir self.tmp.name))
    (.assertFalse self (lease-refresh-due "never-leased" :now 0 :state-dir self.tmp.name)))

  ;; @lat: [[profile-distribution#Fleet secrets#Tests#A pending or denied lease does not stop the profile]]
  (defn test-lease-not-issued [self]
    (setv cfg {#** CFG "github_lease" {"from" "http://m" "resources" "cloud-itonami/*"}})
    (defn run [argv #** kw]
      (types.SimpleNamespace :returncode 0 :stderr "" :stdout "{:status :pending, :request-id \"req-1\"}"))
    (.assertEqual self (lease-env cfg "p1" :run run :state-dir self.tmp.name) {})
    (.assertEqual self (lease-env CFG "p1" :run run :state-dir self.tmp.name) {} "no github_lease: no lease")))
