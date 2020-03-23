(ns banktivity.stockquotesync.core
  (:require [clojure.core.match :refer [match]])
  (:require [clojure.edn :as edn])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [clojure.tools.logging :as log])
  (:require [org.httpkit.client :as http])
  (:gen-class)
  (:import (java.time LocalDate ZoneId ZoneOffset)))

(defn db [db-path]
  (log/info (str "Processing SQLite file " db-path "..."))
  {:dbtype "sqlite"
   :dbname db-path})

(defn securities [db]
  (let [results (jdbc/query db ["SELECT zuniqueid AS securityid, zsymbol AS symbol FROM zsecurity"])]
    (log/info "Found" (count results) "securities...")
    results))

; Sample CSV output:
; Date,Open,High,Low,Close,Adj Close,Volume
; 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
(def yahoo-quote-csv-pattern
  #"Date,Open,High,Low,Close,Adj Close,Volume\n([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)\n?.*")

(defn price [symbol]
  (let [resp (http/get (str "https://query1.finance.yahoo.com/v7/finance/download/" symbol "?interval=1d&events=history"))]
    (future
      (log/info "Downloading prices for " symbol "...")
      (match @resp
             {:status 200 :body body}
             (match (re-matches yahoo-quote-csv-pattern body)
                    ([_ date open high low close volume] :seq)
                    {:date   (LocalDate/parse date)
                     :open   (edn/read-string open)
                     :high   (edn/read-string high)
                     :low    (edn/read-string low)
                     :close  (edn/read-string close)
                     :volume (edn/read-string volume)}
                    :else (throw (IllegalStateException. (str "Unparseable response:\n" body))))
             {:status 404} nil
             {:status bad-status-code} (throw (IllegalStateException. (str "Received HTTP " bad-status-code " for symbol \"" symbol "\"")))))))

(defn ibank-date [date]
  (.toEpochSecond
    (.atStartOfDay
      (.plusDays (.minusYears date 31) 3)
      (ZoneId/ofOffset "" (ZoneOffset/ofHours 3)))))

(defn persist-price [db position]
  (let [updated (jdbc/update! db :zprice
                              {:zvolume       (:volume position)
                               :zclosingprice (:close position)
                               :zhighprice    (:high position)
                               :zlowprice     (:low position)
                               :zopeningprice (:open position)}
                              ["z_ent = ? AND z_opt = ? AND zdate = ? AND zsecurityid = ?"
                               42 1 (ibank-date (:date position)) (:securityid position)])]
    (first
      (if (zero? (first updated))
        ((constantly 1)
         (jdbc/insert! db :zprice
                       {:z_ent         42
                        :z_opt         1
                        :zdate         (ibank-date (:date position))
                        :zsecurityid   (:securityid position)
                        :zvolume       (:volume position)
                        :zclosingprice (:close position)
                        :zhighprice    (:high position)
                        :zlowprice     (:low position)
                        :zopeningprice (:open position)}))
        updated))))

(defn -main [& args]
  (if-not (empty? args)
    (let [db-path (str (first args) "/accountsData.ibank")
          db-spec (db db-path)
          updates (map #(persist-price db-spec %)
                       (remove (comp nil? :open)
                               (map #(merge % @(price (:symbol %))) ; Is the deref (@) here blocking too early?
                                    ;(comp (partial apply merge) (juxt identity (comp deref stock-quote :symbol)))
                                    (securities db-spec))))]
      (log/info "Persisted prices for" (reduce + updates) "securities")
      (log/info "Security prices synchronized successfully."))

    (.println *err* "Please specify path to ibank data file.")))
