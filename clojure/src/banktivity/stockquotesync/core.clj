(ns banktivity.stockquotesync.core
  (:require [clojure.core.match :refer [match]])
  (:require [clojure.edn :as edn])
  (:require [clojure.java.jdbc :as jdbc])
  (:require [org.httpkit.client :as http])
  (:gen-class)
  (:import (java.time LocalDate ZoneId ZoneOffset)))

(defn db [dbname]
  {:dbtype "sqlite"
   :dbname dbname})

(defn positions [db]
  (jdbc/query db ["SELECT zuniqueid AS securityid, zsymbol AS symbol FROM zsecurity"]))

; Sample CSV output:
; Date,Open,High,Low,Close,Adj Close,Volume
; 2020-03-19,1093.050049,1094.000000,1060.107544,1078.910034,1078.910034,333575
(def yahoo-quote-csv-pattern
  #"Date,Open,High,Low,Close,Adj Close,Volume\n([0-9]{4}-[0-9]{2}-[0-9]{2}),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),([0-9]+\.[0-9]+),[0-9]+\.[0-9]+,([0-9]+)\n?.*")

(defn stock-quote [symbol]
  (let [resp (http/get (str "https://query1.finance.yahoo.com/v7/finance/download/" symbol "?interval=1d&events=history"))]
    (future
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

(defn persist-stock-quote [db position]
  (let [[updated] (jdbc/update! db :zprice
                                {:zvolume       (:volume position)
                                 :zclosingprice (:close position)
                                 :zhighprice    (:high position)
                                 :zlowprice     (:low position)
                                 :zopeningprice (:open position)}
                                ["z_ent = ? AND z_opt = ? AND zdate = ? AND zsecurityid = ?"
                                 42 1 (ibank-date (:date position)) (:securityid position)])]
    (if (zero? updated)
      (jdbc/insert! db :zprice
                    {:z_ent         42
                     :z_opt         1
                     :zdate         (ibank-date (:date position))
                     :zsecurityid   (:securityid position)
                     :zvolume       (:volume position)
                     :zclosingprice (:close position)
                     :zhighprice    (:high position)
                     :zlowprice     (:low position)
                     :zopeningprice (:open position)}))))

(defn -main [& args]
  (if-not (empty? args)
    (let [db-spec (db (str (first args) "/accountsData.ibank"))]
      (map (partial persist-stock-quote db-spec)
        (remove (comp nil? :open)
          (map #(merge % @(stock-quote (:symbol %))) ; Is the deref (@) here blocking too early?
            ;(comp (partial apply merge) (juxt identity (comp deref stock-quote :symbol)))
            (positions db-spec)))))

    (throw (IllegalArgumentException. "Please specify path to ibank data file."))))
