(ns banktivity.stockquotesync.core
  (:require [clojure.java.jdbc :as jdbc])
  (:require [org.httpkit.client :as http])
  (:gen-class))

(defn db
  [dbname]
  {:dbtype "sqlite"
   :dbname dbname})

(defn positions
  [db]
  (jdbc/query db ["SELECT zuniqueid AS securityid, zsymbol AS symbol FROM zsecurity"]))

(defn quote
  [symbol]
  (http/get str("https://query1.finance.yahoo.com/v7/finance/download/" symbol "?interval=1d&events=history")))

(defn -main
  [& args]
  (if-not (empty? args)
    (println (positions (db (str (first args) "/accountsData.ibank"))))

    (throw (Exception. "Please specify path to ibank data file."))))
