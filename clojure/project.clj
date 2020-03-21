(defproject quote-sync-clj "1.0-SNAPSHOT"
  :description "Banktivity Stock Quote Synchronization"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[http-kit "2.3.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.xerial/sqlite-jdbc "3.30.1"]]
  :main ^:skip-aot banktivity.stockquotesync.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
