(defproject banktivity-stock-quote-sync "1.0-SNAPSHOT"
  :description "Banktivity Stock Quote Synchronization"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[ch.qos.logback/logback-classic "1.2.3"]
                 [http-kit "2.4.0-alpha6"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/core.match "1.0.0"]
                 [org.clojure/java.jdbc "0.7.11"]
                 [org.clojure/tools.logging "1.0.0"]
                 [org.xerial/sqlite-jdbc "3.30.1"]]
  :main ^:skip-aot banktivity.stockquotesync.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
