(ns build
  (:require [clojure.tools.build.api :as b]))

(def lib 'dtlv-audit/dtlv-audit)
(def version "0.1.0-SNAPSHOT")
(def class-dir "target/classes")
(def jar-file (format "target/%s-%s.jar" (name lib) version))

(defn- basis []
  (b/create-basis {:project "deps.edn"}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (b/write-pom {:class-dir class-dir
                :lib lib
                :version version
                :basis (basis)
                :src-dirs ["src"]})
  (b/copy-dir {:src-dirs ["src" "resources"]
               :target-dir class-dir})
  (b/jar {:class-dir class-dir
          :jar-file jar-file}))

(defn install [_]
  (jar nil)
  (b/install {:basis (basis)
              :lib lib
              :version version
              :jar-file jar-file
              :class-dir class-dir}))
