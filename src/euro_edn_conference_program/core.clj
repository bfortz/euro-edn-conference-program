(ns euro-edn-conference-program.core 
  (:require [hugsql.core :as hugsql] 
            [clojure.set :as s]
            [euro-edn-conference-program.config :as cf])
  (:gen-class))

(hugsql/def-db-fns "euro_edn_conference_program/queries.sql")

(defn to-hashmap [k x]
  "Transforms a list of hashmaps in a hashmap of hasmaps using key k"
  (reduce #(assoc %1 (k %2) (dissoc %2 k)) (sorted-map) x))

(defn extract-data-fn [f k]
  "Creates a function that extracts from the database with function f and makes a hashmap based on key k"
  (fn [conf]
    (->> (f (cf/db conf))
         (to-hashmap k))))

(def timeslots (extract-data-fn all-timeslots :id))
(def rooms (extract-data-fn all-rooms :track))
(def keywords (extract-data-fn all-keywords :id))
(def users-by-username (extract-data-fn all-profiles :username))

;; Helper functions

(defn convert-usernames [f usermap conf]
  "Creates a function that extracts from the database with function f and converts usernames to id using hashmap usermap"
  (letfn [(username->id [x] 
            (->> (:user x)
                 (get usermap)
                 (:id)
                 (assoc x :user)))]
    (->> (f (cf/db conf))
      (map username->id))))

(defn sort-map-of-seqs-by [f m]
  "Sorts the seqs in a map m of seqs using function f"
  (reduce #(assoc %1 (first %2) (sort-by f (second %2))) (hash-map) m))

(defn inner-map [f m]
  "Apply f to all seqs in a map m of seqs"
  (reduce #(assoc %1 (first %2) (map f (second %2))) (hash-map) m))

(defn seq-to-map [f s]
  "Returns a map with keys as the different values of applying f to the
  elements of s, and values the list of elements x in s for which (f x) equals
  the key"
  (reduce #(update %1 (f %2) conj %2) (hash-map) s)) 

(defn make-map [s f sort-fn map-fn]
  "Returns a map with keys as the different values of applying f to the
  elements of s, and values the list of (map-fn x) for x in s, ordered by
  (sort-fn x) for which (f x) equals the key"
  (->> (seq-to-map f s)
       (sort-map-of-seqs-by sort-fn)
       (inner-map map-fn)))

(defn paper->sessionid [sessionmap p]
  "Returns session id based on session code"
  (:id (get sessionmap (:sessioncode p)))) 

;; Maps creation functions

(defn timeslot-map [timeslots]
  "Converts a timeslot hash-indexed by id to a hashmap of ids indexed by day and time"
  (reduce #(assoc-in %1 [(:day (second %2)) (:time (second %2))] (first %2)) (hash-map) timeslots))

(defn stream-sessions-map [rawsessions timeslots]
  "Creates a map of sessions associated to streams sorted by timeslot id"
  (make-map rawsessions :cluster #(get-in timeslots (:day %) (:time %)) :id))

(defn session-papers-map [rawpapers sessionmap]
  "Creates a map of papers associated to sessions"
  (make-map rawpapers (partial paper->sessionid sessionmap) :order :id))

(defn session-chairs-map [chairs]
  "Creates a map of chairs associated to sessions"
  (make-map chairs :session :user :user))

(defn paper-authors-map [authors]
  "Creates a map of authors of papers"
  (make-map authors :paper :speaker :user))

(defn user-chairs-map [chairs]
  "Creates a map of sessions chaired by users"
  (make-map chairs :user :session :session))

(defn user-papers-map [authors]
  "Creates a map of papers by users"
  (make-map authors :user :paper :paper))


;; Data creation functions

(defn streams [rawstreams stream-sessions]
  "Returns a hashmap of streams by id, with an embedded list of sessions ordered by timeslot"
  (->> rawstreams
       (map #(assoc % :sessions (get stream-sessions (:id %))))
       (to-hashmap :id))) 

(defn sessions [rawsessions session-papers session-chairs timeslots]
  "Returns a hashmap of sessions by id, with an embedded list of papers"
  (->> rawsessions
       (map #(assoc % :papers (get session-papers (:id %))))
       (map #(assoc % :timeslot (get-in timeslots [(:day %) (:time %)])))
       (map #(assoc % :chairs (get session-chairs (:id %))))
       (map #(assoc % :stream (:cluster %)))
       (map #(dissoc % :code :day :time :cluster))
       (to-hashmap :id)))

(defn papers [rawpapers sessionmap paper-authors]
  "Returns a hashmap of papers by id, with session codes converted to session ids and an embedded list of authors"
  (->> rawpapers 
       (map #(assoc % :session (paper->sessionid sessionmap %)))   
       (map #(assoc % :authors (get paper-authors (:id %))))
       (map #(dissoc % :order :sessioncode))
       (to-hashmap :id)))

(defn users [allusers user-papers user-chairs papers sessions]
  "Returns a hashmap of users by id, with a list of sessions there are involved in ordered by timeslots"
  (letfn [(addsessions [x]
            "Adds a list of sessions"
            (let [as-chair (set (get user-chairs (:id x)))
                  as-author (->> (get user-papers (:id x))
                                 (map #(:session (get papers %)))
                                 (filter identity)
                                 (set))
                  s (->> (s/union as-chair as-author)
                         (reduce #(conj %1 (assoc (get sessions %2) :id %2)) ())
                         (sort-by :timeslot)
                         (map :id))]
              (assoc x :sessions s)))]
    (->> allusers
      (map addsessions)
      (filter #(not (empty? (:sessions %))))
      (map #(dissoc % :username)) 
      (to-hashmap :id))))

(defn program-data [conf]
  "Returns a hashmap with all the data of the program of conference conf"
  (let [ts (timeslots conf)
        tsmap (timeslot-map ts)
        rawpapers (all-papers (cf/db conf) conf)
        rawsessions (all-sessions (cf/db conf) conf)
        sessionmap (to-hashmap :code rawsessions)
        rawstreams (all-streams (cf/db conf) conf)
        umap (users-by-username cf/userdb)
        stream-sessions (stream-sessions-map rawsessions tsmap)
        session-papers (session-papers-map rawpapers sessionmap)
        ca (convert-usernames all-coauthors umap conf)   
        paper-authors (paper-authors-map ca)
        user-papers (user-papers-map ca)
        ch (convert-usernames all-chairs umap conf)  
        session-chairs (session-chairs-map ch)
        user-chairs (user-chairs-map ch)
        allusers (all-profiles (cf/db cf/userdb) cf/userdb)
        p (papers rawpapers sessionmap paper-authors)
        s (sessions rawsessions session-papers session-chairs tsmap)]
    {:timeslots ts, 
     :streams (streams rawstreams stream-sessions),
     :sessions s, 
     :rooms (rooms conf),
     :papers p,
     :users (users allusers user-papers user-chairs p s)}))

(defn changed? [data filename]
  (try
    (let [new-hash (hash data)
          old-hash (hash (read-string (slurp filename)))]
      (not= new-hash old-hash))
    (catch Exception _ true))) 

(defn -main
  "Generates an edn program file format for a database passed as first argument"
  [& args]
  (let [conf (first args)
        filename (str cf/output-path "/" conf ".edn")
        newdata (program-data conf)]
    (when (changed? newdata filename) 
      (spit filename newdata))))

(comment 
  "useful for repl testing" 
  (do
    (set! *print-length* 10)
    (defonce conf "or2018")
    (defonce ts (timeslots conf))
    (defonce tsmap (timeslot-map ts))  
    (defonce allusers (all-profiles (cf/db cf/userdb) cf/userdb))
    (defonce umap (users-by-username cf/userdb))
    (defonce ca (convert-usernames all-coauthors umap conf))
    (defonce ch (convert-usernames all-chairs umap conf))
    (defonce rawpapers (all-papers (cf/db conf) conf))
    (defonce rawsessions (all-sessions (cf/db conf) conf))
    (defonce rawstreams (all-streams (cf/db conf) conf))
    (defonce sessionmap (to-hashmap :code rawsessions))
    (defonce stream-sessions (stream-sessions-map rawsessions tsmap))
    (defonce session-papers (session-papers-map rawpapers sessionmap))
    (defonce paper-authors (paper-authors-map ca))
    (defonce user-papers (user-papers-map ca))
    (defonce session-chairs (session-chairs-map ch))
    (defonce user-chairs (user-chairs-map ch))
    (defonce p (papers rawpapers sessionmap paper-authors))
    (defonce s (sessions rawsessions session-papers session-chairs tsmap))))
