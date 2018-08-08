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
    (->> conf
         (cf/db)            ;; gets the db connection
         (f)                ;; calls sql query on the db
         (to-hashmap k))))  ;; transforms to a hashmap 

(def timeslots (extract-data-fn all-timeslots :id))
(def rooms (extract-data-fn all-rooms :track))
(def keywords (extract-data-fn all-keywords :id))
(def users-by-username (extract-data-fn all-profiles :username))

(defn convert-usernames [f usermap conf]
  "Creates a function that extracts from the database with function f and converts usernames to id using hashmap usermap"
  (letfn [(username->id [x] 
            (->> x
                 (:user)  
                 (get usermap)
                 (:id)       
                 (assoc x :user)))]
    (->> conf
      (cf/db)              
      (f)                 
      (map username->id))))

(defn daytime->id [timeslots day time]
  (->> timeslots
       (filter #(and (= day (:day (second %)))
                     (= time (:time (second %)))))
                 (first)
                 (first)))

(defn streams [rawstreams rawsessions timeslots conf]
  "Returns a hashmap of streams by id, with an embedded list of sessions ordered by timeslot"
  (letfn [(addsessions [x]
            "Adds list of sessions"
            (let [s (->> rawsessions
                         (filter #(= (:cluster %) (:id x)))
                         (sort-by #(daytime->id timeslots (:day %) (:time %)))
                         (map :id))]
              (assoc x :sessions s)))]
    (->> rawstreams
      (map addsessions)
      (to-hashmap :id)))) 

(defn sessions [rawsessions rawpapers chairs timeslots conf]
  "Returns a hashmap of sessions by id, with an embedded list of papers"
  (letfn [(addpapers [x]
            "Adds list of abstracts"
            (let [p (->> rawpapers
                         (filter #(= (:code x) (:sessioncode %)))
                         (sort-by :order)
                         (map :id))]
              (assoc x :papers p)))
          (addtimeslot [x]
            "Adds timeslot id for day and time"
            (assoc x :timeslot (daytime->id timeslots (:day x) (:time x))))
          (addchairs [x]
            "Adds list of chairs"
            (let [c (->> chairs
                         (filter #(= (:session %) (:id x)))
                         (map :user))]
              (assoc x :chairs c)))]
    (->> rawsessions
      (map addpapers)
      (map addtimeslot)
      (map addchairs)
      (map #(assoc % :stream (:cluster %)))
      (map #(dissoc % :code :day :time :cluster)))))

(defn code->id [rawsessions code]
  (->> rawsessions
       (filter #(= (:code %) code))
       (first)
       (:id)))

(defn papers [rawpapers rawsessions authors conf]
  "Returns a hashmap of papers by id, with session codes converted to session ids and an embedded list of authors"
  (letfn [(addsession [x] 
            "Replaces session code by session ids"
            (assoc x :session (code->id rawsessions (:sessioncode x))))
          (addauthors [x]
            "Adds list of authors"
            (let [a (->> authors
                         (filter #(= (:paper %) (:id x)))
                         (sort-by :speaker)
                         (map :user))]
              (assoc x :authors a)))]
    (->> rawpapers 
      (map addsession)   
      (map addauthors)
      (map #(dissoc % :order :sessioncode))
      (to-hashmap :id))))

(defn paper-session [rawpapers rawsessions id]
  "Returns the sessionid for paper id"  
  (->> rawpapers
       (filter #(= (:id %) id))
       (first)
       (:sessioncode)
       (code->id rawsessions)))

(defn users [rawusers authors chairs rawpapers rawsessions timeslots]
  "Returns a hashmap of users by id, with a list of sessions there are involved in ordered by timeslots"
  (letfn [(addsessions [x]
            "Adds a list of sessions"
            (let [id (:id x)
                  as-chair (->> chairs
                                (filter #(= id (:user %)))
                                (map :session)
                                (set))
                  as-author (->> authors
                                 (filter #(= id (:user %)))
                                 (map :paper)
                                 (map #(paper-session rawpapers rawsessions %))
                                 (filter identity)
                                 (set))
                  session-set (s/union as-chair as-author)
                  s (->> rawsessions
                         (filter #(contains? session-set (:id %)))
                         (sort-by #(daytime->id timeslots (:day %) (:time %)))
                         (map :id))]
              (assoc x :sessions s)))]
    (->> rawusers
      (map addsessions)
      (filter #(not (empty? (:sessions %))))
      (map #(dissoc % :username)) 
      (to-hashmap :id))))

(defn to-set [x]
  (set (map :user x)))

(defn filterusers [u & s]
  "Keeps only users in u that are in key :user for sequences provided"
  (let [userset (reduce #(s/union %1 (to-set %2)) #{} s)]
    (filter #(contains? userset (:id %)) u)))

(defn program-data [conf]
  "Returns a hashmap with all the data of the program of conference conf"
  (let [ts (timeslots conf)
        rawpapers (all-papers (cf/db conf) conf)
        rawsessions (all-sessions (cf/db conf) conf)
        rawstreams (all-streams (cf/db conf) conf)
        umap (users-by-username cf/userdb)
        ca (convert-usernames all-coauthors umap conf)   
        ch (convert-usernames all-chairs umap conf)  
        allusers (all-profiles (cf/db cf/userdb) cf/userdb)
        rawusers (filterusers allusers ca ch)]
    {:timeslots ts, 
     :streams (streams rawstreams rawsessions ts conf),
     :sessions (sessions rawsessions rawpapers ch ts conf),
     :rooms (rooms conf),
     :papers (papers rawpapers rawsessions ca conf),
     :users (users rawusers ca ch rawpapers rawsessions ts)}))

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
    (defonce allusers (all-profiles (cf/db cf/userdb) cf/userdb))
    (defonce umap (users-by-username cf/userdb))
    (defonce ca (convert-usernames all-coauthors umap conf))
    (defonce ch (convert-usernames all-chairs umap conf))
    (defonce rawpapers (all-papers (cf/db conf) conf))
    (defonce rawsessions (all-sessions (cf/db conf) conf))
    (defonce rawstreams (all-streams (cf/db conf) conf))
    (defonce rawusers (filterusers allusers ca ch))
    (defonce ts (timeslots conf))))
