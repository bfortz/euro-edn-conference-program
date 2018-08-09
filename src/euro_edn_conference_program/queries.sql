-- :name all-timeslots :? :*
-- :doc Get all timeslots
select id, schedule, day, time from timeslots

-- :name all-streams :? :*
-- :doc Get all streams
select id, name, `order` from clusters

-- :name all-sessions :? :*
-- :doc Get all sessions
select id, code, name, day, time, track, cluster, specialroom from sessionchairs

-- :name all-rooms :? :*
-- :doc Get all rooms 
select track, room from rooms

-- :name all-keywords :? :*
-- :doc Get all keywords
select * from keywords

-- :name all-papers :? :*
-- :doc Get all accepted papers
select id, sessioncode, title, abstract, keyword1, keyword2, keyword3, `order` from paper where completed=1 and accepted=1 and cancelled=0

-- :name all-coauthors :? :*
-- :doc Get all coauthors of papers
select * from paper_person

-- :name all-chairs :? :*
-- :doc Get all session chairs
select * from cochairs

-- :name all-profiles :? :*
-- :doc Get all users profiles
select users.id, users.username, users_info.firstname, users_info.lastname from users left join users_info on users.id = users_info.id
