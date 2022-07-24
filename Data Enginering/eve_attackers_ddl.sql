use eve_online;

drop table if exists war_attackers;

create table war_attackers    (
alliance_id int,
character_id int,
corporation_id int,
damage_done float,
final_blow boolean,
security_status float,
ship_type_id int,
weapon_type_id int,
killmail_id int,
faction_id int
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/attackers.csv' INTO TABLE war_attackers;

--   3 bowenliao bowenliao  380384868 2022-05-08 21:55 Data/EVE/attackers.csv