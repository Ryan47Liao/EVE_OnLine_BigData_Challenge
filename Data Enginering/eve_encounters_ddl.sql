use eve_online;

drop table if exists encounters;

create table encounters    (
declared string,
finished string,
id int,
mutual boolean,
open_for_allies boolean,
retracted string,
started string,
war_id int,
http_last_modified string,
aggressor.corporation_id string,
aggressor.isk_destroyed string,
aggressor.ships_killed string,
defender.corporation_id string,
defender.isk_destroyed string,
defender.ships_killed string,
defender.alliance_id string,
aggressor.alliance_id string
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/ENCOUNTER.csv' INTO TABLE encounters;

--   3 bowenliao bowenliao  512762810 2022-05-08 21:55 Data/EVE/ITEMS_Agg_All.csv
--   3 bowenliao bowenliao  380384868 2022-05-08 21:55 Data/EVE/attackers.csv