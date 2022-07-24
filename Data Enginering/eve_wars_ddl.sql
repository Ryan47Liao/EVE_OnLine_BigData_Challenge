use eve_online;

drop table if exists wars;

create table wars    (
killmail_id string,
killmail_time string,
solar_system_id string,
war_id string,
killmail_hash string,
http_last_modified string,
victim.character_id string,
victim.corporation_id string,
victim.damage_taken float,
victim.items string,
victim.position.x float,
victim.position.y float,
victim.position.z float,
victim.ship_type_id string,
victim.alliance_id string,
victim.character_id string
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/WARS.csv' INTO TABLE wars;

--   3 bowenliao bowenliao   26140870 2022-05-08 21:55 Data/EVE/ENCOUNTER.csv
--   3 bowenliao bowenliao  512762810 2022-05-08 21:55 Data/EVE/ITEMS_Agg_All.csv
--   3 bowenliao bowenliao  380384868 2022-05-08 21:55 Data/EVE/attackers.csv
