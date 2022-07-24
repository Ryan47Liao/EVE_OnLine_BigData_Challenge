use eve_online;

drop table if exists war_items;

create table war_items    (
index int,
item_type_id int,
quantity_destroyed int,
singleton int,
killmail_id int,
victim.character_id int,
quantity_dropped int,
flag string
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/ITEMS_Agg_All.csv' INTO TABLE war_items;

--   3 bowenliao bowenliao  380384868 2022-05-08 21:55 Data/EVE/attackers.csv