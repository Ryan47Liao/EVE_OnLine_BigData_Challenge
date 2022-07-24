use eve_online;

drop table if exists invTypes;

create table invTypes    (
typeID	int,
groupID	int,
typeName string,
description	string,
mass	float,
volume	float,
capacity	float,
portionSize	int,
raceID	int,
basePrice	float,
published	int,
marketGroupID	int,
iconID	int,
soundID	int,
graphicID int

)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/invTypes.csv' INTO TABLE mapRegions;