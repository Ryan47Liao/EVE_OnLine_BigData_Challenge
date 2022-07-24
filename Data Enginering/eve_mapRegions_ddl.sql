use eve_online;

drop table if exists mapRegions;

create table mapRegions    (
regionID int,
regionName string,
x float,	
y float,	
z float,	
xMin float,
xMax float,	
yMin float,
yMax float,	
zMin float,
zMax float,
factionID int,
nebulaID int,	
radius int 

)

ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
tblproperties("skip.header.line.count"="1"); 

-- Load CSV
LOAD DATA INPATH '/user/bowenliao/Data/EVE/mapRegions.csv' INTO TABLE mapRegions;
