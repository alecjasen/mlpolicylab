CREATE SCHEMA IF NOT EXISTS acs_schema;

CREATE TABLE IF NOT EXISTS acs_schema.alec (
	state VARCHAR NOT NULL, 
	county VARCHAR NOT NULL, 
	tract VARCHAR NOT NULL, 
	block_group DECIMAL NOT NULL, 
	"Male <18" DECIMAL NOT NULL, 
	"Female <18" DECIMAL NOT NULL, 
	"Male 18-29" DECIMAL NOT NULL, 
	"Female 18-29" DECIMAL NOT NULL, 
	"Male 30-44" DECIMAL NOT NULL, 
	"Female 30-44" DECIMAL NOT NULL, 
	"Male 45-64" DECIMAL NOT NULL, 
	"Female 45-64" DECIMAL NOT NULL, 
	"Male >=65" DECIMAL NOT NULL, 
	"Female >=65" DECIMAL NOT NULL
);

DELETE FROM acs_schema.alec;

\COPY acs_schema.alec from '~/data/age_sex_blockgrouplevel.csv' with CSV HEADER;
