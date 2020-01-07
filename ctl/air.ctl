-- **************************************************************
-- This SQL*Loader statement loads the data from hot.asc 'file'
-- into the L_HOT table.
--
-- Change History
-- ==============
-- Date         Version Author          Change
-- 22.10.2019   1.0     SMARTIN        Original Version
-- 11.11.2019   1.1     TWILSON        Added sequence_no column
--
--
-- **************************************************************
--
OPTIONS (ERRORS=99999,SILENT=(HEADER, FEEDBACK))
--
LOAD
CHARACTERSET WE8ISO8859P15
--INFILE '/data/reports/bookingprices.asc'
TRUNCATE
INTO TABLE L_AIR
--FIELDS TERMINATED BY "|" OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
sequence_no SEQUENCE(1, 1),
data_text   POSITION(1:2000) CHAR NULLIF data_text=BLANKS, 
date_row_added  SYSDATE
)

