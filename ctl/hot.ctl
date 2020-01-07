-- **************************************************************
-- This SQL*Loader statement loads the data from hot.asc 'file'
-- into the L_HOT table.
--
-- Change History
-- ==============
-- Date         Version Author          Change
-- 22.10.2019   1.0     SMARTIN        Original Version
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
INTO TABLE L_HOT
--FIELDS TERMINATED BY "|" OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(
recid       POSITION(1:3)     CHAR NULLIF recid=BLANKS,
sequence_no POSITION(4:11)    CHAR NULLIF sequence_no=BLANKS,
rec_type    POSITION(12:13)   CHAR NULLIF rec_type=BLANKS,
data_text   POSITION(14:2000) CHAR NULLIF data_text=BLANKS, 
date_row_added  SYSDATE
)

