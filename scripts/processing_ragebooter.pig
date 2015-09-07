--LOADING PART
lines = LOAD 'dump/ragebooter_fe.csv' USING PigStorage(',') AS (id, userid, type, ip, note);

LB = GROUP lines BY userid;
LC = FOREACH LB GENERATE COUNT ($0);
DUMP LC;

--PROCESSING PART
-- DUMP lines;
-- DUMP COUNT(B)
--OUTPUT PART
--STORE uniq_ INTO '/tmp/tutorial-results' USING PigStorage(); 
--RUNNING
--pig -x local processing_ragebooter.pig