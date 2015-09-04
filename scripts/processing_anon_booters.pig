--------------------------------
-- META DESCRIPTION OF THE DATA 
--------------------------------
-- There are blank lines!!!
-- 7 columns 

-- recordType variable can be the following: 
---- Q(Q) is the original query
---- Q(R) is the query echoed in the answer
---- R(ANS) is the answer
---- R(AUT) is authority
---- R(ADD) is additional

--Note that 1 Q(Q) => 1 Q(R), many R(ANS)

-- Example:
-- 1434735481, Q(Q), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name]
-- 1434735481, Q(R), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name], NOERROR
-- 1434735481, R(ANS), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter IP Address]

-------------------
-- Registrering classes and functions
-------------------
-- https://cwiki.apache.org/confluence/display/PIG/PiggyBank
REGISTER /usr/lib/pig/piggybank.jar ;
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(); 

-------------------
-- FEEDING THE PIG
-------------------
linesRaw = LOAD '../../dumps/anon_booters.txt' USING PigStorage(',') as (timestamp: int, recordType, srcIpAnon, alwaysIn , answerType, booterInformation, error);
lines = FILTER linesRaw BY timestamp is not null;
lines = FOREACH lines GENERATE timestamp as timestamp, 
							   -- remove prepended space from all values
                               REPLACE(recordType, ' ', '') as recordType,
                               REPLACE(srcIpAnon, ' ', '') as srcIpAnon,
                               REPLACE(alwaysIn, ' ', '') as alwaysIn, 
                               REPLACE(answerType, ' ', '') as answerType,
							   -- normalize domain: lowercase and and remove www
                               REPLACE(LOWER(REPLACE(booterInformation, ' ', '')), 'www\\.', '') as booterInformation,
                               REPLACE(error, ' ', '') as error;
--DUMP lines; 
-- EXPLAIN lines;

-------------------
-- 1. Counting the number of lines
-------------------
-- This does not count the empty lines... Why?!
linesGroup = group lines ALL; -- It groups all the lines
numLines = FOREACH linesGroup GENERATE COUNT(lines); -- It counts the total number of lines in the group of lines
--DUMP numLines; 
-------------------
-- 2. How many Anon-IPs are in the database?
-------------------

-------------------
-- 3. (DONE) Which are the unique Anon-IPs? 
-------------------
srcIps = FOREACH lines GENERATE srcIpAnon;
uniqIps = DISTINCT srcIps;
--DUMP uniqIps;

-------------------
-- 4. How many entries are related (requests and answers) to each Anon-IP?
-------------------
grouplines = GROUP lines by srcIpAnon;
ipRequests = FOREACH grouplines GENERATE group as srcIpAnon, COUNT(lines);
--DUMP ipRequests;

groupSrcIpAnon = GROUP grouplines ALL;
countgroupSrcIpAnon = FOREACH groupSrcIpAnon GENERATE COUNT(grouplines);
--DUMP countgroupSrcIpAnon;

-------------------
-- 5. showing the uniq types of requests
-------------------
groupQueries = group lines by recordType;
uniqGroupQueries = FOREACH groupQueries GENERATE COUNT(lines), group as recordType;
--DUMP uniqGroupQueries;

-------------------
-- 6. converting timestamp in readable time
-------------------
readableTime = FOREACH lines GENERATE UnixToISO(timestamp * 1000);
--DUMP readableTime;



-------------------
-- 1) Which is the most common Booter in this database?
-------------------
QQrecords = FILTER lines BY recordType == 'Q(Q)';
--DUMP QQrecords;
groupBooter = GROUP QQrecords BY booterInformation;
counter = FOREACH groupBooter GENERATE group as booterInformation, COUNT(QQrecords) as c;
sortedCounter = ORDER counter BY c DESC;
--dump sortedCounter;


add = FILTER lines BY recordType == 'R(ADD)';
--dump add;

---TODO: this does not work!
--nullError = FILTER lines BY error is not null;
--dump nullError;

-------------------
-- 2) Time-series of the Number of requests X time bin [day]
-- based on: http://stackoverflow.com/questions/17258153/pig-group-by-ranges-binning-data
-- however with an UDF it will be more generic:
-- http://stackoverflow.com/questions/18004054/pig-0-11-1-count-groups-in-a-time-range
-------------------
%declare MIN 1434735481
%declare MAX 1439768573
%declare BIN_COUNT 100

--BINSIZE  ($MAX- $MIN + 1) / $BIN_COUNT

bin_line = foreach lines generate (timestamp - $MIN) * $BIN_COUNT / ($MAX- $MIN + 1) as bin_id, recordType, srcIpAnon;
group_by_bin = group bin_line by (bin_id, recordType);
timeseries_by_type = foreach group_by_bin generate (group.bin_id* ($MAX- $MIN + 1) / $BIN_COUNT) + $MIN, group.recordType, COUNT(bin_line.srcIpAnon);
--output should be: bin_start, recordType, number of records
dump timeseries_by_type;

-------------------
-- 3) Time-series of the Number of requests per booter x time bin
-------------------

--
