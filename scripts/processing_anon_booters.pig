--------------------------------
-- META DESCRIPTION OF THE DATA 
--------------------------------
-- There are blank lines!!!
-- ~7 columns 

-- recordType variable can be the following: 
---- Q(Q) is the original query
---- Q(R) is the query echoed in the answer
---- R(ANS) is the answer
---- R(AUT) is authority
---- R(ADD) is additional

-- **Note: 1 Q(Q) => 1 Q(R) && one or many R(ANS)

-- Example:
-- 1434735481, Q(Q), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name]
-- 1434735481, Q(R), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter domain name], NOERROR
-- 1434735481, R(ANS), c861aaa8307395e94c0bc1d88e9846ff168071252198640801b108219b3899be, IN, A, [Booter IP Address]

-------------------
-- LOADING CLASSES
-------------------
-- https://cwiki.apache.org/confluence/display/PIG/PiggyBank
REGISTER /usr/lib/pig/piggybank.jar ;
DEFINE UnixToISO org.apache.pig.piggybank.evaluation.datetime.convert.UnixToISO(); 

-------------------
-- FEEDING THE PIG
-------------------
linesRaw = LOAD '../../dumps/2015*.gz' USING PigStorage(',') as (timestamp:long, recordType, srcIpAnon, alwaysIn , answerType, booterInformation, error);
/*linesRaw = LOAD '../dumps/anon_booters.txt' USING PigStorage(',') as (timestamp:long, recordType, srcIpAnon, alwaysIn , answerType, booterInformation, error);*/
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
-- DUMP lines; 
-- EXPLAIN lines;

-------------------
-- 0. Deduplicate entries
-------------------

linesGroup = GROUP lines ALL; -- It groups all the lines
lines = FOREACH linesGroup {
	b = lines.(timestamp, recordType, srcIpAnon, alwaysIn, answerType, booterInformation, error);
	s = DISTINCT b;
	GENERATE FLATTEN(s);
};
-- DUMP uniqLines;


-------------------
-- 1. (DONE) How many lines the data has?
-------------------
linesGroup = group lines ALL; -- It groups all the lines
numLines = FOREACH linesGroup GENERATE COUNT(lines); -- It counts the total number of lines in the group of lines
-- DUMP numLines; 

-------------------
-- 2. How many records is related to each recordType?
-------------------
groupRecordType = group lines by recordType;
numReqPerRecordType = FOREACH groupRecordType GENERATE group as recordType, COUNT(lines);
-- DUMP numReqPerRecordType;

-------------------
-- 3. (DONE) How many requests (QQrecords) the database has?
-------------------
QQrecords = FILTER lines BY recordType == 'Q(Q)';
groupQQrecords = GROUP QQrecords ALL;
numQQrecords = FOREACH groupQQrecords GENERATE COUNT(QQrecords);
-- DUMP numQQrecords;

-------------------
-- 4. (DONE) Which are the Booters requested && How many times each Booter was requested?
-------------------
groupQQbyBooter = GROUP QQrecords by booterInformation;
numQQperBooter = FOREACH groupQQbyBooter GENERATE group as groupQQbyBooter, COUNT(QQrecords) as c;
sortedQQperBooter = ORDER numQQperBooter BY c DESC;
-- DUMP sortedQQperBooter;

-------------------
-- 5. (DONE) Who are the srcIpAnon that request for a Booter AND How many request each srcIpAnon made?
-------------------
groupIps = group QQrecords by srcIpAnon;
QQPerIp = FOREACH groupIps GENERATE group as groupIps, COUNT(QQrecords) as c;
sortedQQperIP = ORDER QQPerIp by c DESC;
-- DUMP sortedQQperIP;

-------------------
-- 6. (DONE) Converting timestamp in readable time
-------------------
readableTime = FOREACH lines GENERATE UnixToISO(timestamp * 1000);
-- DUMP readableTime;

-------------------
-- 7. Time-series of the total number of requests X per day [time bin]
-------------------

%declare oneDay 86400

bin_line = FOREACH lines GENERATE (timestamp/$oneDay) as bin_id, recordType;
group_bin = GROUP bin_line by (bin_id, recordType);
timeseries = FOREACH group_bin GENERATE FLATTEN(group), COUNT(bin_line);
STORE timeseries INTO '../output/timeseries-recordType' USING org.apache.pig.piggybank.storage.CSVExcelStorage();

-------------------
-- 8. Time-series of the number of requests per user X day [time bin]
-------------------
