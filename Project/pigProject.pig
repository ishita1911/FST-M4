--Load the text file
dialogfile = LOAD 'hdfs:///user/root/M4Project' USING PigStorage('\t') AS (names:chararray,lines:chararray);
--GROUP file using name
GroupByName = GROUP dialogfile BY names;
cntd = FOREACH GroupByName GENERATE $0, COUNT(dialogfile.$1) AS numlines;
resultdata = ORDER cntd BY numlines DESC;
rmf hdfs:///user/root/projectResults
STORE resultdata INTO 'hdfs:///user/root/projectResults';
