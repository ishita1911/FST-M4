---Load the input file from HDFS
inputFile = LOAD 'hdfs:///user/ishita/pigInput.txt' AS (line);

--Tokenize each word( MAP)
words = FOREACH inputFile GENERATE FLATTEN(TOKENIZE(line)) AS word;


--Group words
grpd= GROUP words BY word;

--Count the words (Reduce)
totalcount= FOREACH grpd GENERATE group, COUNT(words);

-- Store the result in HDFS
STORE totalcount INTO 'hdfs:///user/ishita/pigOutput';	

