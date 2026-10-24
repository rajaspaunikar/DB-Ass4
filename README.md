Newsroom Geo-Coverage Analyzer (PySpark)
========================================

Group members
-------------
- Rajas Paunikar (25M2126)
- Aditya Kumar Verma (25M2131)

Files
-----
- assignment4.py : the complete program (all tasks, single file)
- README.md      : this file

Requirements
------------
- Python 3 (tested with Python <3.14.4>)
- PySpark (tested with PySpark; `pip install pyspark`)
- Java 17 (OpenJDK), with JAVA_HOME set or `java` on the PATH installed with `sudo apt install openjdk-17-jdk-headless`

How to run
----------
1. Unzip newsdata.zip into a directory, e.g. ~/newsdata
   (the directory should contain the article .json files).
2. Run:

       spark-submit assignment4.py <path to the unzipped data folder> > output.txt

   Example:

       spark-submit assignment4.py ~/newsdata > output.txt

The data path is read from sys.argv[1]; no path is hard-coded.
All results are written to standard output (redirected to output.txt above)
in the BEGIN_<TAG> ... END_<TAG> block format given in the assignment.
Spark's own log messages go to standard error and do not appear in output.txt.

Approach
--------
- Input: each JSON file is read with sc.wholeTextFiles and parsed with
  json.loads, keeping only article_id and article_body.
- Task 1 (RDD API): each article is tokenized with
  re.findall(r'[a-z]+', text.lower()); the distinct places in it produce
  (place, article_id) pairs, deduplicated with distinct(). The total count and
  the first 20 rows (sorted by place, then article_id) are printed.
- Task 2: the Task 1 RDD is converted to a DataFrame (place, article_id) with
  an explicit string schema.
- Task 3: articles are tokenized with DataFrame functions
  (F.split(F.lower(col), '[^a-z]+') + F.explode), filtered to the place list,
  and grouped by (article_id, place) to count every occurrence
  (term_frequency). The dominant place(s) of each article are found with a
  window (max term_frequency per article_id); ties are all kept.
- Task 4: places whose article count (from the distinct pairs) is exactly 1,
  joined back to the pairs to get the article_id.
- Task 5: a self-join of the pairs on article_id with p1 < p2 gives each
  co-occurring pair once in alphabetical order; Jaccard =
  cooccurrence_count / (|A| + |B| - cooccurrence_count), computed as a double.
- Task 6: total_mentions = sum of term_frequency per place;
  article_count = number of articles in which the place occurs.
- Task 7: number of distinct places per article from the pairs.

All grouping, counting, joining and ranking in Tasks 2 to 7 use DataFrame
functions. No pandas and no SQL strings (spark.sql, selectExpr, F.expr,
temporary views) are used.

Assumptions
-----------
1. article_id is taken from the JSON field, not the file name, and is kept as
   a string exactly as given (never cast), so all sorting on it is
   lexicographic. This includes the one non-numeric id in the dataset,
   "ISRO, Sivan, GSAT-11,", which is treated as a normal article id.
2. A missing or null article_body is treated as empty text. (In the provided
   dataset all 495 articles have a non-empty body.)
3. Only files ending in .json are read; files that are not valid JSON objects
   or have no article_id are skipped. Subdirectories of the data folder are
   read recursively. (In the provided dataset all 495 files parse and all
   article_ids are unique.)
4. Tokenization follows the assignment exactly. Task 1 uses re.findall, and
   Tasks 3+ use the equivalent F.split with empty strings ignored. Matching
   is on whole lower-case words only ("Delhi's" counts as delhi; "Bengaluru"
   does not count as bengal).
5. Term frequencies (Tasks 3 and 6) count every occurrence in the text and
   are not computed from the deduplicated Task 1 pairs.
6. Articles that mention no place from the list do not appear in the Task 3
   or Task 7 outputs.
7. Task 5 considers only pairs of different places that co-occur in at least
   one article; Jaccard is not rounded (the helper prints it to 4 decimals).
8. Task 4 lists only places occurring in exactly one article; places that
   occur in no article are not reported (not required by the assignment).
9. The Spark log level is set to ERROR and the console progress bar is
   disabled, so standard output contains only the required result blocks.