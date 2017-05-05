# TREC Terabyte Track

This directory contains topics from the (2005)[http://trec.nist.gov/data/terabyte05.html]
and (2006)[http://trec.nist.gov/data/terabyte06.html] TREC Terabyte Tracks. These files have been
edited as follows:

* Remove the topic number and colon from the beginning of each line.
* Replace the following punctuation with spaces: `-;:,&'+.`
* Coalesce multiple spaces into a single space.
* Remove leading and trailing spaces.

These changes make the queries legal for mgj4 and BitFunnel.
