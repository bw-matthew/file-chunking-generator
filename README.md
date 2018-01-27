File Chunking Generator
=======================

This will take a file like object and return a generator of file like objects.
Each of those file like objects returned will return one chunk of data when read.

A chunk is made of lines of text.
A chunk will return as many lines as possible without exceeding a size limit.
