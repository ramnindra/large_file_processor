# large_file_processor

Problem:

Assume you are given a very large file (> 10G) that contains lines of text.  You need to write a program that reads data from the input file and creates two output files:
a.	File 1: each line should print line number with total word count for that line
b.	File 2: unique words (case insensitive) across input file, along with its count

For example:
First few lines of input file:
	This is a sample
	Hacker
	A this hacker

File 1:
	1 4	(4 words in line 1)
	2 1	(1 word in line 2)
	3 3	(3 words in line 3)

File 2:
	This 2
	is 1
	a 2
	sample 1
	hacker 2

You are allowed to use open-source libraries for data structures, such as linked-list, AVL trees and such.  Use a programming language that you think fits the need.  Provide your own analysis of pros and cons of the data structure you use, time complexity, etc.

Objective is to optimize the execution.  At the end of the execution, your program should print the total execution time as well.  Your program should scale with more CPUs, i.e., your program should run faster as more CPUs are available.


Attached is an executable ‘createfile’ that you can use to create a large input file.  This executable will only run on Linux x86-84 systems.  It will create ~10G file (it will take minutes depending on your system).  If you have a different OS, you need to first create a program that can generate  ~10G file with lines of text.
 

