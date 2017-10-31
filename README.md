# Find Political Donors

A Python 3 script that:

- reads the FEC data under the "input" directory which lists campaign contributions by individual donors; 
- output a calculated running median, total dollar amount and total number of contributions by recipient and zip code as "median_byzip.txt" under "output" folder.
- output a calculated running median, total dollar amount and total number of contributions by recipient and zip code as "median_bydate.txt" under "output" folder.

## Requires

Python 3
Bisect ,Time and Sys Module

## How it's done

Reading the input is straightforward. We simply read each line as a string, parse it into a list of (21) values, examine and load the relevant data. 

We use two python dictionaries to organize the data. The keys of the dictionary are tuples of recipient and zip (or recipient and date). The values of the dictionary are lists of the correspondent transaction amounts for individual keys. These lists are kept sorted using the Bisect module whenever the list is updated. It's easy to finding the running median in a sorted list.

For "median_bydate.txt" we load the whole file in RAM, build the dictionary and sort the dictionary keys to keep the output sorted in alphabetic and chronological order. It would be fast as long as the file can fit in RAM.
For "median_byzip.txt" as the data is considered a stream (each line in input file) we instead iterate over it and update the running median on the fly. 

## Usage

run.sh