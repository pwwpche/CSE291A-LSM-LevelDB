# CSE291A-LSM Trees, Bloom Filters, and Column-Oriented Data

## Overview

Task for this project is to build the storage underlying a NoSQL database.


## The Details: Storage and Indexing

* All data can be viewed as strings.
* Persistent storage for columns of data must take the form of one or more sorted string tables on secondary storage.
* Storage should be append-only, but deletes and edits should be accomodated via appends.
* Writes, deletes, edits, etc, should be aggregated and sorted in memory, and spilled to disk only when the amortized cost of doing so makes sense.
* An LSM-like approach should loosely define the relationship between your in memory and persistent data structures.
* In memory Bloom filters should provide for efficient searches
* Commonly used records should be kept in memory, even if only as secondary copies.
It is permitted to construt additional indexes, e.g. hash tables and/or B-Trees, etc.
Access to columns should be paged to improve performance.

## The Details: Column Compression 
Your database should offer three options for the compression of individual columns:

* Efficient general-purposes block compression
* Dictionary compression based upon string values in the domain of the column
* Uncompressed

## The Details: The Data Model
Data should be modeled in a column-oriented way.
* Columns should be organized/organizable into column groups approximating tables.
* Column groups should be organized/organizable into databases representing a group of related column groups, e.g. used by one application.
* Through indexes, it should be "reasonably" efficient to access data by row, where necessary.

## The Details: The API
* You are not required to implement a query language, though you may choose to do so.
* Your API should allow functionality equivalent to basic JOIN, SELECT and WHERE queries in SQL
* Your API should offer three (3) examples of column-oriented operations that would be useful for analytics.
