-------------------------------------------------------------------------
- README file: The Teradata Vantage SQL Engine Functions Plugin 
-              
- Version: 2.0-1
-
- December 2020                                                                   
-
- Copyright (c) 2020 Teradata
-------------------------------------------------------------------------

The Teradata Vantage SQL Engine Functions Plugin allows end users to leverage
Vantage analytics within their DSS data science workflows. This plugin provides support
for a majority of the Advanced SQL Engine analytics functions in 16.20.

Note that Dataiku DSS itself also supports ANSI SQL push-down for most of their 
data preprocessing Visual recipes.  

The Plugins were tested with Vantage 1.1 Release Candidate 9.

I. System Requirements
----------------------

The following component versions are required for the Teradata Vantage Plugin: 

1. Dataiku Data Science Studio version 8.1
2. Teradata Vantage 2.0
3. Teradata JDBC Driver 16.20

For R and Python support in Teradata Vantage, one or both of the following are required:

1. 9687-2000-0120	R Interpreter and Add-on Pkg on Teradata Advanced SQL
2. 9687-2000-0122	Python Interpreter and Add-on Pkg on Teradata Advanced SQL

II. Install / Upgrade Instructions
----------------------------------

In order to install the Teradata Vantage Plugins for Dataiku DSS, perform the following:

1. In DSS Settings page (accessible through the Admin Tools button),
   select the [Plugins] tab, then select the [ADVANCED] option.
2. Click on [Choose File] and browse to the location of the Teradata
   Vantage Analytic Functions plugin zip file in your local filesystem.
3. If a previous installation of the Teradata Vantage Analytic plugin 
   exists, check "Is update".
4. Click on [UPLOAD] button.
5. When the upload succeeds, click on [Reload] button, or do a hard 
   refresh (Ctrl + F5) on all open Dataiku browsers for the change to 
   take effect.
6. Repeat for the Teradata Vantage SCRIPT Table Operator Plugin for R and 
   Python.


III. Limitations
----------------

1. For analytic functions that:
   - take in output table names as arguments, and
   - where the select query produces only a message table indicating the name 
     of the output model/metrics table, it is the responsibility of the user 
	 to specify output table names that are different from those of the existing
	 tables.
   Some analytic functions provide an option to delete an already existing output
   table prior to executing an algorithm, but others do not. In the former case, 
   the Advanced SQL Engine throws an "Already exists" exception.

2. This version of the Dataiku DSS Teradata Vantage Analytic Functions plugin only 
   supports Advanced SQL Engine functions.

3. The plugin only supports Teradata database datasets as input and output.

4. Functions with any OUTPUT TABLE type arguments will require the user to add an 
   output dataset for the SELECT statement results of the query and any additional
   output tables. Please refer to the Teradata Vantage Machine Learning Engine 
   Analytic Function Reference documentation page at docs.teradata.com to learn 
   about the output tables of each function.

5. The following Advanced SQL Engine functions are not supported:
   - DecisionForestPredict
   - DecisionTreePredict
   - GLMPredict
   - NaiveBayesPredict
   - NaiveBayesTextClassifierPredict
   - SVMSparsePredict

IV. References
-------------

For additional information on the Teradata Vantage Analytic Functions search for the following on docs.teradata.com:

1. "Teradata Vantage SQL Operators and User-Defined Functions"
2. "Teradata Vantage User Guide"
3. "Teradata Vantage Analytic Function User Guide"
4. "Teradata Vantage - NewSQL Engine Analytic Functions"