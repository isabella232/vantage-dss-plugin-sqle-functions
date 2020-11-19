# -*- coding: utf-8 -*-
'''
Copyright © 2019 by Teradata.
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
associated documentation files (the "Software"), to deal in the Software without restriction,
including without limitation the rights to use, copy, modify, merge, publish, distribute,
sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or
substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

# Pretty-printing of Dictionaries.
import pprint

import dataiku
from dataiku.customrecipe import *
from dataiku import Dataset
# Import the class that allows us to execute SQL on the Studio connections
from dataiku.core.sql import SQLExecutor2

# Import plugin libs
from querybuilderfacade import *
from inputtableinfo import *
from outputtableinfo import *
from outputtableinfo import *


def asterDo():
    """
    Takes the parameters set by the user from the UI, creates the query, and then executes it.
    """
    # Debug options.
    sep = "="
    sep_length = 80
    
    # Recipe inputs
    main_input_name = get_input_names_for_role('main')[0]
    input_dataset = dataiku.Dataset(main_input_name)

    # Recipe outputs
    main_output_name = get_output_names_for_role('main')[0]
    output_dataset =  dataiku.Dataset(main_output_name)
    
    # Daitaiku DSS params
    client = dataiku.api_client()
    projectkey = main_input_name.split('.')[0]
    project = client.get_project(projectkey)
    
    # Connection properties.
    # TODO: Handle Input and Output Table connection properties.
    properties = input_dataset.get_location_info(sensitive_info=True)['info'].get('connectionParams').get('properties')
    autocommit = input_dataset.get_location_info(sensitive_info=True)['info'].get('connectionParams').get('autocommitMode')
    
    # SQL Executor.
    executor = SQLExecutor2(dataset=input_dataset)   
    
    # Handle pre- and post-query additions.
    # Assume autocommit TERA mode by default.
    pre_query = None
    post_query = None
    
    print(sep*sep_length)
    if not autocommit:
        print("NOT AUTOCOMMIT MODE.")
        print("Assuming TERA mode.")
        pre_query = ["BEGIN TRANSACTION;"]
        post_query = ["END TRANSACTION;"]
        for prop in properties:
            if prop['name'] == 'TMODE':
                if prop['value'] == 'ANSI':
                    print("ANSI mode.")
                    pre_query = [";"]
                    post_query = ["COMMIT WORK;"]
    
    else:
        print("AUTOCOMMIT MODE.")
        print("No pre- and post-query.")
    print (sep*sep_length)

    # Recipe function param
    dss_function = get_recipe_config().get('function', None)
    pp = pprint.PrettyPrinter(indent=4)
    print(sep*sep_length)
    print('DSS Function:')
    pp.pprint(dss_function)
    print(sep*sep_length)

    # output dataset
    try:
        outputTable = outputtableinfo(output_dataset.get_location_info()['info'], main_output_name,
                                  get_recipe_config() or {})
    except Exception as error:
        raise RuntimeError("""Error obtaining connection settings for output table."""                           
                           """ This plugin only supports Teradata tables.""")

    # input datasets
    try:
        main_input_names = get_input_names_for_role('main')
        inputTables = []
        for inputname in main_input_names:
            inconnectioninfo = dataiku.Dataset(inputname).get_location_info()['info']
            inTable = inputtableinfo(inconnectioninfo, inputname, dss_function)
            inputTables.append(inTable)
    except Exception as error:
        raise RuntimeError("""Error obtaining connection settings from one of the input tables."""                           
                           """ This plugin only supports Teradata tables.""")

    if dss_function.get('dropIfExists', False):
        print("Preparing to drop tables.")
        drop_query = dropTableStatement(outputTable)
        
        print(sep*sep_length)
        print("DROP query:")
        print(drop_query)
        print(sep*sep_length)
        
        # executor.query_to_df(dropTableStr)
        # if not autocommit:
        #     print('Start transaction for drop')
        #     print(stTxn)
        #     executor.query_to_df(stTxn)

        #     print('End transaction for drop')
        #     print(edTxn)
        #     executor.query_to_df(edTxn,[dropTableStr])
        # else:
        #     executor.query_to_df([dropTableStr])
        
    
        if not autocommit:
            executor.query_to_df(pre_query)
        executor.query_to_df(drop_query, post_query)

        #Start transaction
        #Move to drop query and make each DROP run separately
        drop_all_query = getDropOutputTableArgumentsStatements(dss_function.get('output_tables', []))
        
        print(sep*sep_length)
        print('Drop ALL Query:')
        print(drop_all_query)
        print(sep*sep_length)
        
        # if requiresTransactions:
            # print('Start transaction for drop')
            # print(stTxn)
            # executor.query_to_df(stTxn)
        # dropAllQuery = getDropOutputTableArgumentsStatements(dss_function.get('arguments', []))
        
        #Change dropAllQuery to string/s? or execute one drop per item in list.       
        for drop_q in drop_all_query:
        #     print('Start transaction for drop')
        #     print(stTxn)
        #     executor.query_to_df(stTxn)

        #     print('End transaction for drop')
        #     print(edTxn)
        #     executor.query_to_df(edTxn,[dropTblStmt])
            if not autocommit:
                executor.query_to_df(pre_query)
            executor.query_to_df(drop_q, post_query)
        # else:
        #     for dropTblStmt in dropAllQuery:
        #         executor.query_to_df([dropTblStmt])
    # executor.query_to_df("END TRANSACTION;", pre_queries=query)

    # actual query
    create_query = getFunctionsQuery(dss_function, inputTables, outputTable, get_recipe_config() or {})
    print(sep*sep_length)
    print("CREATE query:")
    print(query)
    print(sep*sep_length)
    # Detect error
    try:
        if not autocommit:
            executor.query_to_df(pre_query)
        executor.query_to_df(create_query, post_query)
        # selectResult = executor.query_to_df(query, pre_queries=pre_query, post_queries=post_query)
    except Exception as error:
        err_str = str(error)
        err_str_list = err_str.split(" ")
        # trying to shorten the error for the modal in front-end
        if len(err_str_list) > 15:
            print(sep*sep_length)
            print (error)
            print(sep*sep_length)
            new_err_str = err_str_list[:15]
            new_err_str.append("\n\n")
            new_err_str.append("...")
            new_err_str = " ".join(new_err_str)
            raise RuntimeError(new_err_str)
        else:
            raise RuntimeError(err_str)

    
    print('Moving results to output...')

    # if not autocommit:
    #     print('Start transaction for schema building')
    #     print(stTxn)
    #     executor.query_to_df(stTxn)

    # pythonrecipe_out = output_dataset
    # pythonrecipe_out.write_with_schema(selectResult)
    select_sample_query = 'SELECT * from '+ outputTable.tablename + ' SAMPLE 0'
    if not autocommit:
        executor.query_to_df(pre_query)
    sel_res = executor.query_to_df(select_sample_query, post_query)
    # selRes = executor.query_to_df(customOutputTableSQL, pre_queries=pre_query, post_queries=post_query)

    pythonrecipe_out = output_dataset
    pythonrecipe_out.write_schema_from_dataframe(sel_res)

    # Additional Tables
    outtables = dss_function.get('output_tables', [])
    if(outtables != []):
        tableCounter = 1
        print('Working on output tables')
        print(get_output_names_for_role('main'))
        print(outtables)
        for table in outtables:
            if table.get('value') != '' and table.get('value') != None:
                # try:
                print('Table')
                print(table)
                #Need to change this to max of split in order for multiple database or no-database table inputs
                #Change below might fix issue 4 of Jan 4 2018 for Nico. For non-drop tables
                try:
                    main_output_name2 = list(filter(lambda out_dataset: out_dataset.split('.')[len(out_dataset.split('.'))-1] == table.get('value').split('.')[len(table.get('value').split('.'))-1].strip('\"'),get_output_names_for_role('main')))[0]
                except Exception as error:
                    # print(error.message)                    
                    raise RuntimeError('Unable to find an output dataset for '+table.get('value')+ 'It may not exist as an output Dataset: '+table.get('value')+"\n\Runtime Error:"+ error.message)
                print('Output name 2')
                print(main_output_name2)
                output_dataset2 =  dataiku.Dataset(main_output_name2)   
                # print("od2 printer")
                tableNamePrefix = output_dataset2.get_location_info(sensitive_info=True)['info'].get('connectionParams').get('namingRule').get('tableNameDatasetNamePrefix')
                if tableNamePrefix != None or tableNamePrefix != '':
                    print('Table prefix is not empty:' + tableNamePrefix)
                # except:
                #     #Need to change this error
                #     print('Error: Dataset for' + table.get('name') + ' not found')  
                #     raise Value              
                customOutputTableSQL = 'SELECT * from '+ table.get('value') + ' SAMPLE 0'
                print('Working on table number:')
                print(tableCounter)
                print(customOutputTableSQL)
                if not autocommit:
                    executor.query_to_df(pre_query)
                sel_res = executor.query_to_df(customOutputTableSQL, post_query)
                # selRes = executor.query_to_df(customOutputTableSQL, pre_queries=pre_query, post_queries=post_query)
                tableCounter += 1
                pythonrecipe_out2 = output_dataset2
                pythonrecipe_out2.write_schema_from_dataframe(selRes)
    # if not autocommit:
    #     print('End transaction for schema building')
    #     print(edTxn)
    #     executor.query_to_df(edTxn)
    print('Complete!')  


# Uncomment end