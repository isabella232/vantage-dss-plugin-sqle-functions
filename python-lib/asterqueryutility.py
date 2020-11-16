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

try:
    from sets import Set # Support for Python 2.7 and below.
except ImportError:
    Set = set # Support for Python 3 above.
import json

from asterargumentfactory import *

def areStringsEqual(a, b):
    """
    Returns if two strings are the same, disregarding cases.
    """
    return a.upper() == b.upper()

def isStringInList(astring, blist):
    """
    Returns if `astring` is in `blist` regardless of cases.
    """
    return astring.upper() in [y.upper() for y in blist]

def isArgumentDictionaryEntry(argument, x):
    """
    Checks if `argument` is in `x`'s list of alternateNames or is `x`'s name.
    """
    argumentname = argument.get('name', '')
    entryname = x.get('name', '')
    blist = x.get('alternateNames', [])
    return areStringsEqual(argumentname, entryname) or \
        isStringInList(argumentname, blist)
    
def getArgumentClause(cargument, arg_dict, inputTables):
    argumentdef  = next(iter(x for x in arg_dict if isArgumentDictionaryEntry(cargument, x)), {})
    asterarg = AsterArgumentFactory.createArg(cargument, argumentdef, inputTables)
    return asterarg.argumentclause

def getOutTableClause(cargument, arg_dict, inputTables):
    argumentdef  = next(iter(x for x in arg_dict if isArgumentDictionaryEntry(cargument, x)), {})
    asterarg = AsterArgumentFactory.createArg(cargument, argumentdef, inputTables)
    return asterarg.argumentclause

def getJoinedArgumentsString(cargumentslist, arg_dict, inputTables=[]):
    arguments = ''.join(map(lambda x: getArgumentClause(x, arg_dict, inputTables), \
                         cargumentslist))
    return arguments # and 'USING\n' + arguments

def getJoinedOutputTableString(cargumentslist, arg_dict, inputTables=[]):
    # return ''.join(map(lambda x: 'OUT TABLE ' + str(getOutTableClause(x, arg_dict, inputTables)), \
    return ''.join(map(lambda x: '' + str(getOutTableClause(x, arg_dict, inputTables)), \
                         cargumentslist))

def getArgumentClausesFromJson(f):
    return f.get('argument_clauses',[])

def getOutputTableClausesFromJson(f):
    return f.get('output_tables',[])


try:
    from dataiku.customrecipe import *
    DEFAULT_RETURN = ""
except:
    DEFAULT_RETURN = {}
def getJson(function_name):
    """
    Returns the contents of the JSON file with the given file name.
    
    If the file cannot be loaded, returns an empty string with `dataiku.customrecipe` loaded else returns an empty dictionary.
    """
    ret = DEFAULT_RETURN
    try:
        with open("%s/data/%s" % (get_recipe_resource(), function_name +".json")) as json_file: # Removed coprocessor.
            ret = json.load(json_file)
        return ret
    except:
        return ret