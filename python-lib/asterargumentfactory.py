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

from booleanargument import *
from doubleargument import *
from integerargument import *
from listargument import *
from sqlexprargument import *
from tableargument import *

class AsterArgumentFactory(object):
    '''
    Creates specific Aster argument objects depending on the data type of the argument as is specified in the JSON file.
    '''
    
    @staticmethod
    def createArg(argument, argumentDef, inputTables):
        dtype = argument.get('datatype', '').upper()
        allowsLists = argument.get('allowsLists', False)
        name = argument.get('name', '')
        if dtype == "BOOLEAN":
            return BooleanArgument(argument, argumentDef)
        elif dtype == "SQLEXPR":
            return SqlExprArgument(argument, argumentDef)
        elif dtype in ["INTEGER", "LONG"] and not allowsLists:
            return IntegerArgument(argument, argumentDef)
        elif dtype in ["DOUBLE", "DOUBLE PRECISION"] and not allowsLists:
            return DoubleArgument(argument, argumentDef)
        elif dtype == "TABLE_NAME" and not allowsLists and not argumentDef.get('isOutputTable', False):
            return TableArgument(argument, argumentDef, inputTables)
        elif isinstance(argument.get('value',''), (list, tuple)):     
            argument['value'] = filter(None, argument.get('value'))
            return ListArgument(argument, argumentDef)
        # TODO: Better handling of nPath or other clauses like this. 
        # These arguments are handled differently, they require no quotation marks surrounding them. 
        elif name == "Symbols" or name == "Result" or name == "Mode": 
            return SqlExprArgument(argument, argumentDef)
        else:
            return AsterArgument(argument, argumentDef)
