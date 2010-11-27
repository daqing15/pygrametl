"""This module holds classes that can be used as data soures. A data source
   must be iterable and provide dicts with data values.
"""

#  
#  Copyright (c) 2009, 2010 Christian Thomsen (chr@cs.aau.dk)
#  
#  This file is free software: you may copy, redistribute and/or modify it  
#  under the terms of the GNU General Public License version 2 
#  as published by the Free Software Foundation.
#  
#  This file is distributed in the hope that it will be useful, but  
#  WITHOUT ANY WARRANTY; without even the implied warranty of  
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU  
#  General Public License for more details.  
#  
#  You should have received a copy of the GNU General Public License  
#  along with this program.  If not, see <http://www.gnu.org/licenses/>.  
#  

from csv import DictReader
from Queue import Queue, Empty
from threading import Thread

__author__ = "Christian Thomsen"
__maintainer__ = "Christian Thomsen"
__version__ = '0.1.1.1'
__all__ = ['CSVSource', 'SQLSource', 'BackgroundSource', 'HashJoiningSource',
           'MergeJoiningSource']


CSVSource = DictReader

class SQLSource(object):
    """A class for iterating the result set of a single SQL query."""

    def __init__(self, connection, query, names=(), initsql=None, \
                     cursorarg=None):
        """Arguments:
           - connection: the PEP 249 connection to use.
           - query: the query that generates the result
           - names: names of attributes in the result. If not set,
             the names from the database are used. Default: ()
           - initsql: SQL that is executed before the query. The result of this
             initsql is not returned.
           - cursorarg: if not None, this argument is used as an argument when
             the connection's cursor method is called.
        """
        self.connection = connection
        if cursorarg is not None:
            self.cursor = connection.cursor(cursorarg)
        else:
            self.cursor = connection.cursor()
        if initsql:
            self.cursor.execute(initsql)
        self.query = query
        self.names = names
        self.executed = False

    def __iter__(self):
        try:
            if not self.executed:
                self.cursor.execute(self.query)
                names = None
                if self.names or self.cursor.description:
                    names = self.names or \
                        [t[0] for t in self.cursor.description]
            while True:
                data = self.cursor.fetchmany(500)
                if not data:
                    break
                if not names:
                    # We do this to support cursor objects that only have
                    # a meaningful .description after data has been fetched.
                    # This is, for example, the case when using a named
                    # psycopg2 cursor.
                    names = [t[0] for t in self.cursor.description]
                if len(names) != len(data[0]):
                    raise ValueError, \
                        "Incorrect number of names provided. " + \
                        "%d given, %d needed." % (len(names), len(data[0]))
                for row in data:
                    yield dict(zip(names, row))
        finally:
            try:
                self.cursor.close()
            except Exception:
                pass

class BackgroundSource(object):
    """A class for iterating another *Source in a separate thread."""

    def __init__(self, source, size=100):
        """Arguments:
           - source: the Source to iterate
           - size: the size of the Queue used by this BackgoundSource
        """
        self.__source = source
        self.__queue = Queue(size)
        self.__done = False
        t = Thread(target=self.__worker)
        t.start()


    def __worker(self):
        for row in self.__source:
            self.__queue.put(row, True)
        self.__done = True

    def __iter__(self):
        while True:
            try:
                yield self.__queue.get(True, 1)
            except Empty:
                if self.__done:
                    return

class HashJoiningSource(object):
    """A class for equi-joining two data sources."""

    def __init__(self, src1, key1, src2, key2):
        """Arguments:
           - src1: the first source. This source is iterated row by row.
           - key1: the attribute of the first source to use in the join
           - src2: the second soruce. The rows of this source are all loaded
             into memory.
           - key2: the attriubte of the second source to use in the join.
        """
        self.__hash = {}
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2

    def __buildhash(self):
        for row in self.__src2:
            keyval = row[self.__key2]
            l = self.__hash.get(keyval, [])
            l.append(row)
            self.__hash[keyval] = l
        self.__ready = True

    def __iter__(self):
        self.__buildhash()
        for row in self.__src1:
            matches = self.__hash.get(row[self.__key1], [])
            for match in matches:
                newrow = dict(row)
                newrow.update(match)
                yield newrow

JoiningSource = HashJoiningSource # for compatability


class MergeJoiningSource(object):
    def __init__(self, src1, key1, src2, key2):
        self.__src1 = src1
        self.__key1 = key1
        self.__src2 = src2
        self.__key2 = key2
        self.__next = None

    def __iter__(self):
        iter1 = self.__src1.__iter__()
        iter2 = self.__src2.__iter__()

        row1 = iter1.next()
        keyval1 = row1[self.__key1]
        rows2 = self.__getnextrows(iter2)
        keyval2 = rows2[0][self.__key2]

        while True: # At one point there will be a StopIteration
            if keyval1 == keyval2:
                # Output rows
                for part in rows2:
                    resrow = dict(row1)
                    resrow.update(part)
                    yield resrow
                row1 = iter1.next()
                keyval1 = row1[self.__key1]
            elif keyval1 < keyval2:
                row1 = iter1.next()
                keyval1 = row1[self.__key1]
            else: # k1 > k2
                rows2 = self.__getnextrows(iter2)
                keyval2 = rows2[0][self.__key2]

    def __getnextrows(self, iter):
        res = []
        keyval = None
        if self.__next is not None:
            res.append(self.__next)
            keyval = self.__next[self.__key2]
            self.__next = None
        while True:
            try:
                row = iter.next()
            except StopIteration:
                if res:
                    return res
                else:
                    raise
            if keyval is None:
                keyval = row[self.__key2] # for the first row in this round
            if row[self.__key2] == keyval:
                res.append(row)
            else:
                self.__next = row
                return res


