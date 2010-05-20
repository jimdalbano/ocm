import unittest
import pymongo
from pymongo import Connection

from ocm import *


class TestMgr(unittest.TestCase):
    dat = {"fld1": "field-one", "fld2": "field-two"}

    def tearDown(self):
        connection = Connection('localhost', 27017)
        db = connection.test
        coll = db.test
        coll.remove()  
        
        coll = db.sequences
        coll.remove()      

    def getDocNew(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2")                       
                       ]

        return  M.new(self.dat)
    
    def getAllStickies(self):
        connection = Connection('localhost', 27017)
        db = connection.test
        coll = db.test
        
        l = []
        for s in coll.find():
            l.append(s)
        return l
    
    def getColl(self):
        connection = Connection('localhost', 27017)
        db = connection.test
        coll = db.test
        return coll
    
    def test_newgetsobjectid(self):
        m = Mgr("localhost", 27017, "test")
        d = self.getDocNew()
        
        # Not much of a test here.
        self.assertTrue(not d.get("_id", None))
        self.assertTrue(m.save(d))
        self.assertTrue(d["_id"])
        

    def test_saveisupsert(self):
        m = Mgr("localhost", 27017, "test")
        d = self.getDocNew()
        
        # First save a brandy new one -
        # and validate count increases by 1
        before = self.getAllStickies()
        self.assertTrue(d.save())
        after = self.getAllStickies()
        self.assertEqual(len(before) + 1, len(after))
        
        # Change something, then save it -
        # and validate count remains the same,
        # and ours (_id) has changes
        myid = d["_id"]
        d.fld1 = "new val"
        
        self.assertTrue(d.save())
        after2 = self.getAllStickies()
        self.assertEqual(len(after), len(after2))
        for o in after2:
            if o["_id"] == myid:
                p = o
                break
        self.assertTrue(p)
        self.assertEqual(p["fld1"], "new val")
    
    def test_getAll(self):
        m = Mgr("localhost", 27017, "test")
        d = self.getDocNew()
        
        before = self.getAllStickies()
        after = m.get(d, None)
        self.assertEqual(len(before), len(after))
    
    def test_getReturnsProperType(self):
        class N(Doc):
            fields = [Field(str, "sub1"),
                      Field(str, "sub2")]
            
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2"),
                       Field(int, "int1"), 
                       Field(N, "ndoc" ),
                       Field(list, "lst1")                     
                       ]
        data = self.dat.copy()
        data["int1"] = 1
        data["ndoc"] = {"sub1": "sub-1",
                        "sub2": "sub-2"}
        data["lst1"] = ["lst-a", "lst-b", "lst-c"]
        
        
        d = M.new(data)
        c = self.getColl()
        id = c.save(d)
        
        crit = { "_id": id }
        ret = M.find(crit)
        
        self.assertTrue(isinstance(ret, list))
        self.assertEqual(len(ret), 1)
        # just checking the we have what saved
        self.assertEqual(id, ret[0]["_id"])
        self.assertTrue(isinstance(ret[0], d.__class__))
        
        # TODO: Very naive - checks top level fields only.  Should recurse.
        for f in d.fields:
            self.assertTrue(isinstance(ret[0][f.name], f.fldtype))
        
    def test_nextval(self):
        mgr = Mgr("localhost", 27017, "test")
        
        conn = Connection("localhost", 27017)
        mdb = conn["test"]
        coll = mdb["sequences"]

        obj = []
        for x in coll.find( {"tst_seq" : { "$gte" : 0}} ):
            obj.append(x)
            
        self.assertEqual(len(obj), 0)
        
        x = mgr._nextval("tst_seq")  
        self.assertEqual(x, 1)
              
        
        n = mgr._nextval("tst_seq")
        for x in range(5):
            r = mgr._nextval("tst_seq")
            
        self.assertEqual(n + 5, r)
        
    def test_count(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2")                       
                       ]
        self.assertEqual(0, M.count())

        d = M.new(self.dat)
        self.assertEqual(0, M.count({"fld1": "field-one"}))
        
        d.save()
        self.assertEqual(1, M.count())
        self.assertEqual(1, M.count({"fld1": "field-one"}))
        
        e = M.new(self.dat)
        e.fld1 = "something else"
        e.save()
        self.assertEqual(2, M.count())
        self.assertEqual(1, M.count({"fld1": "field-one"}))
        
        e.delete()
        self.assertEqual(1, M.count())
        
        
        
if __name__ == "__main__":
    unittest.main()