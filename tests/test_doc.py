import unittest
import pymongo
from pymongo import Connection

from ocm import *

import time

class TestDoc(unittest.TestCase):
    dat = {"fld1": "field-one", "fld2": "field-two"}

    def getDocNew(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2")                       
                       ]

        return  M.new(self.dat)
    
    def test_DocFieldsDictProper(self):
        m = self.getDocNew()
        
        self.assertEqual(m.fld1, "field-one")
        self.assertEqual(m["fld1"], "field-one")
        
        self.assertEqual(self.dat, dict(m)) 
        
        m.fld3 = "field-three"
        self.assertEqual(m.fld3, "field-three")
        self.assertRaises(Exception, m.__getitem__, "fld3")
        
    def test_DocFields(self):
        """
        1. Required
            a. Present in data
            b. Missing from data
        2. Default
        3. Custom validator
        """
        
        def f3v(field, value):
            if "ok" != value:
                return "%s is not ok" % field.name
            
        class N(Doc):
            mgr = Mgr("localhost", 27017, "test")
            fields = [Field(str, "fld1", required=True),
                       Field(str, "fld3", default="field3"),
                       Field(str, "fld4", validator=f3v)
                       ]
        
        d = self.dat.copy()
        d["fld4"] = "ok"
        
        m = N.new(d)
        
        # 1. Required
        #   a. Present
        self.assertEqual(m.fld1, "field-one")
        #   b. Missing
        del m["fld1"]
        self.assertEqual(m.is_valid(), False)
        m["fld1"] = d["fld1"]
        
        # 2. Default
        self.assertEqual(m.fld3, "field3")

        # 3. Custom Validator
        self.assertEqual(m.is_valid(), True)
        self.assertEqual(m.errors(), {})
        
        m.fld4 = "bad"
        self.assertEqual(m.is_valid(), False)
        self.assertEqual(m.errors()["fld4"], "fld4 is not ok")
        
    def test_DocCustomValidation(self):
        m = self.getDocNew()
        
        def customValidatorTuple(doc):
            return ("customValidator", "object Not Valid")
        
        m.validate = customValidatorTuple
        
        self.assertEqual(m.is_valid(), False)
        self.assertEqual(m.errors()["customValidator"], "object Not Valid")
        
        def customValidatorList(doc):
            return [("customValidator1", "1 object Not Valid"),
                    ("customValidator2", "2 object Not Valid")]
            
        m.validate = customValidatorList
        
        self.assertEqual(m.is_valid(), False)
        self.assertEqual(m.errors()["customValidator1"], "1 object Not Valid")
        self.assertEqual(m.errors()["customValidator2"], "2 object Not Valid")
        
        
        def customValidatorTuple_accessDoc(doc):
            if doc.fld1 != "blah":
                return ("customValidator", "fld1 has bad value")
        
        m.validate = customValidatorTuple_accessDoc

        self.assertEqual(m.is_valid(), False)
        self.assertEqual(m.errors()["customValidator"], "fld1 has bad value")
        
    def test_CRUD_CreateUpdate(self):
        m = self.getDocNew()
        
        # Bare bones - 
        self.assertEqual(m.save(), True)
        
        # Fail Global validation
        def customValidator(doc):
            return ("doc", "just testing")
        
        m.validate = customValidator
        
        self.assertRaises(OCMInvalidException, m.save)
        self.assertEqual(m.errors()["doc"], "just testing")
        
        # Fail Field validation
        class N(Doc):
            mgr = Mgr("localhost", 27017, "test")
            fields = [Field(str, "fld1", required=True)]
            
        d = self.dat.copy()
        del d["fld1"]
        m = N.new(d)
        
        self.assertRaises(OCMInvalidException, m.save) 
        self.assertEqual(m.errors()["fld1"], "fld1 is required")
        
        # before_save
        m = self.getDocNew()
        
        self.var = ""
        def custom_before_save(doc):
            self.var = "custom_before_save"
            return True
        m.before_save = custom_before_save
        
        m.save()
        self.assertEqual(self.var, "custom_before_save")
        
        m.befsave_msg = ""
        def custom_before_save_passmsg(doc):
            doc.befsave_msg = "befsave_msg"
            return False
        m.before_save = custom_before_save_passmsg
        
        self.assertEqual(m.save(), False)
        self.assertEqual(m.befsave_msg, "befsave_msg")
        
        def custom_after_save(doc):
            doc.aftsave_msg = "aftsave_msg"
            
        m.before_save = None
        m.after_save = custom_after_save
        
        self.assertEqual(m.save(), True)
        self.assertEqual(m.aftsave_msg, "aftsave_msg")

    def test_CRUD_Read(self):
        # simple count validation (can we fetch everything)?
        connection = Connection('localhost', 27017)
        db = connection.test
        coll = db.stickies
        
        c = 0
        for x in coll.find():
            c += 1
        
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "stickies"
            
            fields = [Field(str, "title"),
                      Field(str, "unique")
                      ]
            
        o = 0
        for x in M.find():
            o += 1
            
        self.assertEqual(c, o)
        
        # can we fetch with simple critieria?
        c = 0
        for x in coll.find({"summary": "Make something work"}):
            c += 1
            
        o = 0
        for x in M.find({"summary": "Make something work"}):
            o += 1
        
        self.assertEqual(c, o)
      
        # Save something unique, then retrieve it
        coll.remove({"unique": "1234"})
        o = M.find({"unique": "1234"})
        self.assertEqual(0, len(o))
        
        t = M.new({"unique": "1234"})
        t.save()
        
        o = M.retrieve({"unique": "1234"})
        self.assertNotEqual(o, None)
        self.assertEqual(o.unique, "1234")
        
        
    def test_Retrieve(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "stickies"
            
            fields = [Field(str, "title"),
                      Field(str, "unique")
                      ]
        m = M.new(self.dat)
        m.save()
        
        o = M.retrieve({"fld1": "field-one"})
        self.assertNotEqual(o, None)
        self.assertTrue(isinstance(o, M))
#        print o
           
    def test_CRUD_Delete(self):
        
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2")                       
                       ]
            
        m = M.new(self.dat)
            
        connection = Connection('localhost', 27017)
        db = connection["test"]
        coll = db["test"]

        # Clear out the collection
        coll.remove()
        
        # Make sure it's cleared out
        colcount = coll.find().count()
        self.assertEqual(0, colcount)
        
        # Save something and make sure there's one thing in the collection
        r = m.save()
        self.assertEqual(r, True)
#        print db.command({"getlasterror":1})
#        for x in coll.find():
#            print x
#        for x in M.find():
#            print x 
#        colcount = coll.find().count()
        colcount = M.count()
        self.assertEqual(1, colcount, m.errors())
        
        # Delete the thing we just saved
        colcount = None
        m.delete()
#        print "count: ", M.count()
#        time.sleep(2)
#        coll = db["test"]

        # Make sure it was deleted
        #  NOTE: Need to use the connection that was used to do the delete?
#        colcount = coll.find().count()
        colcount = M.count()
        self.assertEqual(0, colcount)
        
    def test_AutoIncField(self):
        
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "stickies"
            
            fields = [AutoIncField("fld_id", "testseq")]
       
       
        connection = Connection('localhost', 27017)
        db = connection.test
        coll = db.sequences
        
        obj = coll.find_one( {"seqname": "testseq", "lastval" : { "$gte" : 0}} )
        if obj:
            coll.remove({"seqname": "testseq"})
            
        d = M.new()
        
        tested = False
        for f in d.fields:
            if f.name == "fld_id":
                self.assertEqual(f.name, "fld_id")
                self.assertEqual(f.seqname, "testseq")
                self.assertEqual(f.required, False)
                self.assertEqual(f.default, None)
                self.assertEqual(f.validator, None)
                self.assertEqual(f.invalid_message, None) 
                
                tested = True
        self.assertEqual(tested, True)

        self.assertEqual(d.fld_id, None)
        
        d2 = M.new()
        d2.save()
        self.assertEqual(d2.fld_id, 1)
        
        # Make sure existing values don't get clobbered
        d3 = M.new({"fld_id": -10})
        self.assertEqual(d3.fld_id, -10)
        d3.save()
        self.assertEqual(d3.fld_id, -10)
        
        # Make sure we can't remove it from the outside
        del d3["fld_id"]
        self.assertEqual(d3.get("fld_id"), None)
        
        d3.save()
        self.assertEqual(d3.fld_id, 2)
        
    def test_NestedType(self):
        class N(Doc):
            mgr = Mgr("localhost", 27017, "test")
            fields = [Field(str, "n-fld1"),
                      Field(int, "n-fld2-int")
                      ]
#            def validate(self, item):
#                return [("my N", "failed validation")]

        
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(str, "fld1"),
                       Field(str, "fld2"),
                       NestedDocField(N, "nested-fld1")                       
                     ]
        d = self.dat
#        print "d: ", d
        d["nested-fld1"] = {"n-fld1": "inside", "n-fld2-int": 2}
#        print "d: ", d
        
        m = M.new(d)
        self.assertTrue(m.is_valid())
        self.assertEqual(m._errors, {})
#        print "m: ", m
#        
#        print "m valid: ", m.is_valid()
#        print "m errs: ", m._errors
        
        o = m["nested-fld1"]
        self.assertTrue(isinstance(o, N))
#        print "o: ", o
#        print "o type: ", o.__class__
#        print "o valid: ", o.is_valid()
#        print "o errs: ", o._errors
        
    def test_ListFieldType(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(list, "alist")                       
                     ]        
        d = {"alist": ["one", "two", "three"]}
        m = M.new(d)
        
        self.assertTrue(m.is_valid())
        self.assertEqual(m._errors, {})
        self.assertTrue(isinstance(m.alist, list))
        self.assertEqual(m.alist, ["one", "two", "three"])
    
    
    def test_ListOfDocFieldType(self):
        class M(Doc):
            mgr = Mgr("localhost", 17017, "test")
            collection = "test"
            fields = [Field(list, "alist")
                      ]
        class N(Doc):
            fields = [Field(str, "astring"),
                      Field(int, "anint")
                      ]
            
        n = N.new({"astring": "somestring",
                   "aning": 1})
        m = M.new({"alist": [n]})
        
        self.assertTrue(m.is_valid())
        self.assertEqual(m._errors, {})
        self.assertTrue(isinstance(m.alist, list))
        self.assertTrue(isinstance(m.alist[0], Doc))
        
        
    
    def test_TupleType(self):
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [Field(tuple, "atuple")                       
                     ]        
        d = {"atuple": ("one", "two")}
        m = M.new(d)
        
        self.assertTrue(m.is_valid())
        self.assertEqual(m._errors, {})
        self.assertTrue(isinstance(m.atuple, tuple))
        self.assertEqual(m.atuple, ("one", "two"))

            
    def test_ListOfDocTypes(self):
        class N(Doc):
            mgr = Mgr("localhost", 27017, "test")
            fields = [Field(str, "n-fld1"),
                      Field(int, "n-fld2-int")
                      ]
            
        class M(Doc):
            mgr = Mgr("localhost", 27017, "test")
            collection = "test"
            fields = [ListOfDocsField(N, "alist"),
                      Field(list, "blist")                      
                     ]
        n1 = {"n-fld1": "fld1-1", "n-fld2-int": 11}        
        n2 = {"n-fld1": "fld1-2", "n-fld2-int": 12}
        l = []
        l.append(n1)
        l.append(n2)
#        print "l: ", l
        blist = ["b1", "b2"]
        
        d = {"alist": l, "blist": blist}
        m = M.new(d)
        
        fs = dict((f.name, f) for f in m.fields)
        if fs.has_key("alist"):
            print "fs has_key"
            r = fs["alist"]
            print fs["alist"].__dict__
            print "instance: ", isinstance(r, ListOfDocsField)
            print "instance 2: ", isinstance(r, ListField)
       
#        print "m: ", m
#        print m.alist
#        print m["alist"]
#        print m["alist"].__class__
#        print m["alist"][0].__class__
        self.assertTrue(m.is_valid())
        self.assertTrue(isinstance(m.alist, list))
        self.assertTrue(isinstance(m["alist"], list))
        self.assertTrue(isinstance(m["alist"][0], N))
        self.assertTrue(isinstance(m.alist[1], N))
        self.assertTrue(isinstance(m.blist, list))
#        print "m: ", m
#        print "m.alist: ", m.alist
#        print "m[alist]: ", m["alist"]
#        print "m.blist: ", m.blist

    def test_ComplexStructures(self):
        
        class N(Doc):
            fields = [Field(str, "astr")]
        
        m = N()
        m.fields.append(Field(N, "test"))
        
        N.fields.append(Field(N, "test"))
        
        
        class M(Doc):
            fields = [Field(str, "author"),
                      ListField(str, "moresels"),
                      Field(dict, "nuggets")
                      ]
            
        d = {"author": "me", 
             "moresels": ["m1", "m2", 3], 
             "nuggets": {"n1": "one", "n2": 2}}

        m = M.new(d)
        self.assertTrue(m.is_valid())
        self.assertTrue(isinstance(m.author, str))
        self.assertTrue(isinstance(m.moresels, list))
        self.assertTrue(len(m.moresels) > 1)
        self.assertTrue(isinstance(m.nuggets, dict))
        self.assertTrue(len(m.nuggets) > 1)
        
#    def test_RefField(self):
#        class O(Doc):
#            mgr = Mgr("localhost", 27017, "test")
#            collection = "other"
#            fields = [Field(str, "greeting")
#                      ]
#            
#        class N(Doc):
#            mgr = Mgr("localhost", 27017, "test")
#            collection = "test"
##            fields = [RefField(O, "ref_id", pymongo.objectid.ObjectId, False)
#            fields = [RefField(O, "ref_id", str, False)
#                      ]
#      
#        reffedobj = O.new({"greeting": "howdy there"})
#        self.assertEqual(reffedobj.save(), True)
#        
#        n = N.new({"ref_id": reffedobj._id})
#        self.assertEqual(n.save(), True)
#        
#        print n
#        print n.ref_id
        
      
      
      
            
      
      
        
if __name__ == "__main__":
    unittest.main()
