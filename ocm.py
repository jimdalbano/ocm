from datetime import datetime
from pymongo import Connection
import pymongo



class OCMInvalidException(Exception): pass
class OCMNotAllowedException(Exception): pass

class Mgr(object):
#    Put connection variable/info here
#    and pop via constructor and/or prop-setters
# 
#    Also little nice-ities like a switch allowing
#    full 'remove's, etc.
    def __init__(self, host, port, db):
        self.host = host
        self.port = port
        self.db = db
    def _getConn(self):
        conn = Connection(self.host, self.port)
        return conn[self.db]
    
    # Upsert functionality
    def save(self, obj):
        
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb[obj.collection]
        
        coll.save(obj)
        
        return True
 
    # Server side.
    def update(self, spec):
        return "update! ", spec
    
     
    def delete(self, obj):
#        print "here in Mgr.delete)"
#        return "delete! ", obj
        # really, do a remove obj.spec, then invalidate obj in some way (? = None)
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb[obj.collection]
        
        if obj.has_key("_id"):
            sp = { "_id": obj._id, "$atomic": True}
            coll.remove(sp)
        else:
            coll.remove(obj)
            
#        print "in delete: ", coll.find().count()
            
        
    def remove(self, criteria=None):
        if not criteria:
            raise OCMNotAllowedException
        
        return "blah!"
    
    # find_one -vs- find ?  multiple results need to be in a list
    # Right now, support either All, or field = val.
    def get(self, cls, criteria=None):
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb[cls.collection]
        
        x = []
#        if criteria:
        for i in coll.find(criteria):
            x.append(cls.new(i))
#        else:
#            for i in coll.find(criteria):
#                x.append(obj.new(i))
        return x
    
    def retrieve(self, obj, criteria=None):
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb[obj.collection]
#        print "db: ", self.db
#        print "coll: ", obj.collection
        
        
        if criteria:
#            print "retrieve crit: ", criteria
            if isinstance(criteria, dict):
#                print "crit is dict"
                spec = criteria
            else:
#                print "crit is not dict"
                spec = dict(criteria)
        else:
#            print "no crit supplied"
            spec = {}
            
#        print "spec: ", spec
        
        mob = coll.find_one(spec)
#        print "mob: ", mob
        return obj.new(mob)      
        
        
    def count(self, collectionName, criteria=None):
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb[collectionName]
        return coll.find(criteria).count()
        
    def _nextval(self, seqname, retries=100):
        # TODO: retries is gawky! must be a better way.
        conn = Connection(self.host, self.port)
        mdb = conn[self.db]
        coll = mdb.sequences

        obj = coll.find_one( {"seqname": seqname, "lastval" : { "$gte" : 0}} )

        # Create a new sequence if need be
#        if 0 == len(obj):
        if not obj:
            coll.save({"seqname": seqname, "lastval": 1})
#            obj = coll.find_one( {"seqname": seqname, "lastval" : { "$gte" : 1}} )
            return 1
        
        v = obj["lastval"]
        vnew = v
        r = 0
        while (1 and r < retries):
            vnew += 1
            coll.update( { "seqname": seqname, "lastval": v }, { "$set": {"lastval": vnew} } );
            rslt = mdb.command({"getlasterror":1})
            if rslt["updatedExisting"]:
                break
            r += 1
        
        return vnew


class Field(object):  
    def __init__(self, fldtype, name, required=False, default=None, validator=None, invalid_message=None): 
        """
        fldtype - dictionary values are of this type
        name    - just to correlate dictionary values w/ appr. validators, etc
        required - will be validated no matter what anybody else says.
        default - None = don't make this item at creation time, any other value
                  means make this item with this value at Thing creation time.
        validator - function to validate this field.  None means 'no validation'
        invalid_message - Human readable string.  This should go away (absorbed into validator)
        """
        self.fldtype = fldtype
        self.name = name
        self.required = required
        self.default = default
        self.validator = validator
        self.invalid_message = invalid_message
    
        self.error = None
     
    # May want to change semantics around this from a question to an imperative or provide both?   
    def is_valid(self, value):
        self.error = ""
        # TODO: Do the validation provided by self.validator and put result in self.error
        if value == None and self.required:
            if self.invalid_message:
                self.error = self.invalid_message
            else:
                self.error = '%s is required' % self.name
            
        # TODO: Type check?  Very likely a string value was used to make this object,
        #    we just want to be sure it can be coerced into self.type
        
        if self.validator:
            s = self.validator(self, value)
            if s:
                self.error += str(s)
        
        return 0 == len(self.error)
    
class ListField(Field):
    def __init__(self, type, name, **kwargs):
        super(ListField, self).__init__(type, name, **kwargs)
        

class AutoIncField(Field):
    def __init__(self, name, seqname, **kwargs):
        # remove or override "default" in **kwargs
        super(AutoIncField, self).__init__(int, name, **kwargs)
        self.seqname = seqname
        
class RefField(Field):
    def __init__(self, doctype, name, fld_type, lazy_load, **kwargs):
        super(RefField, self).__init__(type, name, **kwargs)
        self.doctype = doctype
        self.type = type
        self.lazy_load = lazy_load
        
class Doc(dict):
      
    mgr = None     
    collection = ""
    fields = []
    
    # Start with a simple { "field": "message" }
    _errors = {}

    def errors(self):
        return self._errors
    
    

    # Master validation routine
    def _validate(self):
        self._errors = {}
        
        # Make sure we have a valid Mgr
#    This can cause problems with NestedDocField types
#        if not isinstance(self.mgr, Mgr):
#            self._errors["mgr"] = "Not a valid Mgr"
            
        # run through the fields list calling individual validators
        for f in self.fields:
            if isinstance(f, NestedDocField):
                if not self.get(f.name).is_valid():
                    self._errors[f.name] = f.error
            
            elif not f.is_valid(self.get(f.name, None)):
                self._errors[f.name] = f.error
             
        # call any app specific validation if needed
        errs = self.validate(self)
        if errs:
            if isinstance(errs, list):
                for k, v in errs:
                    self._errors[k] = v
            else:
                self._errors[errs[0]] = errs[1]
    
    def is_valid(self):
        self._validate()
        return 0 == len(self._errors)
    
    
    # These can be overridden by subclasses to get app specific behavior
    def validate(self, item):
        """
        Return a dict from here!
        
        Anything returned here will be interpreted as failing validation
        and stored in the errors dict. 
        """
        pass

    def before_save(self, item): 
        return True
    def after_save(self, item):
        pass
    
    def before_del(self, item):
        pass
    def after_del(self, item):
        pass
    
    
    def save(self):
        """
        1. Perform validations
        2. Do any app defined pre-save stuff
        3. Actually save
        4. Do any app defined post-save stuff
        """
        
        self._validate()
        if 0 != len(self._errors):
            raise OCMInvalidException
        
        for f in self.fields:
            if isinstance(f, AutoIncField):
                if not self.has_key(f.name):
                    self[f.name] = self.mgr._nextval(f.seqname)
                    break
        
#         TODO: Figure out a way for before_save to 
        #        send a message back through the call stack
        #        via _errors?
        if self.before_save and not self.before_save(self):
            return False
        
        for f in self.fields:
            if isinstance(f, AutoIncField):
                if not self.has_key(f.name) or not self.get(f.name):
                    self[f.name] = self.mgr._nextval(f.seqname)
                    continue
        
        # would be good to pass the result of 
        # _mgr.save() to after_save!
        # Need to figure out what to toss around.
        if not self.mgr.save(self):
            return False
        
        if self.after_save:
            self.after_save(self)
            
        return 0 == len(self._errors)
#                o["_id"] = pymongo.objectid.ObjectId(o["_id"])
    def delete(self):
        return self.mgr.delete(self)
        
    @classmethod
    def remove(cls, criteria=None, confirmNoCriteria=None):
        print "remove ", criteria
        
    @classmethod
    def find(cls, criteria=None):
        crit = {}
        if criteria:
            crit = cls._makeNiceSpec(cls, criteria)
        
        return cls.mgr.get(cls, crit)
    
    @classmethod
    def retrieve(cls, criteria=None):
        crit = {}
        if criteria:
            crit = cls._makeNiceSpec(cls, criteria)
            
        return cls.mgr.retrieve(cls, crit)
        
    @staticmethod
    def _makeNiceSpec(obj, spec):
        ret = dict()
        if not isinstance(spec, dict):
            # TODO: Raise an exception?  Bad input.
            return ret
        
        ret.update(spec)
        
        d = dict((f.name, f) for f in obj.fields)
        for k, v in ret.iteritems():
            if k == "_id":
                ret["_id"] = pymongo.objectid.ObjectId(v)
            elif d.has_key(k):
#                for f in obj.fields:
#                    if k == f.name:
                ret[k] = d[k].fldtype(v)
        
        return ret
        
    @classmethod
    def count(cls, criteria=None):
        crit = {}
        if criteria:
            crit = cls._makeNiceSpec(cls, criteria)
            
        return cls.mgr.count(cls.collection, crit)

    @classmethod
    def diag(cls):
        print cls
        print cls.__dict__
        print cls.__bases__
        print cls.mgr
        print cls.collection

    @classmethod
    def new(cls, data=None):
        o = cls()
        if 0 == len(o.fields):
            raise OCMInvalidException
        
        # Everything in data goes into object.  Validate after.
        if data and isinstance(data, dict):
            o.update(data)
            # TODO: Be sure there's no Field name _id!
#            if o.has_key("_id"):
                
#            if o.has_key("status"):
#                pass
#                print o["_id"]


#                o["_id"] = pymongo.objectid.ObjectId(o["_id"])

        # 1. Be sure any fields that have a default value specified,
        #    but aren't in the incoming data get set with default
        # 2. Convert values to types specfied in fields list
        # 3. Validate!
        for f in o.fields:
            if isinstance(f, AutoIncField):
                if not o.has_key(f.name):
                    o[f.name] = None
                    continue

            if f.default and not o.has_key(f.name):
                o[f.name] = f.default

            if o.has_key(f.name):
                if isinstance(o[f.name], f.fldtype):
                    pass # do nothing -it's ok
                elif f.fldtype == int:
                    o[f.name] = int(float(o[f.name]))
                elif isinstance(f, ListOfDocsField):
#                    for x in range(len(o[f.name])):
#                        o[f.name][x] = f.fldtype(o[f.name][x])
                    # TODO: list comprehension!
                    l = []
                    for item in o[f.name]:  #.iteritems():
                        l.append(f.fldtype.new(item))
                    o[f.name] = l
                elif Doc in f.fldtype.__bases__:
                    o[f.name] = f.fldtype.new(o[f.name])
                else:
                    o[f.name] = f.fldtype(o[f.name])

            
        o.is_valid()
        return o          
    
    def __init__(self, *l, **kw):
        super(Doc, self).__init__(*l, **kw)

    def __getattr__(self, name):
        try:
            #  This only works when f.fldtype is what you want
            #   which is not what you want when you've got a:
            #       ListofDocsField(N, "a-name")
#            d = dict((f.name, f.fldtype ) for f in self.fields)
#            if d.has_key(name):
#                if Doc in d[name].__bases__:
#                    return d[name].new(self[name])
                
            d = dict((f.name, f) for f in self.fields)
            if d.has_key(name):
#                if ListField in d[name].__bases__:
#                    return list( (d[name].fldtype(x)) for x in self[name]  )
#                if Doc in d[name].fldtype.__bases__:
                
#                if isinstance(d[name], RefField):
#                    return d[name].doctype.retrieve({"_id": self[name]})
                
                if isinstance(d[name], Doc):
                    return d[name].fldtype.new(self[name])
                elif isinstance(d[name], ListField):
                    return list( (d[name].fldtype(x)) for x in self[name]  )
                
            return self[name]
        except KeyError:
            raise AttributeError, name
        
    def __setattr__(self, name, value):
        # This works nicely for saying fields []  is complete!
        # isn't there a more pythonic way of expressing this?
#        s = False
#        for field in self.fields:
#            if name == field.name:
#                s = True
#                break
        
        if (name in list((f.name) for f in self.fields)):
            self.__setitem__(name, value)
        else:
            super(Doc, self).__setattr__(name, value)


class NestedDocField(Field):
    def is_valid(self):
        self._errors = {}

            
        # run through the fields list calling individual validators
        for f in self.fields:
            if not f.is_valid(self.get(f.name, None)):
                self._errors[f.name] = f.error
             
        # call any app specific validation if needed
        errs = self.validate(self)
        if errs:
            if isinstance(errs, list):
                for k, v in errs:
                    self._errors[k] = v
            else:
                self._errors[errs[0]] = errs[1] 
                
        return 0 == len(self._errors)   
    
class ListOfDocsField(ListField):
    def __init__(self, type, name, **kwargs):
        # remove or override "default" in **kwargs
        super(ListOfDocsField, self).__init__(type, name, **kwargs)
        

#if __name__ == "__main__":
#    class M(Doc):
#        pass
#    #    err = {}
#    #    _blue = "bl"
#        _fields = ["red", "green", "blue"]
#        
#    #    def doit(self):
#    #        print "doneit"
#            
#    #    def newblue(self):
#    #        self.blue = "lb"
#    #        
#    #    def makeerr(self):
#    #        self.err = {"blah": "maybe"}
#    
#    t = M()
#    print "Plain t: ", t
#    
#    print "---- Set a field and see it in the dict - not a prop ---"
#    print """ ---- t.red = "rojo"  ---- """
#    t.red = "rojo"
#    print "t ", t
#    print "t.red ", t.red
#    try:
#        print "t['red'] ", t["red"]
#    except:
#        print "no can do"
#        
#    for k, v in t.iteritems():
#        print k, " = ", v
#    
#    print "---- Set a prop and keep it out of the dictionary ---"
#    print """ ---- t.blue = "azul"   ----"""
#    t.blue = "azul"
#    print "t ", t
#    print "t.blue ", t.blue
#    try:
#        print "t['blue'] ", t["blue"]
#    except:
#        print "no can do"
#        
#    for k, v in t.iteritems():
#        print k, " = ", v
#    
#    print "---- Call a func defined in subclass by parent ---"
#    print "t.parentaccess ", t.parentaccess()
#    print "     Now, define and set the callback and call it"
#    def f(item):
#        print "f ", item
#    t.doit = f
#    print "t.parentaccess ", t.parentaccess()
#    
#    
#    print "---- use a field with the same name as a property and get at both ---"
#    print """ ---- t.blue = "azul"   ----"""
#    t.blue = "azul"
#    t.setBlue("babyblue")
#    
#    print "t ", t
#    print "t.blue ", t.blue
#    try:
#        print "t['blue'] ", t["blue"]
#    except:
#        print "no can do"
#    print "t.getBlue ", t.getBlue()
#        
#    for k, v in t.iteritems():
#        print k, " = ", v
#    
#    print "---- Can we maintain state within an instance variable? ----"
#    print t.getErrors()
#    t.testInstVarManip()
#    print t.getErrors()
#    print t
#    
#    
#    
#    
#        
#    #o = M()
#    #print o
#    #print o.blue
#    #o.blue = "azul"
#    #print o
#    #print o.blue
#    #o.newblue()
#    #print o
#    #print o.blue
#    #o.red = "red"
#    #print o
#    #print o.red
#    #print o.blue
#    #o.makeerr()
#    #print o
#    #print o.blue
#    #print o.err
#    #o.parentaccess()
#    #
#    #for k, v in o.iteritems():
#    #    print k, ' = ', v
#    
#    
#    #
#    #o.blue = "blue"
#    #print o
#    #
#    #print o.blue