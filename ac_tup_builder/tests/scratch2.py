class Test(object):
    d = dict()

    def __init__(self):
        self.d = dict()


test1 = Test()
test2 = Test()

print('%s, %s'%(id(test1), id(test1.d)))

print('%s, %s'%(id(test2), id(test2.d)))