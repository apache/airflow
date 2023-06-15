"""
back to the “nonlocal” topic.  i noticed that you don’t need to use nonlocal when using sets.  so i was like wait why do we need the nonlocal keyword again?

and it has to do with reassignment, which is sort of a symptom of immutability.

so for example this doesn’t work
def main():
    counter = 0

    def add_num_not_ok(num):
        counter += 1
        print(counter)

    add_num_not_ok(1)
    print(counter)
counter+=1 is implicitly counter = counter + 1 and apparently python forces the right hand side counter in this scenario to be only local scope.

i guess what i would expect it to do is effectively this:
def main():
    counter = 0

    def add_num_ok(num):
        _counter = counter  # _counter is counter == True
        _counter += num  # now _counter is counter == False
        print(_counter)
    add_num_ok(1)
    print(counter)
    """

def main():
    counter = 1

    def add_num_ok(num):
        if num > 6:
            return
        _counter = counter
        to_add = add_num_ok(num+1)
        _counter += num + (to_add or 0)
        return _counter
    print(add_num_ok(1))
    print(counter)
0 + 1 + 2 + 3 + 4 + 5

main()
