def test_dic(a):
    a.update({'k2':(234919231039120)})

def test(b):
    b.append('123123121')

if __name__ == '__main__':
    rez = ({},{})
    a = rez[0]
    test_dic(a)
    b = rez[1]
    test_dic(b)

    a.update( {"test2":123})
    print(rez)