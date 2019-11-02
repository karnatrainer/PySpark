from contexts import create_contexts

def wordcount():
    sc = create_contexts()
    r1 = sc.range(1,10)
    print(r1.collect())
    
    
if __name__ =='__main__':
    wordcount()