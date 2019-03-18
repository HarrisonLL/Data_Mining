
from itertools import combinations

##Get input
b =[]
while True:
    try:
        s=input()
        b.append(s)
    except EOFError as error:
        break

# function  has_infrequent_subset
def  has_infrequent_subset(c, Fkm1) :
    num_elem = c.count(' ') + 1
    elems = c.split()
    all_combs = []
    
    for comb in combinations(elems, num_elem-1) :
        all_combs.append(' '.join(comb))

    for comb in all_combs :
        if comb not in Fkm1:
            return True
    return False
    
 
    
    
## first get frequent 1-itemset   
Ck = {}
Fk = {}
min_sup = int(b[0])
for i in range(1, len(b)) :
    elems = b[i].split()
    for elem in elems :
        if elem in Ck: Ck[elem] += 1
        else : Ck[elem] = 1
Fk = {k:v for k,v in Ck.items() if v>= min_sup}
Uk = {}
Uk.update(Fk)
while not len(Fk)==0:
    ##generate candidates 
    Ckp1 = {}
    for p in Fk:
        for q in Fk:
            p1 = p.split()
            q1 = q.split()
            qualify = False
            if len(p1) == 1:
                if (p1[0] < q1[0]) : qualify = True
            else:
                if p1[len(p1)-1] < q1[len(q1)-1] : qualify = True
                if qualify : 
                    for i in range(len(p1)-1) :
                        if p1[i] != q1[i] : 
                            qualify = False
                            break

            # print('p1[0]' + str(p1[0]))
            # print('q1[0]' + str(q1[0]))
            
            if qualify:
                c = ' '
                if len(p1) == 1 : 
                    c = ' '.join([p1[0], q1[0]])
                else : 
                    c = ' '.join(p1[:-1]) + ' ' + ' '.join([p1[len(p1)-1], q1[len(q1)-1]])
                                    
                if has_infrequent_subset(c, Fk): 
                    continue
                else:
                    d = c.split()
                    for j in range(1, len(b)):
                        e = b[j].split()
                        for i in range(len(d)) :
                            if d[i] not in e: break
                            elif (i == len(d)-1) and (c not in Ckp1) : Ckp1[c] = 1
                            elif (i == len(d)-1) and (c in Ckp1): Ckp1[c] += 1
        ##update Fk
    Fk = {k:v for k,v in Ckp1.items() if v>= min_sup}      
    Uk.update(Fk)  
    
sorted_list = [v[0] for v in sorted(Uk.items(), key=lambda kv:(-kv[1],kv[0]))]
for item in sorted_list :
    print (str(Uk[item]) + " " + "[" + item + "]")
    
#################——————————closed_pattern——————————————#####################

closed_pattern_list = []     
not_closed = []             ##找包含它的集合

for item in sorted_list :
    Uk_copy = Uk.copy()
    Uk_copy.pop(item, None)
    item_split = item.split()
    for key in Uk_copy:
        for i in range(len(item_split)):
            if item_split[i] not in key: break
            elif (i==(len(item_split)-1)) and (Uk[item] <= Uk[key]) : not_closed.append(item)

for item in sorted_list:
    if item not in not_closed: closed_pattern_list.append(item)
print()
for item in closed_pattern_list:
    print (str(Uk[item]) + " " + "[" + item + "]")
    
    
#############——————————max_pattern——————————########################

max_pattern_list = []     
not_max = []             ##找包含它的集合

for item in sorted_list :
    Uk_copy = Uk.copy()
    Uk_copy.pop(item, None)
    item_split = item.split()
    for key in Uk_copy:
        for i in range(len(item_split)):
            if item_split[i] not in key: break
            elif (i==(len(item_split)-1)) : not_max.append(item)

for item in sorted_list:
    if item not in not_max: max_pattern_list.append(item)
print()
for item in max_pattern_list:
    print (str(Uk[item]) + " " + "[" + item + "]")
    

