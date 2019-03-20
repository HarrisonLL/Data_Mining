from itertools import product

##Get input
b =[]
while True:
    try:
        s=input()
        b.append(s)
    except EOFError as error:
        break
        
##variable declaration
min_sup = 2
min_length = 2
max_length = 5        

Uk = {}
##first get singleton sequence
## format:  'word':[support, list of sid, list of eids...]

Fk = {}
for i in range(len(b)) :
    items = b[i].split()
    for j in range(len(items)) :
        item_ = items[j]
        if item_ not in Fk: 
            Fk[item_] = [1]
            Fk[item_].append([i])
            Fk[item_].append([j])
        else :
            Fk[item_][0] += 1
            Fk[item_][1].append(i)
            Fk[item_][2].append(j)

Fki = Fk.copy()
# print(Fk)
# print()
# print(len(Fk))
#count=0

for k in range(2, 6) :
    Uki_ = {}
    for s in product(Fki, Fk) :
        ##not same line: continue
        start_dict = ''
        if Fki[s[0]][0] <= Fk[s[1]][0] :  start_dict = 'Fki'
        else : start_dict = 'Fk'

        break_to_next = True
        if start_dict == 'Fki' :
            for elem in Fki[s[0]][1] :
                if elem in Fk[s[1]][1] :
                    break_to_next = False
                    break        
        else :
            for elem in Fk[s[1]][1] :
                if elem in Fki[s[0]][1] :
                    break_to_next = False
                    break
        if break_to_next : continue
 
       
        ##get index list
        idx_list1 = []
        idx_list2 = []
        for i in range(len(Fki[s[0]][1])) :
            for j in range(len(Fk[s[1]][1])) :
                if Fki[s[0]][1][i] == Fk[s[1]][1][j] : 
                    idx_list1.append(i)
                    idx_list2.append(j)

        ##compare
        Cki = {}
        new_idx_list1 = []
        new_idx_list2 = []
        for i in range(len(idx_list1)) :
            idx1 = idx_list1[i]
            idx2 = idx_list2[i]            
            the_idx1 = len(Fki[s[0]])-1
            the_idx2 = len(Fk[s[1]])-1
            eid_1 = Fki[s[0]][the_idx1][idx1]
            eid_2 = Fk[s[1]][the_idx2][idx2]
            if eid_2 - eid_1 == 1:
                new_idx_list1.append(idx1)
                new_idx_list2.append(idx2)
                     
        if len(new_idx_list1) != 0 : 
            support = len(new_idx_list1)
            new_sequence = ' '.join([s[0],s[1]])
            Cki[new_sequence] = [support]
            for i in range(1,len(Fki[s[0]])) :
                for j in range(len(new_idx_list1)) :
                    if j == 0:
                        Cki[new_sequence].append([Fki[s[0]][i][new_idx_list1[j]]])
                    else :
                        Cki[new_sequence][i].append(Fki[s[0]][i][new_idx_list1[j]])
            the_idx3 = len(Fk[s[1]])-1
            for i in range(len(new_idx_list2)) :
                if i == 0 :
                    Cki[new_sequence].append([Fk[s[1]][the_idx3][new_idx_list2[i]]])
                else :
                    temp_idx = len(Cki[new_sequence]) - 1 
                    Cki[new_sequence][temp_idx].append(Fk[s[1]][the_idx3][new_idx_list2[i]])
    
        Cki_ = {k:v for k,v in Cki.items() if v[0]>= min_sup} 
        Uki_.update(Cki_)
        
    Fki = Uki_.copy()
    Uk.update(Fki)

      
#print(Uk)                    
     
Uk_ = {}
for k, v in Uk.items() :
    Uk_[k] = v[0]

sorted_list = [v[0] for v in sorted(Uk_.items(), key=lambda kv:(-kv[1],kv[0]))]
for item in sorted_list :
    print("[" + str(Uk_[item]) + ", " + "'" + item + "'" +"]")
    