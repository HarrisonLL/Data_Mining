data = []
while True:
    try:
        s=input()
        data.append(s.split( " "))
    except EOFError as error:
        break

min_sup = 2
      
    
    
    
def candidate_generation(the_dict) :
    candidates = {}
    for k,v in the_dict.items() :
        for i in range(len(v)) :
            sentence = data[v[i][0]]
            if v[i][1] + 1 < len(sentence) : 
                new_sequence = ' '.join([k, sentence[v[i][1]+1]])
                if new_sequence not in candidates : candidates[new_sequence] = [(v[i][0],v[i][1]+1)]
                else: candidates[new_sequence].append((v[i][0],v[i][1]+1))             
    return candidates

#1st frequent sequence
#data structure: 'word' : [(,), (,) .. ]
org_dict = {}
for i in range(len(data)) :
    for j in range(len(data[i])) :
        if data[i][j] not in org_dict :
            org_dict[data[i][j]]=[(i,j)]
        else :
            org_dict[data[i][j]].append((i,j))
org_dict = {k:v for k,v in org_dict.items() if len(v)>= min_sup}      

#loop through length-2 to length-5 sequence
the_dict = org_dict.copy()
Uk = []
for i in range(2,6) :
    candidates = candidate_generation(the_dict)
    if not candidates : break
    candidates = {k:v for k,v in candidates.items() if len(v)>= min_sup} 
    for k,v in candidates.items():
        Uk.append((k, len(v) ))
    the_dict = candidates.copy()
    

x = sorted(Uk, key=lambda element: (element[1]*-1,element[0]))[:20]
for i in range(len(x)):
    if i >=100:
        break
    print('[' + str(x[i][1])+', \'' + x[i][0]+'\']')