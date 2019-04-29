import math

##get input
s = input().split()
N = int(s[0])
K = int(s[1])
dataset = {}
for i in range(N) :
    k = str(i)
    s = input().split()
    dataset[k] = s
    for j in range(len(s)) :
        dataset[k][j] = float(dataset[k][j])

def distance(data1, data2) :
    sum = 0.0
    for i in range(len(data1)) :
        sum += (data1[i] - data2[i])**2
    return math.sqrt(sum)
    

def initialize_dm(dataset) :
    the_dict = {}
    for i in range(N) :
        for j in range(N) :
            if i != j :
                key1 = str(i)
                key2 = str(j)
                value = distance(dataset[key1], dataset[key2])
                if key1 not in the_dict : the_dict[key1] = {}
                the_dict[key1][key2] = value
    return the_dict

def update_dm(the_dict) :
    
    while len(the_dict) > K :
        min_dist = math.inf
        key1 = '0'
        key2 = '0'
        for k,v in the_dict.items():
            for k2, v2 in v.items():
                if v2 - min_dist < -1*10**-7 :
                    key1 = k
                    key2 = k2
                    min_dist = v2

                elif math.fabs(v2-min_dist) <= 10**-7 :
                    temp0 = k.split()
                    temp1 = k2.split()
                    temp0.sort(key=float)
                    temp1.sort(key=float)
                    pair1 = [temp0[0], temp1[0]]
                    pair1.sort(key=float)
                    temp0 = key1.split()
                    temp1 = key2.split()
                    temp0.sort(key=float)
                    temp1.sort(key=float)
                    pair2 = [temp0[0], temp1[0]]
                    pair2.sort(key=float)
                    if pair1[0] < pair2[0] :
                        key1 = k
                        key2 = k2
                    elif pair1[0] == pair2[0] and pair1[1] < pair2[1] :
                        key1 = k
                        key2 = k2
        new_key = key1 + ' ' + key2
        the_dict[new_key] = {}
        for k,v in the_dict[key1].items() :
            if k != key1 and k!= key2 :
                v2 = the_dict[key2][k]
                the_dict[new_key][k] = min(v,v2)
        del(the_dict[key1])
        del(the_dict[key2])
        for k,v in the_dict.items() :
            if k != new_key :
                del(v[key1])
                del(v[key2])
                v[new_key] = the_dict[new_key][k]
    return the_dict

the_dict = initialize_dm(dataset)
result = update_dm(the_dict)
# print(result)
# print()
cluster = {}
for k,v in the_dict.items() :
    key_list = k.split()
    key_list.sort(key=float)
    for i in range(len(key_list)) :
        cluster[key_list[i]] = key_list[0]
# print(cluster)
keys = list(cluster.keys())
keys.sort(key=float)
for i in range(len(keys)):
    print(cluster[keys[i]])
