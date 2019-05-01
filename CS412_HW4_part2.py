import math
import statistics as stats

#get input
s = input().split()
N = int(s[0])
K = int(s[1])
dim = 0
dataset = {}
cluster_center = {} ##key is cluster id, string; value is center coords
for i in range(N) :
    k = str(i)
    s = input().split()
    dataset[k] = s
    dim = len(s)
    for j in range(dim) :
        dataset[k][j] = float(dataset[k][j])
    if i < K :
        cluster_center[k]= dataset[k]

               
def distance(data1, data2) :
    sum = 0.0
    for i in range(len(data1)) :
        sum += (data1[i] - data2[i])**2
    return math.sqrt(sum)

def update_center(new_key) :
    new_center = []
    key = new_key.split()
    for i in range(dim) :
        temp1 = []
        for j in range(len(key)) :
            temp1.append(dataset[key[j]][i])
        mean = stats.mean(temp1)
        new_center.append(mean)
    return new_center
            
    
count = 0
while count < len(dataset) :

    for i in range(len(dataset)) :
        dist_dict = {}
        for k,v in cluster_center.items() :
            dist = distance(dataset[str(i)], v)
            dist_dict[k] = dist
        assigned_label = sorted(dist_dict.items(), key=lambda x:(x[1], x[0]))[0][0]

        if str(i) in assigned_label :   
            count += 1
            continue      
        else :
            count = 0
            new_key = assigned_label + ' ' + str(i)
            cluster_center[new_key] = cluster_center[assigned_label]
            del cluster_center[assigned_label]
            
    for k,v in cluster_center.items():
        new_v = update_center(k)
        cluster_center[k] = new_v

print(cluster_center)
result = [-1]*N
for key in cluster_center.keys() :
    cluster = key.split()
    label = min(cluster)
    for k in cluster:
        j = int(k)
        result[j] = label
for i in range(len(result)) :
    print(result[i])
        
                    
    
