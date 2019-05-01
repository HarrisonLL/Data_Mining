import math
import statistics as stats

#get input
s = input().split()
N = int(s[0])
K = int(s[1])
dataset = {}
cluster_center = []
dim = 0

for i in range(N) :
    s = input().split()
    dataset[i] = s
    dim = len(s)
    for j in range(dim) :
        dataset[i][j] = float(dataset[i][j])
for i in range(K) :
    s = input().split()
    cluster_center.append(list(map(float, s)))


def distance(data1, data2) :
    sum = 0.0
    for i in range(len(data1)) :
        sum += (data1[i] - data2[i])**2
    return math.sqrt(sum)


def update_center(assigned_labels, cluster_center):
    new_center = []
    temp_set = set(sorted(assigned_labels))
    for idx in temp_set:
        temp_center = []
        for j in range(dim) :
            temp_sum = []
            for k in range(len(assigned_labels)) :
                if assigned_labels[k] == idx :
                    temp_sum.append(dataset[k][j])
            temp_mean = stats.mean(temp_sum)  
            temp_center.append(temp_mean)
        new_center.append(temp_center)
        
    return new_center


labels = []
while True :
    assigned_labels = []
    for i in range(len(dataset)) :
        dists = {} ##key is idx of cc; value is distance
        for j in range(len(cluster_center)) :
            dist = distance(dataset[i], cluster_center[j])
            dists[j] =[dist]
        assigned_label = sorted(dists.items(), key=lambda x:(x[1], x[0]))[0][0]
        assigned_labels.append(assigned_label)
    ##check
    if len(labels) != 0 and labels[len(labels)-1] == assigned_labels : break
    labels.append(assigned_labels)
    ##update center
    cluster_center = update_center(assigned_labels, cluster_center)

for i in range(len(labels[len(labels)-1])) :
    print(labels[len(labels)-1][i])

