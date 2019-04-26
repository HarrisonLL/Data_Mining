from heapq import heappush as hppush
from heapq import heappop as hppop
import math
###get input
dataset = {}
N = 0  ##numbers of data
k = 0  ##numbers of clusters
num_attr = 0
lineidx = 0
# while True:
#     try:
#         if lineidx == 0 :
#             s = input().split(" ")
#             N = int(s[0])
#             K = int(s[1])
#         else: 
#             s = input().split(" ")
#             temp_list = []
#             for i in range(len(s)) :
#                 if (s[i]) == "": break
#                 value = float(s[i])
#                 temp_list.append(value)  
#             dataset[lineidx -1] = temp_list
#         lineidx += 1
#     except EOFError as error:
#         break
# num_attr = len(dataset[0])

######## fot local testing #######
with open('./cs412_personal_git/HW4_test.txt','r') as f:
    for line in f:
        if lineidx == 0 :
            s = line.split(" ")
            N = int(s[0])
            K = int(s[1])
        else: 
            s = line.split(" ")
            temp_list = []
            for i in range(len(s)) :
                if (s[i]) == "": break
                value = float(s[i])
                temp_list.append(value)  
            dataset[lineidx -1] = temp_list
        lineidx += 1
num_attr = len(dataset[0])


def distance(point1, point2) :
    distance = 0.0
    for i in range (len(point1)) :
        distance += (point1[i]-point2[i])**2
    return math.sqrt(distance)

def initialize_heap(dataset) :
    heap_list = []
    for i in range(len(dataset)) :
        for j in range(i+1, len(dataset)) :
            dist = distance(dataset[i], dataset[j])
            hppush(heap_list, (dist, [i,j]))
    print(heap_list)
    return heap_list

def hierarchical_clustering(dataset, N, K) :
    the_heap = initialize_heap(dataset)
    print(the_heap)
    clusters = []
    i = 0
    while i < (N-K) :
        cur_dist, pair = hppop(the_heap)
        idx1 = pair[0]
        idx2 = pair[1]
        ##assign to clusters
        assigned_to_cluster = [-1, -1]
        for j in range(len(clusters)) :
            if assigned_to_cluster[0] != -1 and assigned_to_cluster[1] != -1: break
            if idx1 in clusters[j] : assigned_to_cluster[0] = j
            if idx2 in clusters[j] : assigned_to_cluster[1] = j

        if assigned_to_cluster[0] == -1 and assigned_to_cluster[1] == -1:
            clusters.append([idx1, idx2])
        elif assigned_to_cluster[0] != -1 and assigned_to_cluster[1] == -1:
            clusters[assigned_to_cluster[0]].append(idx2)
            sorted(clusters[assigned_to_cluster[0]])
        elif assigned_to_cluster[0] == -1 and assigned_to_cluster[1] != -1:
            clusters[assigned_to_cluster[1]].append(idx1)
            sorted(clusters[assigned_to_cluster[1]])
        else:
            if assigned_to_cluster[0] == assigned_to_cluster[1] : i-=1
            else :
                min_idx = min(assigned_to_cluster)
                max_idx = max(assigned_to_cluster)
                clusters[min_idx] = sorted(clusters[min_idx] + clusters[max_idx])
                del clusters[max_idx]
        i += 1
    for i in range(len(clusters)) :
        clusters[i] = sorted(clusters[i])
    return clusters

clusters = hierarchical_clustering(dataset, N, K)

         
