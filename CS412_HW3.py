import math
 
## dict: key is line index, value is "label attr0:value attr1:value.."
train_data = {} 
test_data = {}
idx = 0
while True:
    try:
        s=input()
        if (s.split(" ")[0] != "0"): 
            train_data[idx] = s.split(" ")
            idx += 1
        else :
            test_data[idx-len(train_data)] = s.split(" ")
            idx += 1           
    except EOFError as error:
        break

temp0 = []
for k,v in train_data.items() :
    temp0.append(v[0])
temp0 = list(set(temp0))

num_label = len(temp0)
num_train = len(train_data)
num_test = len(test_data)
num_atts = len(train_data[0])-1       
        
# print(num_atts)
# print(num_test)
# print(num_train)
# print(num_label)

#######################decision tree#########################

class Node:

    def __init__(self, the_dataset, the_depth):
        self.left = None
        self.right = None
        self.dataset = the_dataset
        self.attr_idx = 1000  #default value: when node is leaf 
        self.threshold = 1000 #default value: when node is leaf
        self.depth = the_depth
        
    def get_tree_height(self, root) :
        if root == None: return -1
        return (1+max(self.depth(root.left), self.depth(root.right)))
    
    
    # Print the tree IN ORDER TRAVERSAL
    
    def PrintTree(self):
        if self.left:
            self.left.PrintTree()
        print( self.dataset),
        if self.right:
            self.right.PrintTree()


 
    
def get_gini(less_than_idx, greater_than_idx) :
    coef1 = float(len(less_than_idx)/(len(less_than_idx)+len(greater_than_idx)))
    coef2 = float(len(greater_than_idx)/(len(less_than_idx)+len(greater_than_idx)))
    gini1 = 1
    gini2 = 1
    temp_labels = {}
    for idx in less_than_idx:
        label_ = train_data[idx][0] 
        if label_ not in temp_labels: temp_labels[label_] = 1
        else: temp_labels[label_] += 1
    for k,v in temp_labels.items() :
        gini1 -= (float(v/len(less_than_idx)))**2
    
    temp_labels1 = {}
    for idx in greater_than_idx :
        label_ = train_data[idx][0] 
        if label_ not in temp_labels1: temp_labels1[label_] = 1
        else: temp_labels1[label_] += 1
 
    for k,v in temp_labels1.items() :
        gini2 -=  (float(v/len(greater_than_idx)))**2
    # print(gini1 )
    # print(gini2)
    return (coef1*gini1 + coef2*gini2)


def list_of_labels(dataset) :
    labels = []
    for k,v in dataset.items() :
        if v[0] not in labels :
            labels.append(v[0])
    return labels 


def build_DT(node) :
    if node.depth == 2 or len(list_of_labels(node.dataset))==1: return node
    
    else :
        
        attribute_candidates = {}
        data = node.dataset
        ##find best attribute w/ minimum gini
        ##for each attribute, find possible split 
        ##for each split, find the one w/ minimum gini
        for i in range(1, num_atts+1) :
            values = {}
            unique_values = []
            possible_splits = []
            min_gini = 100
            threshold = 100
            best_left_idx = []
            best_right_idx = []
            ## find possible splits
            for k,v in data.items() :
                a = float(v[i].split(":")[1])
                values[k] = a
                unique_values.append(a)
            unique_values = list(set(unique_values))
            j = 0
            while j+1 < len(unique_values):
                split_ = float((unique_values[j]+unique_values[j+1])/2)
                possible_splits.append(split_)
                j += 1
 
            ## min_gin, best_split
            for split_ in possible_splits :
                less_than_idx = [k for k,v in values.items() if v<split_]
                greater_than_idx = [k for k,v in values.items() if v>=split_]
                gini_ = get_gini(less_than_idx, greater_than_idx)
                if gini_ < min_gini :
                    min_gini = gini_
                    threshold = split_
                    best_left_idx = less_than_idx.copy()
                    best_right_idx = greater_than_idx.copy()
            attribute_candidates[i] = [min_gini, best_left_idx, best_right_idx, threshold]

        true_attr = sorted(attribute_candidates.items(), key=lambda x:(x[1][0],x[0]))[0][0]
        left_data = {idx: data[idx] for idx in attribute_candidates[true_attr][1] }
        right_data = {idx: data[idx] for idx in attribute_candidates[true_attr][2] }
        # print(left_data)
        # print(right_data)
        # print()
        left_node = Node(left_data, node.depth + 1)
        right_node = Node(right_data, node.depth + 1)
        node.left = left_node
        node.right = right_node
        node.attr_idx = true_attr
        node.threshold = attribute_candidates[true_attr][3] 
        build_DT(left_node)
        build_DT(right_node)
        

root = Node(train_data, 0)
build_DT(root)
#root.PrintTree()
    

def test(node, test_data) :
    predicted_labels = []
    
    for k,v in test_data.items() :
        while True :
            attr_idx = node.attr_idx
            threshold = node.threshold
            if attr_idx == 1000 or threshold == 1000: break
            value = float(v[attr_idx].split(":")[1])
            if value < threshold : node = node.left
            else : node = node.right        
        label = list_of_labels(node.dataset)[0]
        predicted_labels.append(label)
    return predicted_labels

predicted_labels = test(root, test_data)
print(predicted_labels)
    
