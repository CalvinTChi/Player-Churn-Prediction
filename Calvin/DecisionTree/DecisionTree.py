import copy
import math
from collections import Counter
import pandas as pd
import numpy as np
import sys
pd.options.mode.chained_assignment = None

class DecisionTree:

    def __init__(self, method="simple_tree", num_trees=None, stop=0.3, 
    minSize=10, subset=None, output=False):
        self.method = method
        self.num_trees = num_trees
        self.stop = stop
        self.minSize = minSize
        self.subset = subset
        self.output = output
        self.tree = None
        self.attributes = None

    def train(self, data, labels):
        def simple_tree(data, labels):
            '''Method for building a simple decision tree'''
            attributes = copy.deepcopy(data.columns.values.tolist())
            data['labels'] = pd.Series(labels, index=data.index)
            self.classes = data.ix[:, 'labels'].unique()
            tree = simple_tree_helper(self, data, attributes)
            return tree

        def simple_tree_helper(self, data, attributes, random=False):
            '''Recursive method that builds the decision tree'''
            if data.shape[0] > 0 and len(attributes) > 0: 
                # If all the labels in this group are the same, 
                # then we are done and return the majority label
                if len(data["labels"].unique()) == 1:
                    return int(data["labels"].values[0])
                # Else if the group entropy is less than p% of the original 
                elif (calculate_entropy(data["labels"].values) < 
                    self.stop * math.log(len(self.classes), 2)): 
                    return majority(data["labels"].values)
                elif (data.shape[0] < self.minSize):
                    return majority(data["labels"].values)
                else:
                    # Tree decorrelation step of random forest: random choose 
                    # a fraction of attributes to choose from
                    if random:
                        indices = np.random.choice(len(attributes), 
                            size=math.ceil(self.subset*len(attributes)), replace=False)
                        subset = [attributes[i] for i in indices]
                        bestAttribute, splitVal = choose_attribute(data, subset)
                    else:
                        bestAttribute, splitVal = choose_attribute(data, attributes)
                        if self.output:
                            self.attributes.append(bestAttribute)
                    attributes.remove(bestAttribute)
                    if isinstance(splitVal, list):
                        left = data[data[bestAttribute].isin(splitVal[0])]
                        right = data[data[bestAttribute].isin(splitVal[1])]
                    else:
                        left = data[data[bestAttribute] <= splitVal]
                        right = data[data[bestAttribute] > splitVal]
                    tree = BinaryTree((bestAttribute, splitVal))
                    if len(left) == 0 or len(right) == 0:
                        return majority(data["labels"].values)
                    tree.insertLeft(simple_tree_helper(self, left, attributes))
                    tree.insertRight(simple_tree_helper(self, right, attributes))
                    return tree
            elif data.shape[0] > 0 and len(attributes) == 0:
                return majority(data["labels"].values)
            else:
                return None

        def random_forest(data, labels):
            '''Method for building a random forest'''
            trees = []
            attributes = copy.deepcopy(data.columns.values.tolist())
            attributesTemp = copy.deepcopy(attributes)
            data['labels'] = pd.Series(labels, index=data.index)
            self.classes = data.ix[:, 'labels'].unique()
            for i in range(self.num_trees):
                # Bagging: randomly picking 2/3 of the data for making a simple 
                # decision tree
                baggingIdx = np.random.choice(data.shape[0], size=data.shape[0], 
                    replace=True)
                bag = data.iloc[baggingIdx, :]
                tree = simple_tree_helper(self, bag, attributesTemp, True)
                trees.append(tree)
                attributesTemp = copy.deepcopy(attributes)
            return trees

        # @ input: data frame, available attributes
        # @ output: attribute and attribute value to split on that maximizes difference 
        # between parent entropy and 
        #   average children entropy
        def choose_attribute(data, attributes):
            bestGain = 0
            bestAttribute = None
            bestSplit = None
            parentEntropy = calculate_entropy(data.ix[:, "labels"].values)
            for attribute in attributes:
                if len(data[attribute].unique()) == 1:
                    gain = 0
                    bestAttribute = attribute
                    bestSplit = data[attribute].values[0]
                else:
                    gain, splitVal = calculate_gain(data.ix[:, [attribute, "labels"]], 
                        attribute, parentEntropy)
                    if gain >= bestGain:
                        bestGain = gain
                        bestSplit = splitVal
                        bestAttribute = attribute
            return bestAttribute, bestSplit

        # @ input: data frame, selected attribute, parent entropy
        # @ output: best entropy gain and best split for that attribute
        def calculate_gain(data, attribute, parentEntropy):
            total = data.shape[0]
            minEntropy = float("inf")
            splitVal = None
            bestLeft = None
            bestRight = None
            # If attribute is categorical, sort attribute value by class proportion for 
            # that attribute value, and choose best split point going from least class 
            # proportion to greatest class proportion
            if type(data[attribute].values[0]) == str: 
                label = self.classes[0]
                n = len(data[data["labels"] == label])
                attributeVals = data[attribute].unique()
                proportions = [(val, data[(data[attribute] == val) & 
                    (data["labels"] == label)].shape[0] / n) for val in attributeVals]
                proportions = sorted(proportions, key=lambda x: x[1])
                if len(proportions) == 1:
                    leftLabels = data["labels"].values
                    minEntropy = calculate_entropy(leftLabels)
                    splitVal = [[proportions[0][0]], []]
                else: 
                    for i in range(1, len(proportions)):
                        leftVal = [tup[0] for tup in proportions[:i]]
                        rightVal = [tup[0] for tup in proportions[i:len(proportions)]]
                        leftLabels = data[data[attribute].isin(leftVal)]["labels"].values
                        rightLabels = data[data[attribute].isin(rightVal)]["labels"].values
                        leftEntropy = calculate_entropy(leftLabels)
                        rightEntropy = calculate_entropy(rightLabels)
                        avgEntropy = ((len(leftLabels) / total) * leftEntropy 
                            + (len(rightLabels) / total) * rightEntropy)
                        if avgEntropy < minEntropy:
                            minEntropy = avgEntropy
                            splitVal = [leftVal, rightVal]
                            bestLeft = leftEntropy
                            bestRight = rightEntropy
            # If attribute is continuous, sort attribute value, then choose best split 
            # point from least to greatest only from points corresponding to class 
            # label change.
            else: 
                data = data.sort_values(attribute, axis=0)
                diff = data["labels"].diff(1).values
                idx = np.where(diff == -1)[0] - 1
                if len(idx) > 50:
                    step = len(idx) // 50
                    idxindex = list(range(0, len(idx) + step, step))
                    idxindex = idxindex[:len(idxindex) - 1]
                    idx = idx[idxindex]
                if len(idx) == 0:
                    minEntropy = calculate_entropy(data["labels"].values)
                    splitVal = data[attribute].values[0]
                else: 
                    attributeVals = data.iloc[idx, :][attribute].unique()
                    if len(attributeVals) > 1:
                        end = len(attributeVals) - 1
                    else:
                        end = 1
                    for val in attributeVals[:end]:
                        leftLabels = data[data[attribute] <= val]["labels"].values
                        rightLabels = data[data[attribute] > val]["labels"].values
                        leftEntropy = calculate_entropy(leftLabels)
                        rightEntropy = calculate_entropy(rightLabels)
                        if rightEntropy == None:
                            avgEntropy = leftEntropy
                        else:
                            avgEntropy = ((len(leftLabels) / total) * leftEntropy + 
                                (len(rightLabels) / total) * rightEntropy)
                        if avgEntropy < minEntropy:
                            minEntropy = avgEntropy
                            splitVal = val
            return parentEntropy - minEntropy, splitVal

        def calculate_entropy(data):
            '''Calculate entropy of given list of class labels'''
            if not isinstance(data, np.ndarray):
                data = np.array(data)
            n = len(data)
            if n == 0:
                return None
            entropy = 0.0
            for label in self.classes:
                subset = np.where(data == label)[0]
                p = len(subset)/n + 1e-6
                entropy -= p * math.log(p, 2)
            return entropy

        def majority(labels):
            '''Returns the majority label'''
            a = Counter(labels)
            vote = a.most_common(1)[0][0]
            return int(vote)

        class BinaryTree:
            def __init__(self, decision):
                self.decision = decision
                self.leftChild = None
                self.rightChild = None
            def insertLeft(self, newNode):
                self.leftChild = newNode
            def insertRight(self, newNode):
                self.rightChild = newNode
            def getRightChild(self):
                return self.rightChild
            def getLeftChild(self):
                return self.leftChild
            def setNode(self, decision):
                self.decision = decision
            def getNode(self):
                return self.decision
        data = copy.deepcopy(data)
        labels = copy.deepcopy(labels)
        self.attributes = []
        if self.method == "random_forest": 
            trees = random_forest(data, labels)
            self.tree = trees
        elif self.method == "simple_tree": 
            tree = simple_tree(data, labels)
            self.tree = tree

    def predict(self, dataset):
        def transverse_classify(tree, sample):
            if isinstance(tree, int):
                return int(tree)
            else:
                decision = tree.getNode()
                attribute = decision[0]
                splitVal = decision[1]
                sampleVal = sample[attribute]
                if isinstance(splitVal, list):
                    if sampleVal in splitVal[0]:
                        tree = tree.getLeftChild()
                    else:
                        tree = tree.getRightChild()
                else:
                    if sampleVal <= splitVal:
                        tree = tree.getLeftChild()
                    else:
                        tree = tree.getRightChild()
                return transverse_classify(tree, sample)

        def majority(labels):
            '''Returns the majority label'''
            a = Counter(labels)
            vote = a.most_common(1)[0][0]
            return int(vote)

        tree = self.tree
        predictions = []
        if self.method == "simple_tree":
            for rownum in range(dataset.shape[0]):
                predict = transverse_classify(tree, dataset.iloc[rownum, :])
                predictions.append(predict)
        # In random forest, the majority vote among all trees is the prediction
        elif self.method == "random_forest":
            for rownum in range(dataset.shape[0]):
                ballot = []
                for t in tree:
                    predict = transverse_classify(t, dataset.iloc[rownum, :])
                    ballot.append(predict)
                vote = majority(ballot)
                predictions.append(vote)
        return predictions
