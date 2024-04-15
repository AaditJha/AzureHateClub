import random
import numpy as np

def gen_centroids(dim:int) -> list[int]:
    return [(random.random() - 0.5) * 100_000 for _ in range(dim)]

def euclidean_dist(point:list[int], centroid:list[int]) -> float:
    return ((np.array(point) -  np.array(centroid)) ** 2).sum() ** 0.5
