import random
N = 100
with open("/Users/pr0hum/Documents/JalebiBoi/AzureHateClub/MapReduce/Data/Input/small_data.txt", 'w') as f:
    for _ in range(N):
        f.write(f"{(random.randint(-5, 5))}, ")
        f.write(f"{(random.randint(-5, 5))}")
        f.write("\n")
        