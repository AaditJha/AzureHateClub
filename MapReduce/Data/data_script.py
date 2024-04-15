import random
N = 100
with open("small_data.txt", 'w') as f:
    for _ in range(N):
        for __ in range(10):
            f.write(f"{(random.random() - 0.5) * N} ")
        f.write("\n")
        