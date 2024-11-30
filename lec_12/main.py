import time
import random

def measure_time(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        print(f"{func.__name__}: {end_time - start_time} second")
        return result
    return wrapper

def create_random_file(filename):
    if not os.path.exists(filename):  
        with open(filename, 'w') as file:
            for _ in range(100):
                line = ' '.join(str(random.randint(1, 100)) for _ in range(20)) + '\n'
                file.write(line)
        print(f"File {filename} created.")
    else:
        print(f"File {filename} already exists.")

@measure_time
def process_file(input_filename, output_filename):
    with open(input_filename, 'r') as infile:
        lines = infile.readlines()

    filtered_lines = []
    for line in lines:
        numbers = list(map(int, line.split()))
        filtered_numbers = list(filter(lambda x: x > 40, numbers))
        filtered_lines.append(filtered_numbers)

    with open(output_filename, 'w') as outfile:
        for line in filtered_lines:
            outfile.write(' '.join(map(str, line)) + '\n')

def read_file_generator(filename):
    with open(filename, 'r') as file:
        for line in file:
            yield list(map(int, line.split()))

input_filename = 'random_numbers.txt'
output_filename = 'result_numbers.txt'

create_random_file(input_filename)
process_file(input_filename, output_filename)
print(f"Results written to {output_filename}")

generator = read_file_generator(output_filename)
for line in generator:
    print(line)
    break
