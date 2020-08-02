from multiprocessing import Pool
from typing import List
import subprocess

def run_shell_and_get_result(args: List[str])->str:
    # print(f'executing {" ".join(args)}')
    return subprocess.run(args, stdout=subprocess.PIPE).stdout.decode().strip()

def run_one_test(test_name: str, i: int, log_path: str, time_limit: str):
    # print(f'running test {i}...')
    output = run_shell_and_get_result(['go', 'test', '-run', test_name, '-timeout', time_limit])
    result = output.split('\n')[-1][:2]
    if result == 'ok':
        print(f'test {i} passed')
    else:
        print(f'test {i} failed')
        with open(f"{log_path}/debug_{i}.txt", 'w') as f:
            f.write(output)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="process core num and test time")
    parser.add_argument('test_name', type=str, help='the name of the test function')
    parser.add_argument('-np', dest='num_process', type=int, default=1, required=False, help='number of processes used')
    parser.add_argument('-t', dest='test_times', type=int, default=1, required=False, help='number of test times')
    parser.add_argument('-o', dest='debug_output', type=str, default='.', required=False, help='output directory path for debug log')
    parser.add_argument('-l', dest='limit', type=str, default='10m0s', required=False, help='time limit for test')
    args = parser.parse_args()
    p = Pool(args.num_process)
    test_name = args.test_name
    # print(f'test_name: {test_name}')
    p.starmap(run_one_test, [(test_name, i, args.debug_output, args.limit) for i in range(args.test_times)])