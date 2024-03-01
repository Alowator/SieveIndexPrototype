from sieve_index_builder import SieveIndexBuilder

def build():
    builder = SieveIndexBuilder('./table')
    sieve = builder.build_index('0')
    sieve.save('./index')

def main():
    sieve = SieveIndexBuilder.load_index('./index')
    for i in range(285000000, 285000010):
        res = sieve.point_search(i)
        print(res)

if __name__ == '__main__':
    main()
