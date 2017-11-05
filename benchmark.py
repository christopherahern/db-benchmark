import argparse
import sys
import os
import time
import glob
import bz2
import ujson
import psycopg2
from psycopg2.extras import execute_batch

def main(argv):
    parser = argparse.ArgumentParser(
        description='Options for benchmarking databases.',
    )
    parser.add_argument('-db',
                        nargs='?',
                        default='postgres',
                        choices=['postgres', 'timescaledb'],
                        help='Database to use for benchmarking.')
    parser.add_argument('--data',
                        nargs='?',
                        default='data',
                        help='Location of data directory.')
    parser.add_argument('--drop',
                        default='false',
                        action='store_true',
                        help='Drop current db tables.')
    parser.add_argument('--batch',
                        nargs='?',
                        default=10000,
                        help='Batch size when inserting token counts.')
    args = vars(parser.parse_args())

    # Set port based on db
    if args['db'] == 'postgres':
        port = "5432"
    elif args['db'] == 'timescaledb':
        port = "6543"

    # Creat connection to db and cursor
    conn = psycopg2.connect(
        host="localhost",
        port=port,
        user="postgres",
        password="postgres"
    )

    # Create cursor
    cur = conn.cursor()

    # Load filepaths
    files = glob.glob(os.path.join(args['data'], '*.bz2'))
    for i, f in enumerate(files):
        if (i + 1) % 100 == 0:
            percent = 100 * (i + 1) / float(len(files))
            print 'Processed {} documents ({} %)'.format(i + 1, percent)

        # Load file to json
        data = load_file(f)

        # Limit to English documents
        if data['metadata']['language'] == 'eng':
            # Get token counts
            count_list = get_token_counts(data)

            start_time = time.time()
            # Insert tokens
            insert_tokens(cur, count_list, page_size=args['batch'])

            # NOTE: execute_batch is much faster than inserting individually
            # http://initd.org/psycopg/docs/extras.html#fast-exec
            # for (token, pos), count in token_counts.iteritems():
            #     row = [volume_id, token, pos, count]
            #     cur.execute('INSERT INTO tokens VALUES (%s,%s,%s,%s)', row)

            # Inserts per second
            total_time = time.time() - start_time
            ips = len(count_list) / total_time

        # Commit changes to db
        conn.commit()

    # Close cursor and conneciton
    # cur.close()
    # conn.close()


def load_file(filepath):
    """Load compressed json file from filepath."""
    zipfile = bz2.BZ2File(filepath) # Decompress file
    data = ujson.loads(zipfile.read()) # Load json
    return data


def get_token_counts(data):
    """Get part-of-speech tagged token counts for individual volume data."""
    volume_id = data['id']
    # Store {(token, POS): count}
    count_dict = {}

    # Loop through pages
    pages = data['features']['pages']
    for page in pages:
        # Get token and POS counts
        for token, pos_counts in page['body']['tokenPosCount'].iteritems():
            for pos, value in pos_counts.iteritems():
                key = (token.lower(), pos)
                if key in count_dict:
                    count_dict[key] += value
                else:
                    count_dict[key] = value

    # Convert dictionary to list of lists
    count_list = [
        [volume_id, token, pos, count]
        for (token, pos), count in count_dict.iteritems()
    ]

    return count_list


def insert_tokens(cursor, count_list, page_size=10000):
    """Insert tokens into table in batches."""
    sql = 'INSERT INTO tokens VALUES (%s,%s,%s,%s)'
    execute_batch(cursor, sql, count_list, page_size=page_size)


if __name__ == '__main__':
    main(sys.argv)
