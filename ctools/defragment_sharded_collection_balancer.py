#!/usr/bin/env python3
#

import argparse
import asyncio
import logging
import math
from motor.frameworks.asyncio import is_event_loop
import pymongo
import sys
import time

from common import Cluster, yes_no
from copy import deepcopy
from pymongo import errors as pymongo_errors

from rich.console import Console
from rich.table import Table
from rich.progress import Progress

# Ensure that the caller is using python 3
if (sys.version_info[0] < 3):
    raise Exception("Must be using Python 3")


class ShardedCollection:
    def __init__(self, cluster, ns):
        self.cluster = cluster
        self.name = ns
        self.ns = {'db': self.name.split('.', 1)[0], 'coll': self.name.split('.', 1)[1]}
        self._direct_config_connection = None

    async def init(self):
        collection_entry = await self.cluster.configDb.collections.find_one({'_id': self.name})
        if (collection_entry is None) or collection_entry.get('dropped', False):
            raise Exception(f"""Collection '{self.name}' does not exist""")

        self.uuid = collection_entry['uuid']
        self.shard_key_pattern = collection_entry['key']
        self.fcv = await self.cluster.FCV

    async def set_balancer(self, enable):
        await self.cluster.configDb.collections.update_one(
            {'_id': self.name},
            {'$set': {'noBalance': not enable}})

    def chunks_query_filter(self):
        if self.fcv >= '5.0':
            return {'uuid': self.uuid}
        else:
            return {'ns': self.name}

    async def balancer_status(self):
        return await self.cluster.adminDb.command({'balancerCollectionStatus': self.name}) 
    
    async def is_defragmenting(self):
        status = await self.balancer_status()
        return 'firstComplianceViolation' in status and status['firstComplianceViolation'] == 'defragmentingChunks'

    async def data_size_kb_per_shard(self):
        """Returns an dict:
        {<shard_id>: <size>}
        with collection size in KiB for each shard
        """
        pipeline = [{'$collStats': {'storageStats': {'scale': 1024}}},
                    {'$project': {'shard': True, 'storageStats': {'size': True}}}]
        storage_stats = await self.cluster.client[self.ns['db']][self.ns['coll']].aggregate(pipeline).to_list(300)
        sizes = {}
        for s in storage_stats:
            shard_id = s['shard']
            sizes[shard_id] = s['storageStats']['size']
        return sizes

    async def data_size_kb_from_shard(self, range):
        data_size_response = await self.cluster.client[self.ns['db']].command({
            'dataSize': self.name,
            'keyPattern': self.shard_key_pattern,
            'min': range[0],
            'max': range[1],
            'estimate': True
        }, codec_options=self.cluster.client.codec_options)

        # Round up the data size of the chunk to the nearest kilobyte
        return math.ceil(max(float(data_size_response['size']), 1024.0) / 1024.0)

def fmt_bytes(num):
    suffix = "B"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"

def fmt_kb(num):
    return fmt_bytes(num*1024)


async def main(args):
    cluster = Cluster(args.uri, asyncio.get_event_loop())
    await cluster.check_is_mongos()

    coll = ShardedCollection(cluster, args.ns)
    await coll.init()
        
    db = cluster.client[coll.ns['db']]

    async def write_all_missing_chunk_size():
        async def write_size(ch, progress):
            await coll.try_write_chunk_size(bounds, ch['shard'], size)
            progress.update()

        missing_size_query = coll.chunks_query_filter()
        missing_size_query.update({'defrag_collection_est_size': {'$exists': 0}})
        num_chunks_missing_size = await cluster.configDb.chunks.count_documents(missing_size_query)

        if not num_chunks_missing_size:
            return

        logging.info("Calculating missing chunk size estimations") 
        with tqdm(total=num_chunks_missing_size, unit=' chunks') as progress:
            tasks = []
            async for ch in cluster.configDb.chunks.find(missing_size_query):
                tasks.append(
                        asyncio.ensure_future(write_size(ch, progress)))
            await asyncio.gather(*tasks)

    async def wait_for_defrag_completion():
        is_defragmenting = await coll.is_defragmenting()
        while is_defragmenting:
            await asyncio.sleep(0.2)
            is_defragmenting = await coll.is_defragmenting()
    
    async def get_shard_stats():
        shard_stats = {}

        shard_sizes = await coll.data_size_kb_per_shard()

        for shard_id, size in shard_sizes.items():
            if shard_id not in shard_stats:
                shard_stats[shard_id] = {}
            shard_stats[shard_id]['data_size'] = size

        
        chunks_count_agg = [
                {'$match': coll.chunks_query_filter()},
                {'$group': { '_id': '$shard', 'count': {'$count': {} } }},
                ]
        shard_chunks = await cluster.configDb.chunks.aggregate(chunks_count_agg).to_list(500)
        for e in shard_chunks:
            shard_stats[e['_id']]['num_chunks'] = e['count']

        return shard_stats


    ######## START ###########

    logging.info(f"""collection '{args.ns}', shardkeypattern: '{coll.shard_key_pattern}""")

    shard_stats_prev = await get_shard_stats()
    logging.info(f"""Disabling balancing""")
    await coll.set_balancer(False)
    
    table = Table(title="Shard stats before")

    table.add_column("Shard ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Chunks", justify="right", style="magenta")
    table.add_column("Size", justify="right", style="green")

    total_num_chunks_prev = 0
    total_data_size_prev = 0
    for shard_id, stats in shard_stats_prev.items():
        total_num_chunks_prev += stats['num_chunks']
        total_data_size_prev += stats['data_size']
        table.add_row(shard_id, f"{stats['num_chunks']}", fmt_kb(stats['data_size']))

    logging.info(f"Total num chunks {total_num_chunks_prev}, Total coll size: {fmt_kb(total_data_size_prev)}")
    target_chunk_size_kb = args.defrag_chunk_size_mb * 1024
    small_chunk_size_threshold_kb = target_chunk_size_kb * 0.25
    logging.info(f"Target chunk size: {fmt_kb(target_chunk_size_kb)}, small chunk threshold: {fmt_kb(small_chunk_size_threshold_kb)}")

    console = Console()
    console.print(table)

    logging.info(f"""Starting defrag""")
    await cluster.adminDb.command({
        'configureCollectionBalancing': args.ns,
        'defragmentCollection': True,
        'chunkSize': args.defrag_chunk_size_mb,
    })

    await wait_for_defrag_completion()

    logging.info(f"""Defrag completed""")
    table = Table(title="Shard stats After")
    table.add_column("Shard ID", justify="right", style="cyan", no_wrap=True)
    table.add_column("Chunks", justify="right", style="magenta")
    table.add_column("diff", justify="right", style="magenta")
    table.add_column("Size", justify="right", style="green")
    table.add_column("diff", justify="right", style="green")

    shard_stats_post = await get_shard_stats()
    total_num_chunks_post = 0
    total_data_size_post = 0
    for shard_id, stats in shard_stats_post.items():
        total_num_chunks_post += stats['num_chunks']
        total_data_size_post += stats['data_size']
        stats_prev = shard_stats_prev[shard_id]
        chunks_delta = stats['num_chunks'] - stats_prev['num_chunks']
        size_delta = stats['data_size'] - stats_prev['data_size']
        size_delta_sign = "+" if size_delta >= 0 else "-"
        table.add_row(
                shard_id, 
                f"{stats['num_chunks']}", 
                f"{chunks_delta:+}",
                f"{fmt_kb(stats['data_size'])}", 
                f"{size_delta_sign}{fmt_kb(abs(size_delta))}")

    logging.info(f"Total num chunks {total_num_chunks_post} ({total_num_chunks_post - total_num_chunks_prev}), Total coll size: {fmt_kb(total_data_size_post)}")
    console = Console()
    console.print(table)

    # Check that defragmentation worked properly by inspecting the chunks 
    chunks_by_shard = {}
    chunks_agg = [
            {'$match': coll.chunks_query_filter()},
            {'$sort': {'min': 1}}]

    with Progress() as progress:
        task = progress.add_task("Checking chunks...", total=total_num_chunks_post)

        async for ch in cluster.configDb.chunks.aggregate(chunks_agg):

            # Check size
            ch['size'] = await coll.data_size_kb_from_shard([ch['min'], ch['max']])
            if ch['size'] <= small_chunk_size_threshold_kb:
                logging.warning(f"Found a remaining small chunk: {ch}")
            if ch['size'] > target_chunk_size_kb * 1.33:
                logging.warning(f"Found a chunk too big: {ch}")

            # Check sibling
            shard_id = ch['shard']
            if shard_id not in chunks_by_shard:
                chunks_by_shard[shard_id] = []
            shard_chunks = chunks_by_shard[shard_id]
            if len(shard_chunks) != 0 and shard_chunks[-1]['max'] == ch['min']:
                sibling = shard_chunks[-1]
                if (sibling['size'] + ch['size']) < target_chunk_size_kb * 1.33:
                    logging.warning(f"Found mergable chunks for shard '{shard_id}':\n\tleft:  {shard_chunks[-1]}\n\tright: {ch}]\n")
            shard_chunks.append(ch)
            progress.update(task, advance=1)


if __name__ == "__main__":
    argsParser = argparse.ArgumentParser(
        description=
        """Tool to defragment a sharded cluster in a way which minimises the rate at which the major
           shard version gets bumped in order to minimise the amount of stalls due to refresh.""")
    argsParser.add_argument(
        'uri', help='URI of the mongos to connect to in the mongodb://[user:password@]host format',
        metavar='uri', type=str)
    argsParser.add_argument('ns', help="""The namespace on which to perform defragmentation""",
                            metavar='ns', type=str)
    argsParser.add_argument(
        '--chunk-size',
        help="Chunk size", metavar='chunk_size_mb', dest='defrag_chunk_size_mb',
        type=int, default=1)

    
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=logging.INFO)
    params = " ".join(sys.argv[1:])
    logging.info(f"Starting with parameters: '{params}'")

    args = argsParser.parse_args()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(args))
