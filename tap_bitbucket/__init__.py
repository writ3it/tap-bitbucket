#!/usr/bin/env python3
import base64
import datetime
import os
import json

import pytz
import requests
import singer
from requests import Session
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

from strict_rfc3339 import rfc3339_to_timestamp

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
LOGGER = singer.get_logger()


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schema(entity):
    return utils.load_json(get_abs_path("schemas/{}.json".format(entity)))


RESOURCES = {
    "repositories": {
        "url": "https://api.bitbucket.org/2.0/repositories/{}?sort=updated_on",
        "schema": load_schema('repositories'),
        'key_properties': ['uuid'],
        'replication_method': 'FULL_TABLE',
        'replication_key': 'updated_on'
    },
    "repositories_pullrequests": {
        "url": "https://api.bitbucket.org/2.0/repositories/{}/{}/pullrequests",
        'schema': load_schema('repositories_pullrequests'),
        'key_properties': ['id'],
        'replication_method': 'FULL_TABLE',
        'replication_key': 'updated_on'
    },
    "repositories_pullrequests_commits": {
        "url": "https://api.bitbucket.org/2.0/repositories/{}/{}/pullrequests/{}/commits",
        'schema': load_schema('repositories_pullrequests_commits'),
        'key_properties': ['hash'],
        'replication_method': 'FULL_TABLE',
        'replication_key': 'date'
    }
}


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..
        stream_metadata = []
        key_properties = []
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def format_timestamp(data, typ, schema):
    result = data
    if typ == 'string' and schema.get('format') == 'date-time':
        rfc3339_ts = rfc3339_to_timestamp(data)
        utc_dt = datetime.datetime.utcfromtimestamp(rfc3339_ts).replace(tzinfo=pytz.UTC)
        result = utils.strftime(utc_dt)

    return result


def sync_resource(url: str,replication_key:str, stream, session: Session, headers: dict, next=None):
    transformer = Transformer(pre_hook=format_timestamp)

    while True:
        page = session.get(url, headers=headers).json()
        LOGGER.info(url)
        if len(page['values']) == 0:
            LOGGER.info("{} is empty".format(url))
        for record in page['values']:
            item = transformer.transform(record, stream.schema.to_dict())
            time_extracted = utils.now()
            singer.write_record(stream.tap_stream_id, item, time_extracted=time_extracted)
            singer.write_state({stream.tap_stream_id: item[replication_key]})

            if next is not None:
                next(item, session, headers)
            else:
                LOGGER.info("LEAF")

        if 'next' in page:
            url = page['next']
        else:
            break


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties,
        )

    session = requests.Session()
    headers = {
        "Authorization": "Basic " + base64.b64encode(
            (config["username"] + ":" + config['password']).encode('ascii')).decode('ascii'),
        "Content-Type": "application/json"
    }

    sync_resource(
        url=RESOURCES['repositories']['url'].format(config['workspace']),
        stream=catalog.get_stream("repositories"),
        replication_key=RESOURCES['repositories']['replication_key'],
        session=session,
        headers=headers,
        next=lambda repository, session, headers: sync_resource(
            url=repository['links']['pullrequests']['href']+"?sort=updated_on&state=OPEN,MERGED,DECLINED,SUPERSEDED",
            stream=catalog.get_stream('repositories_pullrequests'),
            replication_key=RESOURCES['repositories_pullrequests']['replication_key'],
            session=session,
            headers=headers,
            next=lambda pullrequest, session, headers: sync_resource(
                url=pullrequest['links']['commits']['href'],
                stream=catalog.get_stream('repositories_pullrequests_commits'),
                replication_key=RESOURCES['repositories_pullrequests_commits']['replication_key'],
                session=session,
                headers=headers
            )
        )
    )

    LOGGER.info("END")
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()
