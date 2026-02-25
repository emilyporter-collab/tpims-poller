#!/usr/bin/env python3
"""
TPIMS Truck Parking Availability Poller
Fetches real-time data from IL, IN, KY, MN and inserts into BigQuery.
Runs as a GitHub Actions scheduled workflow every 5 minutes.
"""

import json
import urllib.request
import sys
from datetime import datetime, timezone
from google.cloud import bigquery

BQ_TABLE = 'geotab-bi.EmilyPorter_StoryData_US.truck_parking_data_pull_from_tpims'

FEEDS = {
    'Illinois': {
        'dynamic': 'https://truckparking.travelmidwest.com/TPIMS_Dynamic.json',
        'static': 'https://truckparking.travelmidwest.com/TPIMS_Static.json',
    },
    'Indiana': {
        'dynamic': 'https://content.trafficwise.org/json/tpims.json',
        'static': 'https://content.trafficwise.org/json/rest_area.json',
    },
    'Kentucky': {
        'dynamic': 'http://www.trimarc.org/dat/tpims/TPIMS_Dynamic.json',
        'static': 'http://www.trimarc.org/dat/tpims/TPIMS_Static.json',
    },
    'Minnesota': {
        'dynamic': 'http://iris.dot.state.mn.us/iris/TPIMS_dynamic',
        'static': 'http://iris.dot.state.mn.us/iris/TPIMS_static',
    },
}


def fetch_json(url, timeout=15):
    req = urllib.request.Request(url, headers={
        'User-Agent': 'tpims-poller/2.0',
        'Accept': 'application/json',
    })
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode('utf-8', errors='replace'))


def parse_standard(state, static_list, dynamic_list):
    dyn_lookup = {d.get('siteId', ''): d for d in (dynamic_list or [])}
    rows = []
    for s in (static_list or []):
        sid = s.get('siteId', '')
        loc = s.get('location', {})
        if isinstance(loc, list) and loc:
            loc = loc[0]
        elif not isinstance(loc, dict):
            loc = {}
        dyn = dyn_lookup.get(sid, {})

        cap = s.get('capacity', dyn.get('capacity'))
        try:
            cap = int(cap)
        except (TypeError, ValueError):
            cap = None

        avail = dyn.get('reportedAvailable')
        try:
            avail = int(avail)
        except (TypeError, ValueError):
            avail = None

        rows.append({
            'state': state,
            'site_id': sid,
            'name': s.get('name', ''),
            'highway': s.get('relevantHighway', ''),
            'direction': s.get('directionOfTravel', ''),
            'latitude': loc.get('latitude'),
            'longitude': loc.get('longitude'),
            'capacity': cap,
            'available_spaces': avail,
            'trend': dyn.get('trend', ''),
            'is_open': str(dyn.get('open', '')),
            'trust_data': str(dyn.get('trustData', '')),
            'source_timestamp': dyn.get('timeStamp', s.get('timeStamp', '')),
        })
    return rows


def parse_indiana(static_data, dynamic_data):
    feats = static_data.get('features', []) if isinstance(static_data, dict) else []
    rows = []
    for feat in feats:
        props = feat.get('properties', {})
        geo = feat.get('geometry', {})
        coords = geo.get('coordinates', [None, None])
        tpims = props.get('tpims', {})

        cap = tpims.get('capacity')
        try:
            cap = int(cap)
        except (TypeError, ValueError):
            cap = None

        avail = tpims.get('spaces_available')
        try:
            avail = int(avail)
            if avail < 0:
                avail = None
        except (TypeError, ValueError):
            avail = None

        status = props.get('site_area_status', '')
        is_open = 'True' if status == 'open' else ('False' if status == 'closed' else '')

        rows.append({
            'state': 'Indiana',
            'site_id': props.get('site_label', feat.get('id', '')),
            'name': props.get('site_area_description', props.get('site_area_location', '')),
            'highway': tpims.get('route', props.get('site_route', '')),
            'direction': props.get('site_area_label', '').split('-')[0] if '-' in props.get('site_area_label', '') else '',
            'latitude': coords[1] if len(coords) >= 2 else None,
            'longitude': coords[0] if len(coords) >= 2 else None,
            'capacity': cap,
            'available_spaces': avail,
            'trend': '',
            'is_open': is_open,
            'trust_data': '',
            'source_timestamp': tpims.get('last_data', ''),
        })
    return rows


def main():
    poll_time = datetime.now(timezone.utc)
    poll_str = poll_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    print(f'Poll starting at {poll_str}')

    raw = {}
    errors = []
    all_rows = []

    for state, urls in FEEDS.items():
        raw[state] = {}
        for feed_type, url in urls.items():
            try:
                data = fetch_json(url)
                raw[state][feed_type] = data
                print(f'  OK   {state} {feed_type}')
            except Exception as e:
                raw[state][feed_type] = None
                errors.append(f'{state} {feed_type}: {e}')
                print(f'  FAIL {state} {feed_type}: {e}')

    # Parse
    for state in ['Illinois', 'Kentucky', 'Minnesota']:
        static = raw[state]['static'] if isinstance(raw[state].get('static'), list) else []
        dynamic = raw[state]['dynamic'] if isinstance(raw[state].get('dynamic'), list) else []
        all_rows.extend(parse_standard(state, static, dynamic))

    if raw['Indiana']['static']:
        all_rows.extend(parse_indiana(raw['Indiana']['static'], raw['Indiana']['dynamic']))

    # Add poll timestamp
    for row in all_rows:
        row['poll_utc'] = poll_str

    print(f'Parsed {len(all_rows)} site records')

    if not all_rows:
        print('No rows to insert, exiting')
        sys.exit(1 if errors else 0)

    # Insert into BigQuery
    client = bigquery.Client(project='geotab-bi')

    bq_rows = []
    for r in all_rows:
        bq_rows.append({
            'poll_utc': r['poll_utc'],
            'state': r.get('state', ''),
            'site_id': r.get('site_id', ''),
            'name': r.get('name', ''),
            'highway': r.get('highway', ''),
            'direction': r.get('direction', ''),
            'latitude': r.get('latitude'),
            'longitude': r.get('longitude'),
            'capacity': r.get('capacity'),
            'available_spaces': r.get('available_spaces'),
            'trend': r.get('trend', ''),
            'is_open': r.get('is_open', ''),
            'trust_data': r.get('trust_data', ''),
            'source_timestamp': r.get('source_timestamp', ''),
        })

    bq_errors = client.insert_rows_json(BQ_TABLE, bq_rows)
    if bq_errors:
        print(f'BQ insert errors: {bq_errors[:3]}')
        sys.exit(1)

    print(f'Successfully inserted {len(bq_rows)} rows into BigQuery')
    if errors:
        print(f'Feed errors (non-fatal): {errors}')


if __name__ == '__main__':
    main()
