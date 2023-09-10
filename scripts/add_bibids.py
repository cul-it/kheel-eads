#!/usr/bin/env python3
# this script assumes configuration in ~/.archivessnake.yml for the account running it

from csv import DictReader
from pathlib import Path
import json
from argparse import ArgumentParser, FileType
import re
if __name__ == '__main__':
    import asnake.logging as logging
    ap = ArgumentParser(prog="ASpace BIBID import script")
    ap.add_argument('bibids_sheet', type=FileType('r', encoding='utf-8'), help="csv of EADID,BIBID with header")
    ap.add_argument('--logfile', type=FileType('a', encoding='utf-8'), default='bibids.log', help="logfile, line-oriented JSON format")
    ap.add_argument('--continue-from', type=FileType('r', encoding='utf-8'), default=None, help="logfile of partially successful ingest to continue from")
    args = ap.parse_args()

    # Argparse takes care of opening these, but we still want to make sure they're closed on exit no matter what
    with (args.logfile as logfile,
          args.bibids_sheet as csvfile):
        successes = set()
        if args.continue_from:
            with args.continue_from as old_log:
                successes = {entry['eadid'] for entry in map(json.loads, (line for line in old_log)) if entry['event'] == 'successfully updated record'}

        logging.setup_logging(stream=logfile, level="INFO")
        log = logging.get_logger('bibids')
        log.info('started')
        from asnake.client import ASnakeClient
        aspace = ASnakeClient()
        log.info('connected to ASpace')

        template_bibid = {
            "jsonmodel_type": "multi_identifier",
            "identifier_type": "bibid"
        }

        bibid_format = re.compile('\d+')
        for record in DictReader(csvfile):
            eadid = record['EADID']
            bibid = record['BIBID']

            # skip record if already successfully ingested
            if eadid in successes:
                log.info(f"Skipping record due to previous success in log", eadid=eadid, bibid=bibid)
                continue

            # skip record if bibid isn't a number
            if not bibid_format.match(bibid):
                log.info(f"Skipping record due to off-format bibid", eadid=eadid, bibid=bibid)
                continue

            bibid_field = [{**template_bibid, "identifier_value": bibid}]

            # search includes the JSON of the record, BUT shouldn't be trusted for updating it, bc it can be out of date
            # therefore, we just fetch the URIs via search
            results = list(aspace.get_paged('search', params={'q': f"ead_id:{eadid}", 'fields': ['uri']}))
            if len(results) == 1:
                uri = results[0]['uri']
            else:
                log.error('wrong number of results for eadid', eadid=eadid, num=len(results))
                continue

            log.debug("Fetched URI via search", eadid=eadid, uri=uri)
            try:
                resource_json = aspace.get(uri).json()
                log.debug("Fetched record for update", eadid=eadid, uri=uri)
                resource_json['multi_identifiers'] = bibid_field

                res = aspace.post(uri, json=resource_json)
                if res.status_code == 200:
                    log.info('successfully updated record', eadid=eadid, uri=uri)
                else:
                    log.error('failed to post record', eadid=eadid, uri=uri, error_code=res.status_code, error_value=res.text)
            except RuntimeError as e:
                log.error('threw error while processing', eadid=eadid, uri=uri, error=e)
        log.info('finished')
