from ti_daf import sql_util, TxMode
from ti_daf.sql_tx import session_scope
import logging
from datetime import datetime, timedelta


class RelatedPartyIdFinder(object):
    def __init__(self):
        self.party_id_to_old = dict()
        self.party_id_to_new = dict()

        with session_scope(tx_mode=TxMode.NONE_TX, ns_server_id='/db/mysql/ac_cif_db') as session:
            sql_text = '''select partyId, relatedPartyId from ac_cif_db.RelatedPartyRecord order by id'''
            row_list = sql_util.select_rows_by_sql(sql_text, {}, max_size=-1)
            for row in row_list:
                partyId = row['partyId']
                relatedPartyId = row['relatedPartyId']
                self.party_id_to_old[partyId] = relatedPartyId
                self.party_id_to_new[relatedPartyId] = partyId

    @classmethod
    def __search_end_party_id(cls, party_id_to_rel, partyId):
        end_party_id = partyId
        searched_party_ids = set()
        searched_party_ids.add(end_party_id)
        while True:
            rel_party_id = party_id_to_rel.get(end_party_id, None)
            if rel_party_id is None:
                return end_party_id

            if rel_party_id in searched_party_ids:
                logging.getLogger(__name__).error(
                    'Found loop relation partyId=[%s], rel_party_id=[%s].' % (partyId, rel_party_id))
                return rel_party_id

            searched_party_ids.add(rel_party_id)
            end_party_id = rel_party_id

    def get_rel_party_id(self, partyId):
        oldestPartyId = self.__search_end_party_id(self.party_id_to_old, partyId)
        newestPartyId = self.__search_end_party_id(self.party_id_to_new, partyId)

        return oldestPartyId, newestPartyId


def parse_time_range(extract_date, from_date, to_date):
    if extract_date is not None:
        extract_date = datetime.strptime(extract_date, '%Y-%m-%d')
        from_time = extract_date.replace(hour=0, minute=0, second=0, microsecond=0)
        to_time = from_time + timedelta(days=1)
    elif from_date is not None and to_date is not None:
        from_time = datetime.strptime(from_date, '%Y-%m-%d')
        to_time = datetime.strptime(to_date, '%Y-%m-%d')
    else:
        raise ValueError('extract_date or from_date/to_date required.')

    return from_time, to_time