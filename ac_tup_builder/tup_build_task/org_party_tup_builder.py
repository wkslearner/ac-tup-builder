from ac_tup_builder.component import DataSource, TupBuilder, CompositeTagsBuilder
from ac_tup_builder.config import init_app
from ac_tup_builder.context import SqlAlchemyRowDataContext, DataContext
from ac_tup_builder.tags_builder.credit.Pcr_basic_Info import PcrBasicPartyDataSource
from ac_tup_builder.tags_builder.credit.base_credit import CreditTagsBuilder
from ac_tup_builder.tags_builder.population.base import GenderTagsBuilder
from ti_daf import SqlTemplate
import sqlalchemy as sa
from ac_tup_builder.util import RelatedPartyIdFinder
import logging
from ac_tup_builder.util import parse_time_range


class OrgPartyDataSource(DataSource):
    def __init__(self, from_time, to_time):
        self.relatedPartyIdFinder = RelatedPartyIdFinder()

        self.session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_cif_db')
        sql_text = '''select OrgParty.*, Party.status status, Party.crtTime crtTime, Party.updTime updTime 
                        from ac_cif_db.OrgParty, ac_cif_db.Party 
                        where OrgParty.partyId=Party.partyId
                        and (
                                (Party.crtTime>=:fromTime and Party.crtTime<:toTime)
                                or
                                (Party.updTime>=:fromTime and Party.updTime<:toTime)
                            )
                        order by Party.updTime
        '''

        sql_paras = dict()
        sql_paras['fromTime'] = from_time
        sql_paras['toTime'] = to_time

        self.result = self.session.execute(sa.text(sql_text), sql_paras)

    def next(self) -> DataContext:
        row = self.result.fetchone()
        if row is None:
            return None

        dataContext = SqlAlchemyRowDataContext(row)
        oldestPartyId, newestPartyId = self.relatedPartyIdFinder.get_rel_party_id(dataContext.get_value('partyId'))
        dataContext.set_value('gpartyId', oldestPartyId)
        dataContext.set_value('lastPartyId', newestPartyId)

        return dataContext

    def close(self):
        self.result.close()
        self.session.close()


def build_by_org_party(extract_date=None, from_date=None, to_date=None):
    logger = logging.getLogger(__name__)

    from_time, to_time = parse_time_range(extract_date, from_date, to_date)
    logger.info('Prepare to build tup by OrgParty, fromTime=[%s], toTime=[%s].' % (from_time, to_time))

    ds = PcrBasicPartyDataSource(from_time=from_time, to_time=to_time)

    tags_builders = list()
    tags_builders.append(CreditTagsBuilder())
    tags_builder = CompositeTagsBuilder(tags_builders)
    tup_builder = TupBuilder(ds, tags_builder)
    row_count = tup_builder.build()

    logger.info('Finish to build tup by OrgParty, fromTime=[%s], toTime=[%s], rowCount=[%s].'%(from_time, to_time, row_count))

    return row_count

def execute():
    init_app()
    build_by_org_party(None, '2015-10-23', '2017-10-24')

execute()