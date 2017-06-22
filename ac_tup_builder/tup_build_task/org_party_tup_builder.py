from ac_tup_builder.component import DataSource
from ac_tup_builder.context import SqlAlchemyRowDataContext, DataContext
from ti_daf import SqlTemplate
import sqlalchemy as sa
from ac_tup_builder.util import RelatedPartyIdFinder


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