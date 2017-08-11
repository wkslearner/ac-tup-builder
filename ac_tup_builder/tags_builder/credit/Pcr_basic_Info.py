from ti_daf import SqlTemplate
import sqlalchemy as sa
from ac_tup_builder.component import DataSource
from ac_tup_builder.context import DataContext, SqlAlchemyRowDataContext
from ac_tup_builder.util import RelatedPartyIdFinder


class PcrBasicPartyDataSource(DataSource):
    def __init__(self, from_time, to_time):
        self.relatedPartyIdFinder = RelatedPartyIdFinder()

        self.session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_cif_db')
        sql_text = '''select  acp.partyId, acp.createTime createTime 
                        from ac_ccis_db.PCRBasicInfo acp 
                        where 
                            (acp.createTime>=:fromTime and acp.createTime<:toTime)
                          
                        order by acp.createTime
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
