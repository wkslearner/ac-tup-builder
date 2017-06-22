from unittest import TestCase
from ti_config import bootstrap
from ti_daf import SqlTemplate
from ac_tup_builder.model import Base, TagNamePrefixes, PopulationTupRecord
from ac_tup_builder.context import DataContext, TupRecordService
from ac_tup_builder.component import TagsBuilder, TupBuilder, DataSource
from ti_daf.sql_tx import session_scope


class TestTagsBuilder(TagsBuilder):
    def build_tags(self, dataContext:DataContext, tupRecordService:TupRecordService):
        tup_rec = tupRecordService.get_tup_record(TagNamePrefixes.POPULATION, dataContext.get_value('gpartyId'))
        tup_rec.set_tag('population.base.gender', dataContext.get_value('gender'))


class TestDataSource(DataSource):
    def __init__(self, dataContextList:list):
        self.idx = 0
        self.dataContextList = dataContextList

    def next(self):
        if self.idx == len(self.dataContextList):
            return None

        self.idx = self.idx + 1
        return self.dataContextList[self.idx-1]


class TestTupBuilder(TestCase):
    def setUp(self):
        bootstrap.init_test_cfg('ac-tup-builder', __name__)
        SqlTemplate.set_default_ns_server_id('/db/sqlite/ac_tup_db')
        Base.metadata.create_all(SqlTemplate.get_engine())

    def testTupBuilder(self):
        dataContextList = list()
        dataContextList.append(DataContext({'gender': '0', 'gpartyId': '0001'}))
        dataContextList.append(DataContext({'gender': '1', 'gpartyId': '0002'}))
        dataContextList.append(DataContext({'gender': '0', 'gpartyId': '0003'}))

        tupBuilder = TupBuilder(TestDataSource(dataContextList), TestTagsBuilder())
        tupBuilder.build()

        with session_scope() as session:
            tup_recs = session.query(PopulationTupRecord).order_by(PopulationTupRecord.gpartyId).all()
            self.assertEqual(3, len(tup_recs))
            self.assertEqual('0', tup_recs[0].get_tag('population.base.gender'))
            self.assertEqual('1', tup_recs[1].get_tag('population.base.gender'))
            self.assertEqual('0', tup_recs[2].get_tag('population.base.gender'))