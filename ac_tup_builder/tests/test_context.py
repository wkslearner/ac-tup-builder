from unittest import TestCase
from ti_config import bootstrap
from ti_daf import SqlTemplate, TxMode
from ti_daf.sql_tx import session_scope
from ac_tup_builder.model import Base, TagNamePrefixes
from ac_tup_builder.context import LruTupRecordService


class TestContext(TestCase):
    def setUp(self):
        bootstrap.init_test_cfg('ac-tup-builder', __name__)
        SqlTemplate.set_default_ns_server_id('/db/sqlite/ac_tup_db')
        Base.metadata.create_all(SqlTemplate.get_engine())

    def testLruTupRecordService(self):
        tup_rec_srv = LruTupRecordService(1)
        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.POPULATION, "0001")
        tup_rec.set_tag('population.base.gender', '0')

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.POPULATION, "0001")
        self.assertEqual('0', tup_rec.get_tag('population.base.gender'))

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.SOCIAL, "0001")
        tup_rec.set_tag('social.org.industry_type', '7779')

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.SOCIAL, "0001")
        self.assertEqual('7779', tup_rec.get_tag('social.org.industry_type'))

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.POPULATION, '0002')
        tup_rec.set_tag('population.base.gender', '1')

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.POPULATION, "0001")
        self.assertEqual('0', tup_rec.get_tag('population.base.gender'))

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.SOCIAL, "0001")
        self.assertEqual('7779', tup_rec.get_tag('social.org.industry_type'))

        tup_rec = tup_rec_srv.get_tup_record(TagNamePrefixes.POPULATION, "0002")
        self.assertEqual('1', tup_rec.get_tag('population.base.gender'))