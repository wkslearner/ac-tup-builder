from unittest import TestCase
from ti_config import bootstrap
from ti_daf import SqlTemplate, TxMode
from ti_daf.sql_tx import session_scope
from ac_tup_builder.model import Base, PopulationTupRecord, TupHistoryRecord
from datetime import datetime


class TestPopulationTupRecord(TestCase):
    def setUp(self):
        bootstrap.init_test_cfg('ac-tup-builder', __name__)
        SqlTemplate.set_default_ns_server_id('/db/sqlite/ac_tup_db')
        Base.metadata.create_all(SqlTemplate.get_engine())

    def test(self):
        with session_scope(tx_mode=TxMode.NONE_TX) as session:
            popTupRec = PopulationTupRecord(gpartyId="0001")
            popTupRec.set_tag('population.base.gender', '0')
            popTupRec.dump_tup_data()

            session.add(popTupRec)

        with session_scope(tx_mode=TxMode.NONE_TX) as session:
            popTupRecs = session.query(PopulationTupRecord)
            print(popTupRecs.count())
            popTupRec = popTupRecs.first()
            print(popTupRec.tagNamePrefix)
            print(popTupRec.tupData)
            print(popTupRec.get_tag('population.base.gender'))
            self.assertEqual('0', popTupRec.get_tag('population.base.gender'))
            self.assertIsNone(popTupRec.get_tag('population.base.age'))

            try:
                popTupRec.set_tag('base.gender', '1')
                self.fail('not throw exception.')
            except ValueError:
                pass

            popTupRec.set_tag('population.base.gender', '1')
            print(popTupRec.oldTags)

            popTupRec.updTime = datetime.now()
            session.add(popTupRec)

            print(session.query(PopulationTupRecord).filter(PopulationTupRecord.gpartyId=='1234').first())

        with session_scope() as session:
            history_rec = TupHistoryRecord()
            history_rec.setNewValueObject("123")
            history_rec.setOldValueObject("321")
            self.assertEquals("123", history_rec.getNewValueObject())
            self.assertEquals("321", history_rec.getOldValueObject())

            history_rec.setOldValueObject(datetime.strptime('2017-06-10', '%Y-%m-%d'))
            self.assertEquals('2017-06-10', history_rec.getOldValueObject().strftime('%Y-%m-%d'))