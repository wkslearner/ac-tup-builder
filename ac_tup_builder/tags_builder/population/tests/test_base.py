from unittest import TestCase
from ti_config import bootstrap
from ti_daf import SqlTemplate
from ac_tup_builder.model import Base, TagNamePrefixes
from ac_tup_builder.tags_builder.population.base import GenderTagsBuilder
from ac_tup_builder.context import LruTupRecordService, DictDataContext


class TestTagsBuilder(TestCase):
    def setUp(self):
        bootstrap.init_test_cfg('ac-tup-builder', __name__)
        SqlTemplate.set_default_ns_server_id('/db/sqlite/ac_tup_db')
        Base.metadata.create_all(SqlTemplate.get_engine())

    def test_build_gender_tags(self):
        builder = GenderTagsBuilder()
        dataContext = DictDataContext(dict())
        dataContext.set_value('gpartyId', '0001')
        dataContext.set_value('corporateRepresentIdNo', '411521199301291954')
        tupRecService = LruTupRecordService(lruSize=100)
        builder.build_tags(dataContext, tupRecordService=tupRecService)
        tupRec = tupRecService.get_tup_record(TagNamePrefixes.POPULATION, '0001')
        self.assertEqual('1', tupRec.get_tag('population.base.gender'))

        dataContext.set_value('corporateRepresentIdNo', '411521199301291984')
        builder.build_tags(dataContext, tupRecordService=tupRecService)
        tupRec = tupRecService.get_tup_record(TagNamePrefixes.POPULATION, '0001')
        self.assertEqual('0', tupRec.get_tag('population.base.gender'))