from ti_daf import SqlTemplate

from ac_tup_builder.component import TagsBuilder
from ac_tup_builder.config import init_app
from ac_tup_builder.context import DataContext, TupRecordService, DictDataContext, LruTupRecordService
from ac_tup_builder.model import TagNamePrefixes


class GenderTagsBuilder(TagsBuilder):
    def __init__(self):
        pass

    def build_tags(self, dataContext: DataContext, tupRecordService: TupRecordService):
        tup_rec = tupRecordService.get_tup_record(TagNamePrefixes.POPULATION, dataContext.get_value('gpartyId'))
        id_no = dataContext.get_value('corporateRepresentIdNo')
        if id_no is None or id_no.strip() == '':
            return

        if int(id_no[-2:-1]) % 2 == 0:
            tup_rec.set_tag('population.base.gender', '0')
        else:
            tup_rec.set_tag('population.base.gender', '1')