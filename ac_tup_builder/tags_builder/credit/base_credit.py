import logging

from ac_tup_builder.component import TagsBuilder
from ac_tup_builder.context import TupRecordService, DataContext
from ac_tup_builder.model import TagNamePrefixes
from ac_tup_builder.tags_builder.credit import query_tup_data
from ac_tup_builder.tags_builder.credit.query_tup_data import  \
     query_number_of_CNYcreditcard, query_number_of_total_crditline, \
    query_number_of_total_crditline_used, \
    query_number_of_creditcardbaddebts, query_overdue_of_creditcard, query_overdue_of_loan, \
    query_number_of_everdelinquencyM3creditcard, query_number_of_everdelinquencyM3loan, \
     query_score_of_zmxycredit, query_score_of_zmxyantifruadlist, \
    query_overdue_of_zmxywatchlist, query_creditcardapply


class CreditTagsBuilder(TagsBuilder):
    def __init__(self):
        pass

    def build_tags(self, dataContext: DataContext, tupRecordService: TupRecordService):
        logger = logging.getLogger(__name__)
        tup_rec = tupRecordService.get_tup_record(TagNamePrefixes.CREDIT, dataContext.get_value('gpartyId'))
        partyId = dataContext.get_value('partyId')
        maxId = dataContext.get_value('id')


        # 信用历史长度
        days = query_tup_data.query_length_of_history(maxId)
        logger.info('Prepare to build tup by length_of_history=[%s], partyId=[%s]', days, partyId)
        tup_rec.set_tag('credit.pcr.length_of_history', days)

        