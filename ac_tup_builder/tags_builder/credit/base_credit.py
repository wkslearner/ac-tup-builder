import logging

from ac_tup_builder.component import TagsBuilder
from ac_tup_builder.context import TupRecordService, DataContext
from ac_tup_builder.model import TagNamePrefixes
from ac_tup_builder.tags_builder.credit import query_tup_data
from ac_tup_builder.tags_builder.credit.query_tup_data import query_number_of_creditcard, \
    query_number_of_uncanceledCNYcreditcard, query_number_of_CNYcreditcard, query_number_of_total_crditline, \
    query_number_of_total_crditline_used, query_freq_of_loan, query_total_loanamount, query_total_loanamount_used, \
    query_number_of_creditcardbaddebts, query_overdue_of_creditcard, query_overdue_of_loan, \
    query_number_of_everdelinquencyM3creditcard, query_number_of_everdelinquencyM3loan, query_number_of_countercheck, \
    query_number_of_onlinecheck, query_number_of_creditcardapply, query_number_of_loanapply, query_number_of_postloan, \
    query_number_of_otheraccessreason, query_score_of_zmxycredit, query_score_of_zmxyantifruadlist, \
    query_overdue_of_zmxywatchlist

class CreditTagsBuilder(TagsBuilder):
    def __init__(self):
        pass

    def build_tags(self, dataContext: DataContext, tupRecordService: TupRecordService):
        logger = logging.getLogger(__name__)
        tup_rec = tupRecordService.get_tup_record(TagNamePrefixes.CREDIT, dataContext.get_value('gpartyId'))
        partyId = dataContext.get_value('partyId')

        # 信用历史长度
        days = query_tup_data.query_length_of_history(partyId)
        logger.info('Prepare to build tup by length_of_history=[%s]', days)
        tup_rec.set_tag('credit.pcr.length_of_history', days)

        # 信用卡张数
        number_creditcard = query_number_of_creditcard(partyId)
        logger.info('Prepare to build tup by number_creditcard=[%s]', number_creditcard)
        tup_rec.set_tag('credit.pcr.number_of_creditcard', number_creditcard)

        # 人民币信用卡张数
        number_CNYcreditcard = query_number_of_CNYcreditcard(partyId)
        logger.info('Prepare to build tup by number_CNYcreditcard=[%s]')
        tup_rec.set_tag('credit.pcr.number_of_CNYcreditcard', number_CNYcreditcard)

        # 未销户人民币信用卡张数
        number_uncanceledCNYcreditcard = query_number_of_uncanceledCNYcreditcard(partyId)
        logger.info('Prepare to build tup by number_uncanceledCNYcreditcard=[%s]',number_uncanceledCNYcreditcard)
        tup_rec.set_tag('credit.pcr.number_of_uncaneledCNYcreditcard', number_uncanceledCNYcreditcard)

        # 信用卡额度总和
        number_total_crditLine = query_number_of_total_crditline(partyId)
        logger.info('Prepare to build tup by number_total_crditLine=[%s]', number_total_crditLine)
        tup_rec.set_tag('credit.pcr.total_crditLine', number_total_crditLine)

        # 信用卡已用额度总和
        number_total_crditLine_used = query_number_of_total_crditline_used(partyId)
        logger.info('Prepare to build tup by number_total_crditLine_used=[%s]', number_total_crditLine_used)
        tup_rec.set_tag('credit.pcr.total_creditline_used', number_total_crditLine_used)

        # 信用卡额度使用率
        usage_of_crditLine = []
        if number_total_crditLine != [] and number_total_crditLine_used != []:
            usage_of_crditLine = "%.2f%%" % ((100*number_total_crditLine_used) / number_total_crditLine)
        logger.info('Prepare to build tup by usage_of_crditLine=[%s]', usage_of_crditLine)
        tup_rec.set_tag('credit.pcr.usage_of_creditline', usage_of_crditLine)

        # 贷款笔数
        freq_of_loan = query_freq_of_loan(partyId)
        logger.info('Prepare to build tup by freq_of_loan=[%s]', freq_of_loan)
        tup_rec.set_tag('credit.pcr.freq_of_loan', freq_of_loan)

        # 贷款金额总和
        total_loanamount = query_total_loanamount(partyId)
        logger.info('Prepare to build tup by total_loanamount=[%s]', total_loanamount)
        tup_rec.set_tag('credit.pcr.total_Loanamount', total_loanamount)

        # 贷款余额总和
        total_loanamount_used = query_total_loanamount_used(partyId)
        logger.info('Prepare to build tup by total_loanamount_used=[%s]', total_loanamount_used)
        tup_rec.set_tag('credit.pcr.total_Loanamount_used', total_loanamount_used)

        # 呆账信用卡数
        number_of_creditcardbadebts = query_number_of_creditcardbaddebts(partyId)
        logger.info('Prepare to build tup by number_of_creditcardbadebts=[%s]', number_of_creditcardbadebts)
        tup_rec.set_tag('credit.pcr.number_of_creditcardbaddebts', number_of_creditcardbadebts)

        # 信用卡是否当前逾期

        card_result = query_overdue_of_creditcard(partyId)
        if card_result == []:
            overdue_of_creditcard = '0'
        else:
            overdue_of_creditcard = '1'
        logger.info('Prepare to build tup by card_result=[%s]', overdue_of_creditcard)
        tup_rec.set_tag('credit.pcr.overdue_of_creditcard', overdue_of_creditcard)

        # 贷款是否当前逾期
        lean_result = query_overdue_of_loan(partyId)

        if lean_result == [] or lean_result == 0.00:
            overdue_of_loan = '0'
        else:
            overdue_of_loan = '1'
        logger.info('Prepare to build tup by lean_result=[%s]', overdue_of_loan)
        tup_rec.set_tag('credit.pcr.overdue_of_loan', overdue_of_loan)

        # 逾期90天及以上信用卡数
        over_card_result = query_number_of_everdelinquencyM3creditcard(partyId)
        if over_card_result == []:
            number_of_everdelinquencycard = 0
        else:
            number_of_everdelinquencycard = over_card_result
        logger.info('Prepare to build tup by over_card_result=[%s]', number_of_everdelinquencycard)
        tup_rec.set_tag('credit.pcr.number_of_everdelinquencyM3creditcard', number_of_everdelinquencycard)

        # 逾期90天及以上贷款数
        over_loan_result = query_number_of_everdelinquencyM3loan(partyId)

        if over_loan_result == []:
            number_of_exceedloan = 0
        else:
            number_of_exceedloan = over_card_result
        logger.info('Prepare to build tup by over_loan_result=[%s]', number_of_exceedloan)
        tup_rec.set_tag('credit.pcr.number_of_everdelinquencyM3loan', number_of_exceedloan)

        # 临柜查询次数
        number_of_countercheck = query_number_of_countercheck(partyId)
        logger.info('Prepare to build tup by number_of_countercheck=[%s]', number_of_countercheck)
        tup_rec.set_tag('credit.pcr.number_of_countercheck', number_of_countercheck)

        # 互联网个人查询次数
        number_of_onlinecheck = query_number_of_onlinecheck(partyId)
        logger.info('Prepare to build tup by number_of_onlinecheck=[%s]', number_of_onlinecheck)
        tup_rec.set_tag('credit.pcr.number_of_onlinecheck', number_of_onlinecheck)

        # 信用卡审批次数
        number_of_creditcardapply = query_number_of_creditcardapply(partyId)
        logger.info('Prepare to build tup by number_of_creditcardapply=[%s]', number_of_creditcardapply)
        tup_rec.set_tag('credit.pcr.number_of_creditcardapply', number_of_creditcardapply)

        # 贷款审批次数
        number_of_loanapply = query_number_of_loanapply(partyId)
        logger.info('Prepare to build tup by number_of_loanapply=[%s]', number_of_loanapply)
        tup_rec.set_tag('credit.pcr.number_of_loanapply', number_of_loanapply)

        # 贷后管理次数
        number_of_postloan = query_number_of_postloan(partyId)
        logger.info('Prepare to build tup by number_of_postloan=[%s]', number_of_postloan)
        tup_rec.set_tag('credit.pcr.number_of_postloan', number_of_postloan)

        # 其它类型审批次数
        number_of_otheraccessreason = query_number_of_otheraccessreason(partyId)
        logger.info('Prepare to build tup by number_of_otheraccessreason=[%s]', number_of_otheraccessreason)
        tup_rec.set_tag('credit.pcr.number_of_otheraccessreason', number_of_otheraccessreason)

        # 芝麻信用
        score_of_zmxycredit = query_score_of_zmxycredit(partyId)
        logger.info('Prepare to build tup by score_of_zmxycredit=[%s]', score_of_zmxycredit)
        tup_rec.set_tag('credit.zmxy.score_of_zmxycredit', score_of_zmxycredit)

        # 芝麻信用行业关注名单当前逾期笔数

        overdue_of_zmxywatchlist = query_overdue_of_zmxywatchlist(partyId)
        logger.info('Prepare to build tup by overdue_of_zmxywatchlist=[%s]', overdue_of_zmxywatchlist[0])
        tup_rec.set_tag('credit.zmxy.overdue_of_zmxywatchlist', overdue_of_zmxywatchlist[0])
        logger.info('Prepare to build tup by everoverdue_of_zmxywatchlist=[%s]', overdue_of_zmxywatchlist[1])
        tup_rec.set_tag('credit.zmxy.everoverdue_of_zmxywatchlist', overdue_of_zmxywatchlist[1])

        # 芝麻信用反欺诈分
        score_of_zmxyantifruadlist = query_score_of_zmxyantifruadlist(partyId)
        logger.info('Prepare to build tup by score_of_zmxyantifruadlist=[%s]', score_of_zmxyantifruadlist)
        tup_rec.set_tag('credit.zmxy.score_of_zmxyantifruadlist', score_of_zmxyantifruadlist)