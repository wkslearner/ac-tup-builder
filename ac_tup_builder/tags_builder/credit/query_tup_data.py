import datetime, time
import json

from ti_config import bootstrap
from ti_daf import SqlTemplate, sql_util
from ti_util import json_util

from ac_tup_builder.config import init_app

def get_max_count():
    return int(bootstrap.ti_config_service.get_value('query_max_size'))

# 查询信用历史长度
def query_length_of_history(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')

    sql_text = '''select MIN(acard.opendate) MinTime, MIN(aload.loanDate) MinloanTime, acard.creditId
                  from ac_ccis_db.PCRCardCreditRecord acard,  ac_ccis_db.PCRLoanRecord aload

                  WHERE acard.creditId = :maxId
                  and acard.creditId = aload.creditId
                  
                  GROUP BY acard.creditId, aload.creditId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)
    now = datetime.date.today()
    result = []
    for row in row_list:
        if(row[0] >= row[1]):
            result = (now - row[1]).days
        else:
            result = (now - row[0]).days

    return str(result)

'''
# 查询信用卡张数  不用
def query_number_of_creditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = SELECT creditCardNum FROM ac_ccis_db.PCRBasicInfo apc
                  WHERE apc.partyId = :partyId
                  
                
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result
'''


# 查询人民币信用卡张数
def query_number_of_CNYcreditcard(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT apc.closeDate, apc.creditId
                  FROM ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = :maxId                 
               '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    number_of_CNYcreditcard = 0
    number_of_uncanceledCNYcreditcard = 0
    result = 0
    for row in row_list:
        number_of_CNYcreditcard = number_of_CNYcreditcard + 1
        if row[0] is None:
            number_of_uncanceledCNYcreditcard = number_of_uncanceledCNYcreditcard + 1

    return number_of_CNYcreditcard, number_of_uncanceledCNYcreditcard

# 查询信用卡额度总和(不包含未激活或已注销的卡)
def query_number_of_total_crditline(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT SUM(creditLine) number, creditId
                  FROM ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = :maxId
                  AND apc.creditCardStatus <> '04'
                  AND apc.creditCardStatus <> '05'
                  
                  GROUP BY creditId
                '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 查询信用卡已用额度总和(不包含未激活或已注销的卡)
def query_number_of_total_crditline_used(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT SUM(creditLineUsed) number, creditId
                  FROM ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = :maxId
                  AND apc.creditCardStatus <> '04'
                  AND apc.creditCardStatus <> '05'

                  GROUP BY creditId
                '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        if row[0] is None:
            result = 0
        else:
            result = float(row[0])

    return result

# 呆账信用卡数
def query_number_of_creditcardbaddebts(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.creditId) sum FROM
	               ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.creditId = :maxId
                  AND apc.creditCardStatus = '03'
              '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 信用卡是否当前逾期
def query_overdue_of_creditcard(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.creditId) sum FROM
    	              ac_ccis_db.PCRCardCreditRecord apc

                      WHERE apc.creditId = :maxId
                      AND apc.creditCardStatus = '06'
                  '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 贷款是否当前逾期
def query_overdue_of_loan(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT sum(apc.amountOverdued) FROM
	               ac_ccis_db.PCRLoanRecord apc

                  WHERE apc.creditId = :maxId
                  AND apc.amountOverdued is not NULL
                      '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 逾期90天及以上信用卡数
def query_number_of_everdelinquencyM3creditcard(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.exceedNinetyDaysMonths) sum, apc.creditId FROM
	              ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.creditId = :maxId
                  AND apc.exceedNinetyDaysMonths <> 0
                          '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 逾期90天及以上贷款数
def query_number_of_everdelinquencyM3loan(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.exceedNinetyDaysMonths) sum, apc.creditId FROM
    	             ac_ccis_db.PCRLoanRecord apc

                      WHERE apc.creditId = :maxId
                      AND apc.exceedNinetyDaysMonths <> 0
                      
                              '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 信用卡审批
def query_creditcardapply(maxId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT apc.accessReason FROM
        	       ac_ccis_db.PCRAccessRecord apc
                  WHERE apc.creditId = :maxId
                  GROUP BY apc.accessReason
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'maxId': maxId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    number_of_countercheck = 0
    number_of_onlinecheck = 0
    number_of_creditcardapply = 0
    number_of_loanapply = 0
    number_of_postloan = 0
    number_of_otheraccessreason = 0

    for row in row_list:
        if "临柜" in row[0]:
            number_of_countercheck = number_of_countercheck + 1
        elif "互联网个人信用信息服务平台" in row[0]:
            number_of_onlinecheck = number_of_onlinecheck + 1
        elif "信用卡审批" in row[0]:
            number_of_creditcardapply = number_of_creditcardapply + 1
        elif "贷款审批" in row[0]:
            number_of_loanapply = number_of_loanapply + 1
        elif "贷后管理" in row[0]:
            number_of_postloan = number_of_postloan + 1
        elif "本人查询" in row[0]:
            pass
        else:
            number_of_otheraccessreason = number_of_otheraccessreason + 1


    return number_of_countercheck,number_of_onlinecheck,number_of_creditcardapply, number_of_loanapply, \
           number_of_postloan, number_of_otheraccessreason

# 芝麻信用分
def query_score_of_zmxycredit(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT zmxy.data 
                  from ac_ccis_db.ZmxyReport zmxy 
                  WHERE zmxy.partyId = :partyId
                  order by zmxy.idZmxyReport desc
                    '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)

        if 'zmScore' in snapshot_data.keys():
            result = snapshot_data['zmScore']
            break

    return result

# 芝麻信用行业关注名单当前逾期笔数
def query_overdue_of_zmxywatchlist(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT zmxy.data 
                  from ac_ccis_db.ZmxyWatchListReport zmxy
                  WHERE zmxy.partyId = :partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    count_result_ture = 0
    count_result_false = 0
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)

        if 'details' in snapshot_data.keys():
            if 'settlement' in snapshot_data['details'][0].keys():
                if snapshot_data['details'][0]['settlement'] == True:
                    count_result_ture = count_result_ture + 1
                elif snapshot_data['details'][0]['settlement'] == False:
                    count_result_false = count_result_false + 1

    return count_result_false, count_result_ture


# 芝麻反欺诈分
def query_score_of_zmxyantifruadlist(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT zmxy.data 
                      from ac_ccis_db.ZmxyAntifraudScoreReport zmxy 
                      WHERE zmxy.partyId = :partyId
                      order by zmxy.idZmxyAntifraudScoreReport desc
                        '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)
        if 'score' in snapshot_data.keys():
            result = snapshot_data['score']
            break

    return result

