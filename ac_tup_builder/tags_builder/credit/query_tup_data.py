import datetime, time
import json

from ti_daf import SqlTemplate, sql_util
from ti_util import json_util

from ac_tup_builder.config import init_app


# 查询信用历史长度
def query_length_of_history(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')

    sql_text = '''select MIN(acard.opendate) MinTime, MIN(aload.loanDate) MinloanTime, acard.creditId, maxid.partyId
                  from (
	                  SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  )maxid,
                  ac_ccis_db.PCRCardCreditRecord acard,  ac_ccis_db.PCRLoanRecord aload

                  WHERE acard.creditId = maxid.id
                  and acard.creditId = aload.creditId
                  and maxid.partyId = :partyId
                  GROUP BY acard.creditId, aload.creditId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)
    now = datetime.date.today()
    result = []
    for row in row_list:
        if(row[0] >= row[1]):
            result = (now - row[1]).days
        else:
            result = (now - row[0]).days

    return result


# 查询信用卡张数
def query_number_of_creditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT creditCardNum FROM ac_ccis_db.PCRBasicInfo apc
                  WHERE apc.partyId = :partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 查询人民币信用卡张数
def query_number_of_CNYcreditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT COUNT(*) number, creditId, maxid.partyId
                  FROM(
	                    SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      )maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = maxid.id
                  AND maxid.partyId = :partyId

                  GROUP BY creditId
               '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 查询未销户的人民币信用卡张数
def query_number_of_uncanceledCNYcreditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT COUNT(*) number, creditId, maxid.partyId
                  FROM(
    	                SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      )maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY'
                  AND apc.closeDate is NULL
                  AND apc.creditId = maxid.id
                  AND maxid.partyId = :partyId

                  GROUP BY creditId
            '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 查询信用卡额度总和(不包含未激活或已注销的卡)
def query_number_of_total_crditline(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT SUM(creditLine) number, creditId, maxid.partyId
                  FROM(
        	          SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      )maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = maxid.id
                  AND apc.creditCardStatus <> '04'
                  AND apc.creditCardStatus <> '05'
                  AND maxid.partyId = :partyId
                  
                  GROUP BY creditId
                '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 查询信用卡已用额度总和(不包含未激活或已注销的卡)
def query_number_of_total_crditline_used(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT SUM(creditLineUsed) number, creditId, maxid.partyId
                  FROM(
            	      SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  )maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE apc.currencyCode = 'CNY' 
                  AND apc.creditId = maxid.id
                  AND apc.creditCardStatus <> '04'
                  AND apc.creditCardStatus <> '05'
                  AND maxid.partyId = :partyId

                  GROUP BY creditId
                '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 贷款笔数
def query_freq_of_loan(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = ''' SELECT abasic.loanFreq loanFreq, MAX(abasic.id) id, abasic.partyId partyId
                    FROM ac_ccis_db.PCRBasicInfo abasic 
                    WHERE abasic.partyId = :partyId
                    GROUP BY partyId
               '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 贷款金额总和
def query_total_loanamount(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = ''' SELECT abasic.totalLoanAmount, MAX(abasic.id) id, abasic.partyId partyId
                   FROM ac_ccis_db.PCRBasicInfo abasic 
                    WHERE abasic.partyId = :partyId
                    GROUP BY partyId
                   '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 贷款余额总和
def query_total_loanamount_used(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = ''' SELECT abasic.totalCreditLineUsed, MAX(abasic.id) id, abasic.partyId partyId
                   FROM ac_ccis_db.PCRBasicInfo abasic 
                   WHERE abasic.partyId = :partyId
                   GROUP BY partyId
                '''

    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = float(row[0])

    return result


# 呆账信用卡数
def query_number_of_creditcardbaddebts(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.creditId) sum, maxid.partyId FROM
	              (
		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  ) maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.creditCardStatus = '03'
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
              '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 信用卡是否当前逾期
def query_overdue_of_creditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.creditId) sum, maxid.partyId FROM
    	              (
    		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      ) maxid, ac_ccis_db.PCRCardCreditRecord apc

                      WHERE maxid.id = apc.creditId
                      AND apc.creditCardStatus = '06'
                      AND maxid.partyId = :partyId
                      GROUP BY maxid.partyId
                  '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result


# 贷款是否当前逾期
def query_overdue_of_loan(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT sum(apc.amountOverdued), maxid.partyId FROM
	              (
		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  ) maxid, ac_ccis_db.PCRLoanRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.amountOverdued is not NULL
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                      '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 逾期90天及以上信用卡数
def query_number_of_everdelinquencyM3creditcard(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.exceedNinetyDaysMonths) sum, apc.creditId, maxid.partyId FROM
	              (
		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  ) maxid, ac_ccis_db.PCRCardCreditRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.exceedNinetyDaysMonths <> 0
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                          '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 逾期90天及以上贷款数
def query_number_of_everdelinquencyM3loan(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.exceedNinetyDaysMonths) sum, apc.creditId, maxid.partyId FROM
    	              (
    		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      ) maxid, ac_ccis_db.PCRLoanRecord apc

                      WHERE maxid.id = apc.creditId
                      AND apc.exceedNinetyDaysMonths <> 0
                      AND maxid.partyId = :partyId
                      GROUP BY maxid.partyId
                              '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 临柜查询次数
def query_number_of_countercheck(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
	              (
		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                  ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason like "%临柜%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 互联网个人查询次数
def query_number_of_onlinecheck(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
    	              (
    		            SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason like "%互联网个人信用信息服务平台%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                    '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 信用卡审批次数
def query_number_of_creditcardapply(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
        	            (
        		          SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                        ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason like "%信用卡审批%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 贷款审批次数
def query_number_of_loanapply(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
            	        (
            		      SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                        ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason like "%贷款审批%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 贷后管理次数
def query_number_of_postloan(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
                	    (
                		  SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                        ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason like "%贷后管理%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
            '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/ac_ccis_db', max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 其他类型审批次数
def query_number_of_otheraccessreason(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/ac_ccis_db')
    sql_text = '''SELECT count(apc.accessReason) sum,apc.accessReason, maxid.partyId FROM
                      (
                    	SELECT MAX(abasic.id) id, abasic.partyId partyId FROM ac_ccis_db.PCRBasicInfo abasic GROUP BY partyId
                      ) maxid, ac_ccis_db.PCRAccessRecord apc

                  WHERE maxid.id = apc.creditId
                  AND apc.accessReason not like "%信用卡审批%"
                  AND apc.accessReason not like "%贷款审批%"
                  AND apc.accessReason not like "%贷后管理%"
                  AND apc.accessReason not like "%本人查询%"
                  AND maxid.partyId = :partyId
                  GROUP BY maxid.partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/ac_ccis_db',max_size=-1)

    result = []
    for row in row_list:
        result = row[0]

    return result

# 芝麻信用分
def query_score_of_zmxycredit(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/bi_bigdata_db')
    sql_text = '''SELECT zmxy.data 
                  from ac_ccis_db.ZmxyReport zmxy 
                  WHERE zmxy.partyId = :partyId
                  order by zmxy.idZmxyReport desc
                    '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId},ns_server_id='/db/mysql/bi_bigdata_db', max_size=-1)

    result = []
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)

        if 'zmScore' in snapshot_data.keys():
            result = snapshot_data['zmScore']
            break

    return result

# 芝麻信用行业关注名单当前逾期笔数
def query_overdue_of_zmxywatchlist(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/bi_bigdata_db')
    sql_text = '''SELECT zmxy.data 
                  from ac_ccis_db.ZmxyWatchListReport zmxy
                  WHERE zmxy.partyId = :partyId
                '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/bi_bigdata_db',max_size=-1)

    count_result_ture = 0
    count_result_false = 0
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)

        if 'details' in snapshot_data.keys():
            if snapshot_data['details'][0]['settlement'] == True:
                count_result_ture = count_result_ture + 1
            elif snapshot_data['details'][0]['settlement'] == False:
                count_result_false = count_result_false + 1

    return count_result_false, count_result_ture


# 芝麻反欺诈分
def query_score_of_zmxyantifruadlist(partyId):
    session = SqlTemplate.new_session(ns_server_id='/db/mysql/bi_bigdata_db')
    sql_text = '''SELECT zmxy.data 
                      from ac_ccis_db.ZmxyAntifraudScoreReport zmxy 
                      WHERE zmxy.partyId = :partyId
                      order by zmxy.idZmxyWatchListReport desc
                        '''
    row_list = sql_util.select_rows_by_sql(sql_text, {'partyId': partyId}, ns_server_id='/db/mysql/bi_bigdata_db',max_size=-1)

    result = []
    for row in row_list:
        snapshot_data = json.loads(row[0], object_hook=json_util.object_hook_ts_str)
        if 'score' in snapshot_data.keys():
            result = snapshot_data['score']
            break

    return result

#init_app()
#data = query_overdue_of_zmxywatchlist()
#print(data[0])