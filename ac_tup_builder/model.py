from abc import abstractmethod
from base64 import decode
from cx_Oracle import LOB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, String, Integer, DateTime, Boolean, desc, orm
from datetime import datetime
import json
from ti_util import json_util
import dpath.util, types
from ti_daf.sql_tx import session_scope


Base = declarative_base()


class AbstractTupRecord(object):
    gpartyId = Column('gpartyid',String, primary_key=True, nullable=False)
    lastPartyId = Column('lastpartyid',String, nullable=False)
    tupData = Column('tupdata',String, nullable=False)
    crtTime = Column('crttime',DateTime, nullable=False)
    updTime = Column('updtime',DateTime, nullable=False)

    historyRecs = None

    tupDataObject = None
    tagNamePrefix = None

    @abstractmethod
    def __init__(self):
        pass

    def _init_fields(self, gpartyId=None, tupData="{}"):
        self.gpartyId = gpartyId
        self.lastPartyId = gpartyId
        self.tupData = tupData
        self.crtTime = datetime.now()
        self.updTime = self.crtTime

    def load_tup_data(self):
        if self.tupData is None:
            self.tupDataObject = dict()
        else:
            if isinstance(self.tupData, (str)):
                pass
            else:
                self.tupData = self.tupData.read()
            self.tupDataObject = json.loads(self.tupData, object_hook=json_util.object_hook_ts_str)

    def dump_tup_data(self):
        if self.tupDataObject is not None:
            self.tupData = json.dumps(self.tupDataObject, default=json_util.default_ts_str)

    def __check_tag_name(self, tagName:str):
        if tagName.startswith(self.tagNamePrefix+'.') is False:
            raise ValueError('The prefix of tagName must be [' + self.tagNamePrefix + '.]')

    def get_tag(self, tagName:str):
        self.__check_tag_name(tagName)

        if self.tupDataObject is None:
            self.load_tup_data()

        try:
            return dpath.util.get(self.tupDataObject, tagName, separator='.')
        except KeyError:
            return None

    def set_tag(self, tagName:str, value):
        self.__check_tag_name(tagName)

        if self.tupDataObject is None:
            self.load_tup_data()

        try:
            old_value = dpath.util.get(self.tupDataObject, tagName, separator='.')
        except KeyError:
            old_value = None

        if old_value is None or old_value != value:
            if self.historyRecs is None:
                self.historyRecs = dict()

            self.historyRecs[tagName] = [old_value, value]

        dpath.util.new(self.tupDataObject, tagName, value, separator='.')


class TupHistoryRecord(Base):
    __tablename__ = 'tuphistoryrecord'

    idTupHistoryRecord = Column('idtupistoryrecord',BigInteger, primary_key=True, nullable=False,autoincrement=True)
    gpartyId = Column('gpartyid',String, nullable=False)
    tagName = Column('tagname',String, nullable=False)
    oldValue = Column('oldvalue',String, nullable=True)
    newValue = Column('newvalue',String, nullable=False)
    crtTime = Column('crttime',DateTime, nullable=False)

    def __init__(self):
        pass

    @classmethod
    def _convertToValueObject(cls, value):
        if value is None:
            return None

        return json.loads(value, object_hook=json_util.object_hook_ts_str)

    @classmethod
    def _convertFromValueObject(cls, valueObject):
        if valueObject is None:
            return None

        return json.dumps(valueObject, default=json_util.default_ts_str)

    def getOldValueObject(self):
        return self._convertToValueObject(self.oldValue)

    def setOldValueObject(self, oldValueObj):
        self.oldValue = self._convertFromValueObject(oldValueObj)

    def getNewValueObject(self):
        return self._convertToValueObject(self.newValue)

    def setNewValueObject(self, newValueObj):
        self.newValue = self._convertFromValueObject(newValueObj)


class TagNamePrefixes:
    POPULATION = "population"

    SOCIAL = "social"

    PREFERENCE = "preference"

    CREDIT = "credit"

    SALES = "sales"

    RISK = "risk"

    PAYMENT = "payment"

    LOAN = "loan"

    def __init__(self):
        raise Exception("Unable to instance.")


class PopulationTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'PopulationTupRecord'

    @orm.reconstructor
    def __init_on_load(self):
        self.tagNamePrefix = TagNamePrefixes.POPULATION

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.POPULATION


class SocialTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'SocialTupRecord'

    @orm.reconstructor
    def __init_on_load(self):
        self.tagNamePrefix = TagNamePrefixes.SOCIAL

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.SOCIAL


class PreferenceTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'PreferenceTupRecord'

    @orm.reconstructor
    def __init_on_load(self):
        self.tagNamePrefix = TagNamePrefixes.PREFERENCE

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.PREFERENCE


class CreditTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'credittuprecord'

    @orm.reconstructor
    def __init_on_load(self):
        self.tagNamePrefix = TagNamePrefixes.CREDIT

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.CREDIT


class SalesTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'SalesTupRecord'

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.SALES


class RiskTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'RiskTupRecord'

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.RISK


class PaymentTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'PaymentTupRecord'

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.PAYMENT


class LoanTupRecord(AbstractTupRecord, Base):
    __tablename__ = 'LoanTupRecord'

    def __init__(self, gpartyId=None, tupData="{}"):
        self._init_fields(gpartyId=gpartyId, tupData=tupData)
        self.tagNamePrefix = TagNamePrefixes.LOAN


class TupRecordStorage(object):
    @abstractmethod
    def save(self, tupRec:AbstractTupRecord):
        pass

    @abstractmethod
    def query(self, tagNamePrefix:str, gpartyId:str):
        pass

    @abstractmethod
    def query_by_lastPartyId(self, tagNamePrefix: str, lastPartyId: str):
        pass

    def _new(self, tagNamePrefix:str, gpartyId:str) -> AbstractTupRecord:
        if tagNamePrefix == TagNamePrefixes.POPULATION:
            return PopulationTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.SOCIAL:
            return SocialTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.PREFERENCE:
            return PreferenceTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.CREDIT:
            return CreditTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.SALES:
            return SalesTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.RISK:
            return RiskTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.PAYMENT:
            return PaymentTupRecord(gpartyId=gpartyId)
        elif tagNamePrefix == TagNamePrefixes.LOAN:
            return LoanTupRecord(gpartyId=gpartyId)

        raise ValueError('Unsupported tagNamePrefix=[' + tagNamePrefix + ']')

    def query_or_new(self, tagNamePrefix:str, gpartyId:str):
        tupRec = self.query(tagNamePrefix, gpartyId)
        if tupRec is None:
            tupRec = self._new(tagNamePrefix, gpartyId)

        return tupRec

    def query_or_new_by_lastPartyId(self, tagNamePrefix:str, lastPartyId:str):
        tupRec = self.query_by_lastPartyId(tagNamePrefix, lastPartyId)
        if tupRec is None:
            tupRec = self._new(tagNamePrefix, lastPartyId)

        return tupRec


class SqlTupRecordStorage(TupRecordStorage):
    def save(self, tupRec:AbstractTupRecord):
        with session_scope() as session:
            tupRec.dump_tup_data()
            session.add(tupRec)

            if tupRec.historyRecs is not None:
                tup_history_recs = []
                for tagName, values in tupRec.historyRecs.items():
                    tup_history_rec = TupHistoryRecord()
                    tup_history_rec.gpartyId = tupRec.gpartyId
                    tup_history_rec.tagName = tagName
                    tup_history_rec.crtTime = tupRec.updTime
                    tup_history_rec.setOldValueObject(values[0])
                    tup_history_rec.setNewValueObject(values[1])

                    tup_history_recs.append(tup_history_rec)
                    his_str = json.dumps(tup_history_rec, default=json_util.default_ts_str,
                                         ensure_ascii=False)
                    print(his_str)
                session.bulk_save_objects(tup_history_recs)

    def query(self, tagNamePrefix:str, gpartyId:str, lastPartyId:str=None):
        with session_scope() as session:
            if tagNamePrefix == TagNamePrefixes.POPULATION:
                return session.query(PopulationTupRecord).filter(PopulationTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.SOCIAL:
                return session.query(SocialTupRecord).filter(SocialTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.PREFERENCE:
                return session.query(PreferenceTupRecord).filter(PreferenceTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.CREDIT:
                return session.query(CreditTupRecord).filter(CreditTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.SALES:
                return session.query(SalesTupRecord).filter(SalesTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.RISK:
                return session.query(RiskTupRecord).filter(RiskTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.PAYMENT:
                return session.query(PaymentTupRecord).filter(PaymentTupRecord.gpartyId == gpartyId).first()
            elif tagNamePrefix == TagNamePrefixes.LOAN:
                return session.query(LoanTupRecord).filter(LoanTupRecord.gpartyId == gpartyId).first()

        raise ValueError('Unsupported tagNamePrefix=[' + tagNamePrefix + ']')

    def query_by_lastPartyId(self, tagNamePrefix: str, lastPartyId: str):
        with session_scope() as session:
            if tagNamePrefix == TagNamePrefixes.POPULATION:
                return session.query(PopulationTupRecord).filter(PopulationTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.SOCIAL:
                return session.query(SocialTupRecord).filter(SocialTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.PREFERENCE:
                return session.query(PreferenceTupRecord).filter(PreferenceTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.CREDIT:
                return session.query(CreditTupRecord).filter(CreditTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.SALES:
                return session.query(SalesTupRecord).filter(SalesTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.RISK:
                return session.query(RiskTupRecord).filter(RiskTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.PAYMENT:
                return session.query(PaymentTupRecord).filter(PaymentTupRecord.lastPartyId == lastPartyId).first()
            elif tagNamePrefix == TagNamePrefixes.LOAN:
                return session.query(LoanTupRecord).filter(LoanTupRecord.lastPartyId == lastPartyId).first()

        raise ValueError('Unsupported tagNamePrefix=[' + tagNamePrefix + ']')
