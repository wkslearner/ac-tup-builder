from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, BigInteger, String, Integer, DateTime, Boolean, desc, orm
from sqlalchemy.orm import session
from abc import abstractmethod
from datetime import datetime
import json
from ti_util import json_util
import dpath.util
from ti_daf.sql_tx import session_scope


Base = declarative_base()


class AbstractTupRecord(object):
    gpartyId = Column(String, primary_key=True, nullable=False)
    lastPartyId = Column(String, nullable=False)
    tupData = Column(String, nullable=False)
    crtTime = Column(DateTime, nullable=False)
    updTime = Column(DateTime, nullable=False)

    oldTags = dict()

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

        if old_value is not None:
            if old_value == value:
                return
            else:
                dpath.util.new(self.oldTags, tagName, old_value, separator='.')

        dpath.util.new(self.tupDataObject, tagName, value, separator='.')


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
    __tablename__ = 'CreditTupRecord'

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