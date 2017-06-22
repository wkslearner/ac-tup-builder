from ac_tup_builder.config import init_app
from ac_tup_builder.tup_build_task.org_party_tup_builder import OrgPartyDataSource
from ac_tup_builder.component import TupBuilder
from ac_tup_builder.tags_builder.population.base import GenderTagsBuilder
from datetime import datetime


init_app()

ds = OrgPartyDataSource(from_time=datetime.strptime('2017-01-01', '%Y-%m-%d'), to_time=datetime.strptime('2017-06-01', '%Y-%m-%d'))
tags_builder = GenderTagsBuilder()

tup_builder = TupBuilder(ds, tags_builder)

tup_builder.build()