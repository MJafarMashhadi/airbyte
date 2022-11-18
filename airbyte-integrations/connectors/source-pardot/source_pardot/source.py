#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


from typing import Any, List, Mapping, Tuple

from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .auth import ExpirationAwareTokenAuthenticator

from .api import Pardot
from .stream import Campaigns, EmailClicks, ListMembership, Lists, ProspectAccounts, Prospects, Users, VisitorActivities, Visitors, Visits


# Source
class SourcePardot(AbstractSource):
    @staticmethod
    def _get_pardot_object(config: Mapping[str, Any]) -> Pardot:
        pardot = Pardot(**config)
        pardot.login()
        return pardot

    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            pardot = self._get_pardot_object(config)
            pardot.access_token
            return True, None
        except Exception as e:
            return False, e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        pardot = self._get_pardot_object(config)
        
        def _refresh_token() -> tuple[str, datetime.datetime]:
            pardot.logout()
            pardot.login()
            return pardot.access_token, pardot.token_issued_at
        
        auth = ExpirationAwareTokenAuthenticator(
            pardot.access_token, 
            issued_at=pardot.token_issued_at, 
            refresh_token_fn=_refresh_token
        )
        args = {"authenticator": auth, "config": config}

        visitors = Visitors(**args)

        return [
            EmailClicks(**args),
            Campaigns(**args),
            ListMembership(**args),
            Lists(**args),
            ProspectAccounts(**args),
            Prospects(**args),
            Users(**args),
            VisitorActivities(**args),
            visitors,
            Visits(parent_stream=visitors, **args),
        ]
