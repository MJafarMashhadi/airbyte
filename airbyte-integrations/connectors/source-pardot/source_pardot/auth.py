from __future__ import annotations
#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import datetime

from airbyte_cdk.entrypoint import logger
from airbyte_cdk.sources.streams.http.auth import TokenAuthenticator


class ExpirationAwareTokenAuthenticator(TokenAuthenticator):
    # Conservative assumption on the token's lifetime
    TOKEN_EXPIRY = datetime.timedelta(hours=6)  

    def __init__(self, token: str, refresh_token_fn, issued_at: datetime.datetime, *args, **kwargs):
        super().__init__(token, *args, **kwargs)
        self.token_expires_at = issued_at + self.TOKEN_EXPIRY
        self._refresh_token_fn = refresh_token_fn
    
    def token_has_expired(self):
        return datetime.datetime.now() > self.token_expires_at

    def get_auth_header(self):
        if self.token_has_expired():
            logger.info("Pardot token expired")
            self.token, issued_at = self._refresh_token_fn()
            self.token_expires_at = issued_at + self.TOKEN_EXPIRY
            logger.info("Issued a new token at", issued_at, "assuming the expiration to be on", self.token_expires_at)

        return super().get_auth_header()
